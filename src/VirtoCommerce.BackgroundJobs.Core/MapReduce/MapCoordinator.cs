using System;
using System.Reflection;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using VirtoCommerce.BackgroundJobs;
using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs.Core.MapReduce;

/// <summary>
/// Built-in handler that runs ONE map item: resolves the user's <see cref="IMapJobHandler{TItem, TResult}"/>, runs it,
/// idempotently stores the result by index, and — when it observes the last item completing — claims and enqueues the
/// single reduce task. A failed item is recorded (not retried) so the join is deterministic across engines; the
/// failure policy decides at reduce time whether failures abort the batch.
/// </summary>
public sealed class MapCoordinator : IBackgroundJobHandler<MapTaskEnvelope>
{
    private readonly IServiceProvider _serviceProvider;
    private readonly IJobPayloadSerializer _serializer;
    private readonly IMapReduceBatchStore _store;
    private readonly IBackgroundJob _backgroundJob;
    private readonly ILogger<MapCoordinator> _logger;

    public MapCoordinator(
        IServiceProvider serviceProvider,
        IJobPayloadSerializer serializer,
        IMapReduceBatchStore store,
        IBackgroundJob backgroundJob,
        ILogger<MapCoordinator> logger)
    {
        _serviceProvider = serviceProvider;
        _serializer = serializer;
        _store = store;
        _backgroundJob = backgroundJob;
        _logger = logger;
    }

    // NOTE on DI scope: this coordinator is itself an IBackgroundJobHandler, so DefaultJobDispatcher resolves it
    // INSIDE its per-job scope. The IServiceProvider injected here is therefore that job scope (not the root), and the
    // user IMapJobHandler resolved from it below shares the same scope as every other per-job dependency.
    public async Task Execute(MapTaskEnvelope envelope, IJobExecutionContext context, CancellationToken cancellationToken = default)
    {
        var batch = await _store.GetAsync(envelope.BatchId, cancellationToken);
        if (batch is null)
        {
            _logger.LogWarning("Map task for unknown or expired batch {BatchId}; skipping.", envelope.BatchId);
            return;
        }

        var record = await RunMapAsync(envelope, batch, context, cancellationToken);
        var completed = await _store.SaveResultAndCountAsync(envelope.BatchId, record, cancellationToken);

        // Throttle progress to ~100 updates plus the last one, so a large batch doesn't flood the admin UI / SignalR
        // with one notification per item.
        var step = Math.Max(1, batch.Total / 100);
        if (!string.IsNullOrEmpty(batch.ProgressNotificationId) && (completed >= batch.Total || completed % step == 0))
        {
            await context.Progress.Report(
                new JobProgressInfo { Message = $"Mapped {completed}/{batch.Total}", ProcessedCount = completed, TotalCount = batch.Total },
                cancellationToken);
        }

        // The worker that observes the final completion (and wins the atomic claim) triggers reduce exactly once.
        if (completed >= batch.Total && await _store.TryBeginReduceAsync(envelope.BatchId, cancellationToken))
        {
            try
            {
                await _backgroundJob.Enqueue<ReduceCoordinator>(new ReduceTaskEnvelope { BatchId = envelope.BatchId },
                    new EnqueueOptions { Queue = batch.Queue }, cancellationToken);
            }
            catch
            {
                // We won the claim but the enqueue failed — release it so a redelivered map task can re-trigger reduce.
                // Otherwise the claim stays set, no reduce task exists, and the batch never completes (stuck until TTL).
                await _store.ReleaseReduceAsync(envelope.BatchId, CancellationToken.None);
                throw;
            }
        }
    }

    private async Task<MapResultRecord> RunMapAsync(MapTaskEnvelope envelope, MapReduceBatch batch, IJobExecutionContext context, CancellationToken cancellationToken)
    {
        try
        {
            // Use the batch's item CONTRACT type (the handler's TItem) for the interface, and the message's concrete
            // item type only to deserialize — so a derived item still invokes the handler's IMapJobHandler<TItem,...>.
            var itemContractType = Type.GetType(batch.ItemType)
                ?? throw new InvalidOperationException($"Cannot resolve map item type '{batch.ItemType}'.");
            var resultType = Type.GetType(batch.ResultType)
                ?? throw new InvalidOperationException($"Cannot resolve map result type '{batch.ResultType}'.");

            var handlerInterfaceType = typeof(IMapJobHandler<,>).MakeGenericType(itemContractType, resultType);

            // Handler-explicit batch names the concrete map handler; resolve it by type (so several handlers can share
            // one item/result type). Otherwise resolve by the IMapJobHandler<,> interface.
            object handler;
            if (!string.IsNullOrEmpty(batch.MapHandlerType))
            {
                var concreteHandlerType = Type.GetType(batch.MapHandlerType)
                    ?? throw new InvalidOperationException($"Cannot resolve map handler type '{batch.MapHandlerType}'.");
                handler = _serviceProvider.GetService(concreteHandlerType)
                    ?? throw new InvalidOperationException($"No map handler '{concreteHandlerType.Name}' is registered.");
            }
            else
            {
                handler = _serviceProvider.GetService(handlerInterfaceType)
                    ?? throw new InvalidOperationException($"No IMapJobHandler<{itemContractType.Name}, {resultType.Name}> is registered.");
            }

            var item = _serializer.Deserialize(envelope.ItemType, envelope.ItemJson);
            var mapMethod = handlerInterfaceType.GetMethod(nameof(IMapJobHandler<object, object>.Map))!;

            var task = (Task)mapMethod.Invoke(handler, [item, context, cancellationToken])!;
            await task;

            var value = task.GetType().GetProperty("Result")!.GetValue(task);

            // A map handler may legitimately return null; record it as a successful null result rather than passing
            // null to Serialize (which would surface as a confusing NRE-shaped failure that faults the whole batch
            // under the default FailFast policy).
            var resultJson = value is null ? "null" : _serializer.Serialize(value).PayloadJson;

            return new MapResultRecord { Index = envelope.Index, Succeeded = true, ResultJson = resultJson };
        }
        catch (Exception ex)
        {
            // MethodInfo.Invoke wraps a synchronous throw from the handler (e.g. a guard clause before its first
            // await, or a non-async handler) in TargetInvocationException — unwrap so we log and record the real
            // cause, not the "Exception has been thrown by the target of an invocation" wrapper.
            var actual = (ex as TargetInvocationException)?.InnerException ?? ex;

            if (actual is OperationCanceledException)
            {
                ExceptionDispatchInfo.Throw(actual); // cancellation is not a job failure — propagate it
            }

            _logger.LogError(actual, "Map item {Index} of batch {BatchId} failed.", envelope.Index, envelope.BatchId);
            return new MapResultRecord { Index = envelope.Index, Succeeded = false, Error = actual.Message };
        }
    }
}
