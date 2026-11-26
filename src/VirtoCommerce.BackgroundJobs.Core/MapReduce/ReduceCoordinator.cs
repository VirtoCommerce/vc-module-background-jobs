using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using VirtoCommerce.BackgroundJobs;
using VirtoCommerce.BackgroundJobs.Core.Notifications;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.PushNotifications;

namespace VirtoCommerce.BackgroundJobs.Core.MapReduce;

/// <summary>
/// Built-in handler that runs the single reduce step: loads every map result, re-hydrates them into
/// <see cref="MapResult{TResult}"/>, and invokes the user's <see cref="IReduceJobHandler{TState, TResult}"/>. Under
/// <see cref="FailurePolicy.FailFast"/> a batch with any failed item is marked faulted and the reduce handler is not
/// called.
/// </summary>
public sealed class ReduceCoordinator : IBackgroundJobHandler<ReduceTaskEnvelope>
{
    private readonly IServiceProvider _serviceProvider;
    private readonly IJobPayloadSerializer _serializer;
    private readonly IMapReduceBatchStore _store;
    private readonly IPushNotificationManager _pushNotificationManager;
    private readonly ILogger<ReduceCoordinator> _logger;

    public ReduceCoordinator(
        IServiceProvider serviceProvider,
        IJobPayloadSerializer serializer,
        IMapReduceBatchStore store,
        IPushNotificationManager pushNotificationManager,
        ILogger<ReduceCoordinator> logger)
    {
        _serviceProvider = serviceProvider;
        _serializer = serializer;
        _store = store;
        _pushNotificationManager = pushNotificationManager;
        _logger = logger;
    }

    // NOTE on DI scope: like MapCoordinator, this handler is resolved by DefaultJobDispatcher inside its per-job
    // scope, so the injected IServiceProvider IS that scope — the user IReduceJobHandler resolves in the same scope.
    public async Task Execute(ReduceTaskEnvelope envelope, IJobExecutionContext context, CancellationToken cancellationToken = default)
    {
        var batch = await _store.GetAsync(envelope.BatchId, cancellationToken);
        if (batch is null)
        {
            _logger.LogWarning("Reduce task for unknown or already-completed batch {BatchId}; skipping.", envelope.BatchId);
            return;
        }

        var results = await _store.GetResultsAsync(envelope.BatchId, cancellationToken);
        var failures = results.Count(x => !x.Succeeded);

        if (batch.FailurePolicy == FailurePolicy.FailFast && failures > 0)
        {
            _logger.LogError("Map/reduce batch {BatchId} faulted: {Failures}/{Total} map items failed (FailFast); reduce skipped.",
                envelope.BatchId, failures, batch.Total);
            await MarkFinishedAsync(batch, $"Faulted: {failures}/{batch.Total} map items failed.", JobOutcome.Failed);
            await _store.CompleteAsync(envelope.BatchId, cancellationToken);
            return;
        }

        try
        {
            var resultType = Type.GetType(batch.ResultType)
                ?? throw new InvalidOperationException($"Cannot resolve map result type '{batch.ResultType}'.");
            var stateType = Type.GetType(batch.StateType)
                ?? throw new InvalidOperationException($"Cannot resolve reduce state type '{batch.StateType}'.");

            var state = _serializer.Deserialize(batch.StateType, batch.StateJson);
            var mapResults = BuildMapResults(results, resultType);

            var handlerInterfaceType = typeof(IReduceJobHandler<,>).MakeGenericType(stateType, resultType);

            // Handler-explicit batch names the concrete reduce handler; resolve it by type. Otherwise resolve by interface.
            object handler;
            if (!string.IsNullOrEmpty(batch.ReduceHandlerType))
            {
                var concreteHandlerType = Type.GetType(batch.ReduceHandlerType)
                    ?? throw new InvalidOperationException($"Cannot resolve reduce handler type '{batch.ReduceHandlerType}'.");
                handler = _serviceProvider.GetService(concreteHandlerType)
                    ?? throw new InvalidOperationException($"No reduce handler '{concreteHandlerType.Name}' is registered.");
            }
            else
            {
                handler = _serviceProvider.GetService(handlerInterfaceType)
                    ?? throw new InvalidOperationException($"No IReduceJobHandler<{stateType.Name}, {resultType.Name}> is registered.");
            }

            var reduceMethod = handlerInterfaceType.GetMethod(nameof(IReduceJobHandler<object, object>.Reduce))!;
            await (Task)reduceMethod.Invoke(handler, [state, mapResults, context, cancellationToken])!;
        }
        catch (Exception ex)
        {
            // A synchronous throw from the reduce handler is wrapped by Invoke — surface the real exception (with its
            // stack) so the engine retries/dead-letters on the actual cause, not the reflection wrapper.
            var actual = ex is TargetInvocationException { InnerException: { } inner } ? inner : ex;

            // A cancel leaves the bar open: the engine decides what a cancel means.
            var canceled = actual is OperationCanceledException && cancellationToken.IsCancellationRequested;
            if (context is JobExecutionContext { IsFinalAttempt: true } && !canceled)
            {
                await MarkFinishedAsync(batch, $"Reduce failed: {actual.Message}", JobOutcome.Failed);
            }

            ExceptionDispatchInfo.Throw(actual);
        }

        await MarkFinishedAsync(batch, $"Completed ({batch.Total} items, {failures} failed).", JobOutcome.Succeeded);
        await _store.CompleteAsync(envelope.BatchId, cancellationToken);
        _logger.LogInformation("Map/reduce batch {BatchId} reduced ({Total} items, {Failures} failed).",
            envelope.BatchId, batch.Total, failures);
    }

    // Best effort on both paths: a thrown send would re-run a finished reduce, or replace the handler's exception.
    private Task MarkFinishedAsync(MapReduceBatch batch, string description, JobOutcome outcome)
    {
        if (string.IsNullOrEmpty(batch.ProgressNotificationId))
        {
            return Task.CompletedTask;
        }

        return BestEffortNotificationSend.TrySendAsync(
            () => _pushNotificationManager.SendAsync(JobProgressNotificationBuilder.Build(new JobProgressNotificationFields(
                batch.ProgressNotificationId,
                batch.UserName ?? "system",
                batch.Title,
                batch.Started,
                description,
                ProcessedCount: batch.Total,
                TotalCount: batch.Total,
                JobId: null,
                Attempt: null,
                TotalAttempts: null,
                outcome.ToString(),
                Error: null,
                Finished: DateTime.UtcNow,
                IsNew: true))),
            ex => _logger.LogError(ex, "Could not close the batch progress notification {NotificationId} as {Outcome}; the batch's outcome is unchanged.",
                batch.ProgressNotificationId, outcome));
    }

    // Re-hydrate the type-erased records into a List<MapResult<TResult>> the reduce handler can accept as
    // IReadOnlyCollection<MapResult<TResult>>.
    private object BuildMapResults(IReadOnlyCollection<MapResultRecord> records, Type resultType)
    {
        var mapResultType = typeof(MapResult<>).MakeGenericType(resultType);
        var list = (IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(mapResultType))!;

        foreach (var record in records.OrderBy(x => x.Index))
        {
            object? value = record.Succeeded && record.ResultJson != null
                ? _serializer.Deserialize(resultType.AssemblyQualifiedName!, record.ResultJson)
                : null;

            list.Add(Activator.CreateInstance(mapResultType, record.Index, value, record.Succeeded, record.Error)!);
        }

        return list;
    }
}
