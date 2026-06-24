using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using VirtoCommerce.BackgroundJobs.Core.Services;
using VirtoCommerce.Platform.Core.Jobs;

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
    private readonly ILogger<ReduceCoordinator> _logger;

    public ReduceCoordinator(
        IServiceProvider serviceProvider,
        IJobPayloadSerializer serializer,
        IMapReduceBatchStore store,
        ILogger<ReduceCoordinator> logger)
    {
        _serviceProvider = serviceProvider;
        _serializer = serializer;
        _store = store;
        _logger = logger;
    }

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
            await _store.CompleteAsync(envelope.BatchId, cancellationToken);
            return;
        }

        var resultType = Type.GetType(batch.ResultType)
            ?? throw new InvalidOperationException($"Cannot resolve map result type '{batch.ResultType}'.");
        var stateType = Type.GetType(batch.StateType)
            ?? throw new InvalidOperationException($"Cannot resolve reduce state type '{batch.StateType}'.");

        var state = _serializer.Deserialize(batch.StateType, batch.StateJson);
        var mapResults = BuildMapResults(results, resultType);

        var handlerType = typeof(IReduceJobHandler<,>).MakeGenericType(stateType, resultType);
        var handler = _serviceProvider.GetService(handlerType)
            ?? throw new InvalidOperationException($"No IReduceJobHandler<{stateType.Name}, {resultType.Name}> is registered.");

        var reduceMethod = handlerType.GetMethod(nameof(IReduceJobHandler<object, object>.Reduce))!;
        try
        {
            await (Task)reduceMethod.Invoke(handler, [state, mapResults, context, cancellationToken])!;
        }
        catch (TargetInvocationException ex) when (ex.InnerException is not null)
        {
            // A synchronous throw from the reduce handler is wrapped by Invoke — surface the real exception (with its
            // stack) so the engine retries/dead-letters on the actual cause, not the reflection wrapper.
            ExceptionDispatchInfo.Throw(ex.InnerException);
        }

        await _store.CompleteAsync(envelope.BatchId, cancellationToken);
        _logger.LogInformation("Map/reduce batch {BatchId} reduced ({Total} items, {Failures} failed).",
            envelope.BatchId, batch.Total, failures);
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
