using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using VirtoCommerce.BackgroundJobs.Core.MapReduce;

namespace VirtoCommerce.BackgroundJobs.Conformance;

internal sealed class ConformanceFaultInjectingBatchStore(IMapReduceBatchStore inner) : IMapReduceBatchStore
{
    private readonly ConcurrentDictionary<string, string?> _reduceHandlers = new();

    public Task CreateAsync(MapReduceBatch batch, CancellationToken cancellationToken = default)
    {
        _reduceHandlers[batch.BatchId] = batch.ReduceHandlerType;
        return inner.CreateAsync(batch, cancellationToken);
    }

    public Task<MapReduceBatch?> GetAsync(string batchId, CancellationToken cancellationToken = default)
        => inner.GetAsync(batchId, cancellationToken);

    public Task SaveItemsAsync(string batchId, IReadOnlyList<MapItem> items, CancellationToken cancellationToken = default)
        => inner.SaveItemsAsync(batchId, items, cancellationToken);

    public Task<IReadOnlyList<MapItem>> GetItemsAsync(string batchId, CancellationToken cancellationToken = default)
        => inner.GetItemsAsync(batchId, cancellationToken);

    public Task<int> GetFanOutProgressAsync(string batchId, CancellationToken cancellationToken = default)
    {
        ThrowIfFaulted<ConformanceFanOutFailureReduceHandler>(batchId, "Conformance-induced fan-out failure.");
        return inner.GetFanOutProgressAsync(batchId, cancellationToken);
    }

    public Task SetFanOutProgressAsync(string batchId, int dispatched, CancellationToken cancellationToken = default)
    {
        ThrowIfFaulted<ConformanceFanOutCheckpointFailureReduceHandler>(batchId, "Conformance-induced fan-out checkpoint failure.");
        return inner.SetFanOutProgressAsync(batchId, dispatched, cancellationToken);
    }

    public Task<int> SaveResultAndCountAsync(string batchId, MapResultRecord result, CancellationToken cancellationToken = default)
        => inner.SaveResultAndCountAsync(batchId, result, cancellationToken);

    public Task<bool> TryBeginReduceAsync(string batchId, CancellationToken cancellationToken = default)
    {
        ThrowIfFaulted<ConformanceMapFailureReduceHandler>(batchId, "Conformance-induced map failure.");
        return inner.TryBeginReduceAsync(batchId, cancellationToken);
    }

    public Task ReleaseReduceAsync(string batchId, CancellationToken cancellationToken = default)
        => inner.ReleaseReduceAsync(batchId, cancellationToken);

    public Task<IReadOnlyCollection<MapResultRecord>> GetResultsAsync(string batchId, CancellationToken cancellationToken = default)
    {
        ThrowIfFaulted<ConformanceResultsReadFailureReduceHandler>(batchId, "Conformance-induced results read failure.");
        return inner.GetResultsAsync(batchId, cancellationToken);
    }

    public Task MarkProgressClosedAsync(string batchId, ProgressClose close, CancellationToken cancellationToken = default)
        => inner.MarkProgressClosedAsync(batchId, close, cancellationToken);

    public Task<ProgressClose?> GetProgressCloseAsync(string batchId, CancellationToken cancellationToken = default)
        => inner.GetProgressCloseAsync(batchId, cancellationToken);

    public Task CompleteAsync(string batchId, CancellationToken cancellationToken = default)
        => inner.CompleteAsync(batchId, cancellationToken);

    private void ThrowIfFaulted<TMarker>(string batchId, string message)
    {
        if (_reduceHandlers.TryGetValue(batchId, out var reduceHandler) && reduceHandler == typeof(TMarker).AssemblyQualifiedName)
        {
            throw new InvalidOperationException(message);
        }
    }
}
