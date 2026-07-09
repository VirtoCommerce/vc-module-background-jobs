#nullable enable
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using VirtoCommerce.BackgroundJobs.Core.MapReduce;

namespace VirtoCommerce.BackgroundJobs.Data.MapReduce;

/// <summary>
/// Single-instance <see cref="IMapReduceBatchStore"/>. Results are keyed by item index (idempotent on redelivery) and
/// the reduce trigger is claimed with an interlocked flag. Not shared across instances — use the Redis store for a
/// multi-instance fleet.
/// </summary>
public sealed class InMemoryMapReduceBatchStore : IMapReduceBatchStore
{
    private sealed class Entry
    {
        public required MapReduceBatch Batch { get; init; }
        public ConcurrentDictionary<int, MapResultRecord> Results { get; } = new();
        public IReadOnlyList<MapItem> Items { get; set; } = [];
        public int ReduceClaimed;
        public int FanOutClaimed;
        public int FanOutProgress;
    }

    private readonly ConcurrentDictionary<string, Entry> _batches = new();

    public Task CreateAsync(MapReduceBatch batch, CancellationToken cancellationToken = default)
    {
        _batches[batch.BatchId] = new Entry { Batch = batch };
        return Task.CompletedTask;
    }

    public Task<MapReduceBatch?> GetAsync(string batchId, CancellationToken cancellationToken = default)
        => Task.FromResult(_batches.TryGetValue(batchId, out var entry) ? entry.Batch : null);

    public Task SaveItemsAsync(string batchId, IReadOnlyList<MapItem> items, CancellationToken cancellationToken = default)
    {
        if (_batches.TryGetValue(batchId, out var entry))
        {
            entry.Items = items;
        }
        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<MapItem>> GetItemsAsync(string batchId, CancellationToken cancellationToken = default)
        => Task.FromResult(_batches.TryGetValue(batchId, out var entry) ? entry.Items : []);

    public Task<bool> TryBeginFanOutAsync(string batchId, CancellationToken cancellationToken = default)
    {
        if (!_batches.TryGetValue(batchId, out var entry))
        {
            return Task.FromResult(false);
        }

        return Task.FromResult(Interlocked.CompareExchange(ref entry.FanOutClaimed, 1, 0) == 0);
    }

    public Task ReleaseFanOutAsync(string batchId, CancellationToken cancellationToken = default)
    {
        if (_batches.TryGetValue(batchId, out var entry))
        {
            Interlocked.Exchange(ref entry.FanOutClaimed, 0);
        }
        return Task.CompletedTask;
    }

    public Task<int> GetFanOutProgressAsync(string batchId, CancellationToken cancellationToken = default)
        => Task.FromResult(_batches.TryGetValue(batchId, out var entry) ? Volatile.Read(ref entry.FanOutProgress) : 0);

    public Task SetFanOutProgressAsync(string batchId, int dispatched, CancellationToken cancellationToken = default)
    {
        if (_batches.TryGetValue(batchId, out var entry))
        {
            Volatile.Write(ref entry.FanOutProgress, dispatched);
        }
        return Task.CompletedTask;
    }

    public Task<int> SaveResultAndCountAsync(string batchId, MapResultRecord result, CancellationToken cancellationToken = default)
    {
        if (!_batches.TryGetValue(batchId, out var entry))
        {
            return Task.FromResult(0);
        }

        entry.Results[result.Index] = result;
        return Task.FromResult(entry.Results.Count);
    }

    public Task<bool> TryBeginReduceAsync(string batchId, CancellationToken cancellationToken = default)
    {
        if (!_batches.TryGetValue(batchId, out var entry))
        {
            return Task.FromResult(false);
        }

        return Task.FromResult(Interlocked.CompareExchange(ref entry.ReduceClaimed, 1, 0) == 0);
    }

    public Task ReleaseReduceAsync(string batchId, CancellationToken cancellationToken = default)
    {
        if (_batches.TryGetValue(batchId, out var entry))
        {
            Interlocked.Exchange(ref entry.ReduceClaimed, 0);
        }
        return Task.CompletedTask;
    }

    public Task<IReadOnlyCollection<MapResultRecord>> GetResultsAsync(string batchId, CancellationToken cancellationToken = default)
    {
        IReadOnlyCollection<MapResultRecord> results = _batches.TryGetValue(batchId, out var entry)
            ? entry.Results.Values.ToList()
            : [];
        return Task.FromResult(results);
    }

    public Task CompleteAsync(string batchId, CancellationToken cancellationToken = default)
    {
        _batches.TryRemove(batchId, out _);
        return Task.CompletedTask;
    }
}
