using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace VirtoCommerce.BackgroundJobs.Core.MapReduce;

/// <summary>
/// Shared, fleet-safe store backing a map/reduce batch: the metadata, the per-item results, and the exactly-once
/// reduce trigger. Results are keyed by item index so a redelivered map task overwrites (idempotent join);
/// <see cref="TryBeginReduceAsync"/> guarantees a single worker enqueues the reduce step. Redis-backed when
/// configured (atomic across instances), in-memory otherwise (single instance).
/// </summary>
public interface IMapReduceBatchStore
{
    Task CreateAsync(MapReduceBatch batch, CancellationToken cancellationToken = default);

    Task<MapReduceBatch?> GetAsync(string batchId, CancellationToken cancellationToken = default);

    /// <summary>Idempotently store one item's result and return the distinct number of items completed so far.</summary>
    Task<int> SaveResultAndCountAsync(string batchId, MapResultRecord result, CancellationToken cancellationToken = default);

    /// <summary>Atomically claim the reduce step — returns true for exactly one caller per batch.</summary>
    Task<bool> TryBeginReduceAsync(string batchId, CancellationToken cancellationToken = default);

    Task<IReadOnlyCollection<MapResultRecord>> GetResultsAsync(string batchId, CancellationToken cancellationToken = default);

    /// <summary>Mark the batch finished and release its state.</summary>
    Task CompleteAsync(string batchId, CancellationToken cancellationToken = default);
}
