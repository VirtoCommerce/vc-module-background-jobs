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

    /// <summary>Stores the batch's serialized items (in order) so the fan-out task can enqueue them from a worker.</summary>
    Task SaveItemsAsync(string batchId, IReadOnlyList<MapItem> items, CancellationToken cancellationToken = default);

    /// <summary>Reads the stored items (in order; index = position) for the fan-out task.</summary>
    Task<IReadOnlyList<MapItem>> GetItemsAsync(string batchId, CancellationToken cancellationToken = default);

    /// <summary>
    /// How many map tasks the fan-out has already dispatched (a checkpoint of contiguous indices 0..N-1). This is the
    /// fan-out's idempotency AND crash-recovery mechanism: a redelivered fan-out — after a clean redelivery or a hard
    /// worker crash mid-dispatch — reads this and resumes from there instead of re-dispatching from index 0, so a
    /// fully-completed fan-out re-enqueues nothing and an interrupted one still finishes the remaining indices.
    /// Returns 0 when the fan-out hasn't checkpointed yet.
    /// </summary>
    Task<int> GetFanOutProgressAsync(string batchId, CancellationToken cancellationToken = default);

    /// <summary>Records the fan-out dispatch checkpoint (the count of map tasks dispatched so far).</summary>
    Task SetFanOutProgressAsync(string batchId, int dispatched, CancellationToken cancellationToken = default);

    /// <summary>Idempotently store one item's result and return the distinct number of items completed so far.</summary>
    Task<int> SaveResultAndCountAsync(string batchId, MapResultRecord result, CancellationToken cancellationToken = default);

    /// <summary>
    /// Atomically claim the reduce step — returns true for exactly one caller per batch. Pair with
    /// <see cref="ReleaseReduceAsync"/> so a caller that wins the claim but then fails to enqueue the reduce task can
    /// release it, letting a redelivered map task re-trigger reduce instead of stranding the batch until TTL.
    /// </summary>
    Task<bool> TryBeginReduceAsync(string batchId, CancellationToken cancellationToken = default);

    /// <summary>Releases the reduce claim so a failed reduce enqueue can be retried (the claim becomes re-acquirable).</summary>
    Task ReleaseReduceAsync(string batchId, CancellationToken cancellationToken = default);

    Task<IReadOnlyCollection<MapResultRecord>> GetResultsAsync(string batchId, CancellationToken cancellationToken = default);

    /// <summary>Mark the batch finished and release its state.</summary>
    Task CompleteAsync(string batchId, CancellationToken cancellationToken = default);
}
