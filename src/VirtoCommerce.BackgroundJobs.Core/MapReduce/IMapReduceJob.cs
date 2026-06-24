using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace VirtoCommerce.BackgroundJobs.Core.MapReduce;

/// <summary>
/// Developer-facing facade for fan-out/aggregate jobs: splits work into items that each run as an independent map
/// task in parallel across all workers, then runs the reduce handler once when every item has finished.
/// Engine-agnostic — works on whichever <c>IJobEngine</c> is active.
/// </summary>
public interface IMapReduceJob
{
    /// <summary>
    /// Enqueue a map/reduce batch: each item runs through the registered <see cref="IMapJobHandler{TItem, TResult}"/>
    /// in parallel, then the registered <see cref="IReduceJobHandler{TState, TResult}"/> runs once with all results
    /// plus the supplied <paramref name="state"/>. Returns the batch id. The map/reduce handlers are resolved from DI
    /// (register them with <c>AddMapReduceJob</c>), so they are not type parameters here. <typeparamref name="TItem"/>
    /// and <typeparamref name="TState"/> are inferred from the arguments; specify <typeparamref name="TResult"/>
    /// explicitly (it appears in no argument).
    /// </summary>
    /// <param name="items">
    /// The work split into independent units — the engine runs <b>one map task per item</b>, in parallel across all
    /// workers. Partition coarsely (e.g. a page of ids, not a single document) so the task count and each result stay
    /// small; this is the fan-out width.
    /// </param>
    /// <param name="state">
    /// Shared, read-only context passed to the <b>reduce</b> step once (NOT to the map handler). Carry batch-level
    /// info the reducer needs — what was processed, a start timestamp, an index alias to swap, a correlation id, etc.
    /// </param>
    /// <param name="options">Queue, failure policy, and progress settings. See <see cref="MapReduceOptions"/>.</param>
    /// <param name="cancellationToken">Cancels enqueuing the batch (not the running jobs).</param>
    Task<string> Enqueue<TItem, TResult, TState>(
        IEnumerable<TItem> items,
        TState state,
        MapReduceOptions? options = null,
        CancellationToken cancellationToken = default)
        where TItem : class
        where TResult : class
        where TState : class;
}
