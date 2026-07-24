using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs.Core.MapReduce;

/// <summary>
/// Built-in handler that performs the fan-out on a worker: reads the batch's stored items and enqueues one map task
/// per item. Running this off the enqueueing request keeps <see cref="IMapReduceJob.Enqueue{TMap, TReduce}"/>
/// fast even for very large batches. Idempotent AND crash-recoverable via a persisted dispatch checkpoint
/// (<see cref="IMapReduceBatchStore.GetFanOutProgressAsync"/>): a redelivered fan-out resumes from where the last run
/// stopped — a fully-completed one re-enqueues nothing, and one interrupted by a worker crash finishes the remaining
/// indices. Map results are keyed by item index, so any re-enqueue is idempotent for the join.
/// </summary>
public sealed class FanOutCoordinator : IBackgroundJobHandler<MapFanOutEnvelope>
{
    // Persist a fan-out dispatch checkpoint every this many items, so a retried fan-out resumes near the failure
    // point instead of re-dispatching from index 0. Bounds the re-dispatch window without a store write per item.
    private const int CheckpointEvery = 25;

    private readonly IMapReduceBatchStore _store;
    private readonly IBackgroundJob _backgroundJob;
    private readonly ILogger<FanOutCoordinator> _logger;

    public FanOutCoordinator(IMapReduceBatchStore store, IBackgroundJob backgroundJob, ILogger<FanOutCoordinator> logger)
    {
        _store = store;
        _backgroundJob = backgroundJob;
        _logger = logger;
    }

    public async Task Execute(MapFanOutEnvelope envelope, IJobExecutionContext context, CancellationToken cancellationToken = default)
    {
        var batch = await _store.GetAsync(envelope.BatchId, cancellationToken);
        if (batch is null)
        {
            _logger.LogWarning("Fan-out for unknown or completed batch {BatchId}; skipping.", envelope.BatchId);
            return;
        }

        var items = await _store.GetItemsAsync(envelope.BatchId, cancellationToken);

        // Defensive: fan-out only runs for a non-empty batch (MapReduceJob enqueues reduce directly when Total == 0
        // and never fans out), so a stored-item count that doesn't match Total means the items were lost or expired
        // — a corrupt batch. Fanning out a short set would never reach Total, so completion (and the reduce trigger
        // in MapCoordinator) would never fire and the progress notification would hang until TTL. Instead, log it
        // and enqueue reduce directly so the batch still reaches a terminal state (finalize + close notification +
        // cleanup). This path is not reachable in the normal flow; it's a guard against a stuck batch.
        if (items.Count != batch.Total)
        {
            _logger.LogError(
                "Fan-out for batch {BatchId} found {Count} stored item(s) but expected {Total}; finalizing via reduce to avoid a stuck batch.",
                envelope.BatchId, items.Count, batch.Total);

            await _backgroundJob.Enqueue<ReduceCoordinator>(new ReduceTaskEnvelope { BatchId = envelope.BatchId },
                new EnqueueOptions { Queue = batch.Queue }, cancellationToken);
            return;
        }

        // Resume from the last checkpoint. This is the fan-out's ONLY idempotency + crash-recovery mechanism — there is
        // deliberately no one-time "claim", because a claim orphaned by a hard worker crash (its release never runs)
        // would make the redelivered fan-out skip dispatch and strand the batch below Total forever. Resuming instead:
        //   • a fully-dispatched fan-out (checkpoint == Total) re-enqueues nothing;
        //   • one interrupted by a crash re-runs only the indices since the last checkpoint and finishes the rest.
        // Map delivery is at-least-once regardless (a map task can be redelivered on its own), so handlers must be
        // idempotent and re-enqueuing an index is safe for the join (results are keyed by index).
        var startIndex = await _store.GetFanOutProgressAsync(envelope.BatchId, cancellationToken);
        if (startIndex >= items.Count)
        {
            _logger.LogInformation("Fan-out for batch {BatchId} already fully dispatched ({Total} task(s)); skipping.", envelope.BatchId, items.Count);
            return;
        }

        _logger.LogInformation("Fanning out map task(s) {Start}..{Total} for batch {BatchId}.", startIndex, items.Count, envelope.BatchId);

        for (var index = startIndex; index < items.Count; index++)
        {
            var item = items[index];
            var mapEnvelope = new MapTaskEnvelope
            {
                BatchId = envelope.BatchId,
                Index = index,
                ItemType = item.ItemType,
                ItemJson = item.ItemJson,
            };

            await _backgroundJob.Enqueue<MapCoordinator>(mapEnvelope,
                new EnqueueOptions { Queue = batch.Queue, ProgressNotificationId = batch.ProgressNotificationId, Title = batch.Title },
                cancellationToken);

            // Checkpoint every few items so a retry (redelivery or crash) resumes near the failure point rather than
            // re-dispatching from the last checkpoint; the re-dispatch window is bounded by CheckpointEvery.
            if ((index + 1) % CheckpointEvery == 0)
            {
                await _store.SetFanOutProgressAsync(envelope.BatchId, index + 1, cancellationToken);
            }
        }

        // Final checkpoint at Total so a later redelivery of a fully-completed fan-out re-enqueues nothing (the
        // early-return above), keeping "run twice → dispatch once" while still allowing crash recovery.
        await _store.SetFanOutProgressAsync(envelope.BatchId, items.Count, cancellationToken);
    }
}
