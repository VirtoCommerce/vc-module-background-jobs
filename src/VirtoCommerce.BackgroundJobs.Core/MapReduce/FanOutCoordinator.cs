using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs.Core.MapReduce;

/// <summary>
/// Built-in handler that performs the fan-out on a worker: reads the batch's stored items and enqueues one map task
/// per item. Running this off the enqueueing request keeps <see cref="IMapReduceJob.Enqueue{TMap, TReduce}"/>
/// fast even for very large batches. Idempotent under redelivery — map results are keyed by item index, so re-running
/// re-enqueues the same indices without inflating the completion count.
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

        // Claim the one-time fan-out. A redelivered or retried fan-out job (broker redelivery, engine retry, or
        // ack-after-dispatch) gets false and skips, so the map tasks are enqueued exactly once — otherwise a second
        // full set would run duplicate handler side effects and race reduce/cleanup (completion is keyed by item
        // index, so reduce can fire once each index has any result while duplicates are still in flight).
        if (!await _store.TryBeginFanOutAsync(envelope.BatchId, cancellationToken))
        {
            _logger.LogInformation("Fan-out for batch {BatchId} already dispatched; skipping duplicate.", envelope.BatchId);
            return;
        }

        try
        {
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

            // Resume from the last checkpoint so a retried fan-out doesn't re-dispatch already-enqueued indices (which
            // would re-run those map handlers). Map delivery is at-least-once regardless — a map task can be
            // redelivered independently — so handlers must be idempotent; this just avoids re-dispatching the whole
            // set on a partial-failure retry.
            var startIndex = await _store.GetFanOutProgressAsync(envelope.BatchId, cancellationToken);

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

                // Checkpoint every few items so a retry resumes near the failure point rather than restarting; the
                // window of possible re-dispatch is bounded by CheckpointEvery (index+1 tasks are now dispatched).
                if ((index + 1) % CheckpointEvery == 0)
                {
                    await _store.SetFanOutProgressAsync(envelope.BatchId, index + 1, cancellationToken);
                }
            }
        }
        catch
        {
            // Fan-out failed part-way: release the claim so a retry can re-run it (resuming from the last checkpoint;
            // map results are keyed by index, so re-enqueuing an index is idempotent) instead of leaving the batch
            // with no tasks.
            await _store.ReleaseFanOutAsync(envelope.BatchId, CancellationToken.None);
            throw;
        }
    }
}
