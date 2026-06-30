using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs.Core.MapReduce;

/// <summary>
/// Built-in handler that performs the fan-out on a worker: reads the batch's stored items and enqueues one map task
/// per item. Running this off the enqueueing request keeps <see cref="IMapReduceJob.Enqueue{TItem, TResult, TState}"/>
/// fast even for very large batches. Idempotent under redelivery — map results are keyed by item index, so re-running
/// re-enqueues the same indices without inflating the completion count.
/// </summary>
public sealed class FanOutCoordinator : IBackgroundJobHandler<MapFanOutEnvelope>
{
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

            _logger.LogInformation("Fanning out {Count} map task(s) for batch {BatchId}.", items.Count, envelope.BatchId);

            var index = 0;
            foreach (var item in items)
            {
                var mapEnvelope = new MapTaskEnvelope
                {
                    BatchId = envelope.BatchId,
                    Index = index++,
                    ItemType = item.ItemType,
                    ItemJson = item.ItemJson,
                };

                await _backgroundJob.Enqueue<MapCoordinator>(mapEnvelope,
                    new EnqueueOptions { Queue = batch.Queue, ProgressNotificationId = batch.ProgressNotificationId, Title = batch.Title },
                    cancellationToken);
            }
        }
        catch
        {
            // Fan-out failed part-way: release the claim so a retry can re-run it (map results are keyed by index,
            // so re-enqueuing already-dispatched indices is idempotent) instead of leaving the batch with no tasks.
            await _store.ReleaseFanOutAsync(envelope.BatchId, CancellationToken.None);
            throw;
        }
    }
}
