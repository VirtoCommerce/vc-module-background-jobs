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
}
