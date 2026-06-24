using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using VirtoCommerce.BackgroundJobs.Core.Notifications;
using VirtoCommerce.BackgroundJobs.Core.Services;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.PushNotifications;
using VirtoCommerce.Platform.Core.Security;

namespace VirtoCommerce.BackgroundJobs.Core.MapReduce;

/// <summary>
/// Default <see cref="IMapReduceJob"/>: records the batch metadata, then fans out one ordinary background job per
/// item (handled by <c>MapCoordinator</c>). The map coordinators drive the join and trigger the single reduce job.
/// Everything rides the active engine via <see cref="IBackgroundJob"/>, so this is fully engine-agnostic.
/// </summary>
public sealed class MapReduceJob : IMapReduceJob
{
    private readonly IBackgroundJob _backgroundJob;
    private readonly IMapReduceBatchStore _store;
    private readonly IJobPayloadSerializer _serializer;
    private readonly IUserNameResolver _userNameResolver;
    private readonly IPushNotificationManager _pushNotificationManager;

    public MapReduceJob(
        IBackgroundJob backgroundJob,
        IMapReduceBatchStore store,
        IJobPayloadSerializer serializer,
        IUserNameResolver userNameResolver,
        IPushNotificationManager pushNotificationManager)
    {
        _backgroundJob = backgroundJob;
        _store = store;
        _serializer = serializer;
        _userNameResolver = userNameResolver;
        _pushNotificationManager = pushNotificationManager;
    }

    public async Task<string> Enqueue<TItem, TResult, TState>(
        IEnumerable<TItem> items,
        TState state,
        MapReduceOptions? options = null,
        CancellationToken cancellationToken = default)
        where TItem : class
        where TResult : class
        where TState : class
    {
        ArgumentNullException.ThrowIfNull(items);
        ArgumentNullException.ThrowIfNull(state);

        // Honor cancellation up front, but once we commit to a batch the fan-out below must complete as a unit —
        // a request abort mid-loop would leave Total set with only some map tasks enqueued, so the batch could never
        // reach completion (reduce would never fire). The fan-out therefore does not observe the caller's token.
        cancellationToken.ThrowIfCancellationRequested();

        var itemList = items as IReadOnlyList<TItem> ?? items.ToList();
        var batchId = Guid.NewGuid().ToString("N");
        var userName = _userNameResolver.GetCurrentUserName();
        var title = $"Map/reduce: {typeof(TItem).Name}";
        var (_, stateJson) = _serializer.Serialize(state);

        var progressNotificationId = await TryCreateProgressNotificationAsync(options, itemList.Count, userName, title);

        var batch = new MapReduceBatch
        {
            BatchId = batchId,
            Total = itemList.Count,
            ItemType = typeof(TItem).AssemblyQualifiedName!,
            ResultType = typeof(TResult).AssemblyQualifiedName!,
            StateType = typeof(TState).AssemblyQualifiedName!,
            StateJson = stateJson,
            Queue = options?.Queue,
            FailurePolicy = options?.FailurePolicy ?? FailurePolicy.FailFast,
            ProgressNotificationId = progressNotificationId,
            Title = title,
            UserName = userName,
        };

        await _store.CreateAsync(batch, CancellationToken.None);

        // Empty batch: nothing to map, run reduce immediately so the finalize step still happens.
        if (itemList.Count == 0)
        {
            await _backgroundJob.Enqueue(new ReduceTaskEnvelope { BatchId = batchId },
                new EnqueueOptions { Queue = options?.Queue }, CancellationToken.None);
            return batchId;
        }

        // Store the items and hand the actual fan-out (one map task per item) to a worker, so this call stays fast
        // even for very large batches instead of publishing N messages inline on the request thread.
        var mapItems = new List<MapItem>(itemList.Count);
        foreach (var item in itemList)
        {
            var (itemType, itemJson) = _serializer.Serialize(item);
            mapItems.Add(new MapItem { ItemType = itemType, ItemJson = itemJson });
        }

        await _store.SaveItemsAsync(batchId, mapItems, CancellationToken.None);

        await _backgroundJob.Enqueue(new MapFanOutEnvelope { BatchId = batchId },
            new EnqueueOptions { Queue = options?.Queue }, CancellationToken.None);

        return batchId;
    }

    private async Task<string?> TryCreateProgressNotificationAsync(MapReduceOptions? options, int total, string? userName, string title)
    {
        if (options?.ReportProgress != true)
        {
            return null;
        }

        var notification = new JobProgressPushNotification(userName ?? "system")
        {
            Title = title,
            Description = "Queued",
            Started = DateTime.UtcNow,
            TotalCount = total,
        };
        await _pushNotificationManager.SendAsync(notification);

        return notification.Id;
    }
}
