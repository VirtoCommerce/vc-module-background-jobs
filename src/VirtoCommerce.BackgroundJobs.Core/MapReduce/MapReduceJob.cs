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

    public async Task<string> Enqueue<TMap, TReduce>(
        IEnumerable<object> items,
        object state,
        MapReduceOptions? options = null,
        CancellationToken cancellationToken = default)
        where TMap : class
        where TReduce : class
    {
        ArgumentNullException.ThrowIfNull(items);
        ArgumentNullException.ThrowIfNull(state);

        // Derive the item/result/state contract from the handler interfaces (validates the pair agrees on the result
        // type), then validate the supplied state/items match — so a mismatched handler/payload fails fast here.
        var (itemType, resultType, stateType) = MapReduceHandlerTypes.ResolveTypes(typeof(TMap), typeof(TReduce));

        if (!stateType.IsInstanceOfType(state))
        {
            throw new ArgumentException(
                $"Reduce handler '{typeof(TReduce).Name}' expects state of type '{stateType.Name}', but got '{state.GetType().Name}'.",
                nameof(state));
        }

        // Honor cancellation up front, but once we commit to a batch the fan-out below must complete as a unit —
        // a request abort mid-loop would leave Total set with only some map tasks enqueued, so the batch could never
        // reach completion (reduce would never fire). The fan-out therefore does not observe the caller's token.
        cancellationToken.ThrowIfCancellationRequested();

        var itemList = items as IReadOnlyList<object> ?? items.ToList();
        var batchId = Guid.NewGuid().ToString("N");
        var userName = _userNameResolver.GetCurrentUserName();
        var title = $"Map/reduce: {itemType.Name}";
        var (_, stateJson) = _serializer.Serialize(state);

        var progressNotificationId = await TryCreateProgressNotificationAsync(options, itemList.Count, userName, title);

        var batch = new MapReduceBatch
        {
            BatchId = batchId,
            Total = itemList.Count,
            ItemType = itemType.AssemblyQualifiedName!,
            ResultType = resultType.AssemblyQualifiedName!,
            StateType = stateType.AssemblyQualifiedName!,
            StateJson = stateJson,
            MapHandlerType = typeof(TMap).AssemblyQualifiedName!,
            ReduceHandlerType = typeof(TReduce).AssemblyQualifiedName!,
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
            await _backgroundJob.Enqueue<ReduceCoordinator>(new ReduceTaskEnvelope { BatchId = batchId },
                new EnqueueOptions { Queue = options?.Queue }, CancellationToken.None);
            return batchId;
        }

        // Store the items and hand the actual fan-out (one map task per item) to a worker, so this call stays fast
        // even for very large batches instead of publishing N messages inline on the request thread.
        var mapItems = new List<MapItem>(itemList.Count);
        foreach (var item in itemList)
        {
            ArgumentNullException.ThrowIfNull(item);
            if (!itemType.IsInstanceOfType(item))
            {
                throw new ArgumentException(
                    $"Map handler '{typeof(TMap).Name}' expects items of type '{itemType.Name}', but got '{item.GetType().Name}'.",
                    nameof(items));
            }

            var (serializedType, itemJson) = _serializer.Serialize(item);
            mapItems.Add(new MapItem { ItemType = serializedType, ItemJson = itemJson });
        }

        await _store.SaveItemsAsync(batchId, mapItems, CancellationToken.None);

        await _backgroundJob.Enqueue<FanOutCoordinator>(new MapFanOutEnvelope { BatchId = batchId },
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
