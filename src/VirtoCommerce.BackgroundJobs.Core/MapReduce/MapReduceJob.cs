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

        var itemList = items as IReadOnlyList<TItem> ?? items.ToList();
        var batchId = Guid.NewGuid().ToString("N");
        var userName = _userNameResolver.GetCurrentUserName();
        var (_, stateJson) = _serializer.Serialize(state);

        var progressNotificationId = await TryCreateProgressNotificationAsync(options, itemList.Count, userName);

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
            UserName = userName,
        };

        await _store.CreateAsync(batch, cancellationToken);

        // Empty batch: nothing to map, run reduce immediately so the finalize step still happens.
        if (itemList.Count == 0)
        {
            await _backgroundJob.Enqueue(new ReduceTaskEnvelope { BatchId = batchId },
                new EnqueueOptions { Queue = options?.Queue }, cancellationToken);
            return batchId;
        }

        var index = 0;
        foreach (var item in itemList)
        {
            var (itemType, itemJson) = _serializer.Serialize(item);
            var envelope = new MapTaskEnvelope
            {
                BatchId = batchId,
                Index = index++,
                ItemType = itemType,
                ItemJson = itemJson,
            };

            // Threading the shared progress-notification id makes every map task report to one aggregate bar.
            await _backgroundJob.Enqueue(envelope,
                new EnqueueOptions { Queue = options?.Queue, ProgressNotificationId = progressNotificationId },
                cancellationToken);
        }

        return batchId;
    }

    private async Task<string?> TryCreateProgressNotificationAsync(MapReduceOptions? options, int total, string? userName)
    {
        if (options?.ReportProgress != true)
        {
            return null;
        }

        var notification = new JobProgressPushNotification(userName ?? "system")
        {
            Title = "Map/reduce job",
            Description = "Queued",
            Started = DateTime.UtcNow,
            TotalCount = total,
        };
        await _pushNotificationManager.SendAsync(notification);

        return notification.Id;
    }
}
