using System;
using System.Threading;
using System.Threading.Tasks;
using VirtoCommerce.BackgroundJobs.Core.Notifications;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.Modularity;
using VirtoCommerce.Platform.Core.PushNotifications;

namespace VirtoCommerce.BackgroundJobs.Core.Services;

/// <summary>
/// <see cref="IJobProgress"/> that streams updates to the admin UI by sending a
/// <see cref="JobProgressPushNotification"/> (keyed by the enqueue-time notification id) over SignalR.
/// </summary>
public sealed class PushNotificationJobProgress(
    IPushNotificationManager pushNotificationManager,
    string notificationId,
    string? creator,
    string? title = null) : IJobProgress
{
    private long _processedCount;
    private long _totalCount;

    public async Task Report(JobProgressInfo progress, CancellationToken cancellationToken = default)
    {
        _processedCount = progress.ProcessedCount ?? _processedCount;
        _totalCount = progress.TotalCount ?? _totalCount;

        // Push storage replaces the notification by id, so each update must carry the display fields (Title) or they
        // are lost — re-apply the title here so it doesn't reset to null after the enqueue-time notification.
        var notification = new JobProgressPushNotification(creator ?? "system")
        {
            Id = notificationId,
            Title = title,
            Description = progress.Message,
            ProcessedCount = _processedCount,
            TotalCount = _totalCount,
        };

        if (!string.IsNullOrEmpty(progress.Message))
        {
            notification.ProgressLog.Add(new ProgressMessage { Message = progress.Message });
        }

        await pushNotificationManager.SendAsync(notification);
    }

    /// <summary>
    /// Marks the notification finished (so the admin UI closes the live bar). Preserves the last reported counts and
    /// title; records an error entry when the job failed. Called by the dispatcher when the job owns its notification.
    /// </summary>
    public async Task Complete(string? error, CancellationToken cancellationToken = default)
    {
        var notification = new JobProgressPushNotification(creator ?? "system")
        {
            Id = notificationId,
            Title = title,
            Description = error ?? "Completed",
            ProcessedCount = _processedCount,
            TotalCount = _totalCount,
            Finished = DateTime.UtcNow,
        };

        if (!string.IsNullOrEmpty(error))
        {
            notification.ProgressLog.Add(new ProgressMessage { Level = ProgressMessageLevel.Error, Message = error });
        }

        await pushNotificationManager.SendAsync(notification);
    }
}
