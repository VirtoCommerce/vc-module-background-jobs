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
    string? creator) : IJobProgress
{
    public async Task Report(JobProgressInfo progress, CancellationToken cancellationToken = default)
    {
        var notification = new JobProgressPushNotification(creator ?? "system")
        {
            Id = notificationId,
            Description = progress.Message,
            ProcessedCount = progress.ProcessedCount ?? 0,
            TotalCount = progress.TotalCount ?? 0,
        };

        if (!string.IsNullOrEmpty(progress.Message))
        {
            notification.ProgressLog.Add(new ProgressMessage { Message = progress.Message });
        }

        await pushNotificationManager.SendAsync(notification);
    }
}
