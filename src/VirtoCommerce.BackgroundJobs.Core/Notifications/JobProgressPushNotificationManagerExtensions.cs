using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using VirtoCommerce.Platform.Core.PushNotifications;

namespace VirtoCommerce.BackgroundJobs.Core.Notifications;

public static class JobProgressPushNotificationManagerExtensions
{
    /// <summary>Closes <paramref name="queued"/> as failed for a caller whose enqueue failed; a failed send is logged, never thrown, so the caller's exception survives.</summary>
    public static Task TryCloseEnqueueFailedAsync(this IPushNotificationManager manager, JobProgressPushNotification queued, string error, ILogger logger)
    {
        return BestEffortNotificationSend.TrySendAsync(
            () => manager.SendAsync(JobProgressPushNotification.CreateEnqueueFailed(queued, error)),
            ex => logger.LogError(ex, "Could not close the progress notification {NotificationId} of a job whose enqueue failed; the caller still gets the error.", queued.Id));
    }
}
