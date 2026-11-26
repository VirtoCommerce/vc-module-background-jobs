using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using VirtoCommerce.Platform.Core.PushNotifications;

namespace VirtoCommerce.BackgroundJobs.Core.Notifications;

public static class JobProgressPushNotificationManagerExtensions
{
    /// <summary>
    /// Closes the notification <paramref name="queued"/> as failed (<see cref="JobProgressPushNotification.CreateEnqueueFailed"/>),
    /// for a caller whose enqueue failed and that is about to rethrow. A failed send is logged and never thrown, so the
    /// caller's own exception is the one that survives.
    /// </summary>
    public static Task TryCloseEnqueueFailedAsync(this IPushNotificationManager manager, JobProgressPushNotification queued, string error, ILogger logger)
    {
        return BestEffortNotificationSend.TrySendAsync(
            () => manager.SendAsync(JobProgressPushNotification.CreateEnqueueFailed(queued, error)),
            ex => logger.LogError(ex, "Could not close the progress notification {NotificationId} of a job whose enqueue failed; the caller still gets the error.", queued.Id));
    }
}
