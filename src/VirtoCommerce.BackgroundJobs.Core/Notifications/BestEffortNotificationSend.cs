using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace VirtoCommerce.BackgroundJobs.Core.Notifications;

/// <summary>
/// Runs a send to the notification channel that only displays a job: whatever the send throws is logged and swallowed,
/// so a failure to display never changes what the job does or replaces the error the caller is about to get.
/// </summary>
internal static class BestEffortNotificationSend
{
    public static async Task TrySendAsync(Func<Task> send, string operation, string? notificationId, ILogger logger)
    {
        try
        {
            await send();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Could not {Operation} (notification {NotificationId}); the job's outcome is unchanged.", operation, notificationId);
        }
    }
}
