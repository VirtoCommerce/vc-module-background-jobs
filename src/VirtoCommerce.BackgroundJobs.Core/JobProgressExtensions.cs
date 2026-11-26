using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using VirtoCommerce.BackgroundJobs.Core.Notifications;
using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs;

public static class JobProgressExtensions
{
    /// <summary>
    /// Closes the job's progress notification as <see cref="JobOutcome.Canceled"/>; a failed close is logged and
    /// swallowed, leaving the notification open.
    /// </summary>
    public static async Task TryCompleteCanceledAsync(
        this IJobProgress progress,
        ILogger logger,
        string jobId,
        CancellationToken cancellationToken = default)
    {
        await BestEffortNotificationSend.TrySendAsync(
            () => progress.Complete(JobOutcome.Canceled, null, cancellationToken),
            ex => logger.LogWarning(ex, "Could not close the progress notification of the cancelled job {JobId}; it stays open.", jobId));
    }
}
