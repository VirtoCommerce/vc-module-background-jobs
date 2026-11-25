using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs;

public static class JobProgressExtensions
{
    /// <summary>
    /// Closes the job's progress notification as <see cref="JobOutcome.Canceled"/>. A failure to close it must neither
    /// turn a requested cancel into a failure nor keep a queue message unacked (it would be redelivered and discarded
    /// again), so the notification stays open (the lesser harm), as when the engine does not close it at all.
    /// </summary>
    public static async Task TryCompleteCanceledAsync(
        this IJobProgress progress,
        ILogger logger,
        string jobId,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await progress.Complete(JobOutcome.Canceled, null, cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Could not close the progress notification of the cancelled job {JobId}; it stays open.", jobId);
        }
    }
}
