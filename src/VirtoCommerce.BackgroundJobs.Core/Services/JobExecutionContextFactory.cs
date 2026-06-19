using VirtoCommerce.BackgroundJobs.Core.Models;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.PushNotifications;

namespace VirtoCommerce.BackgroundJobs.Core.Services;

/// <summary>
/// Builds the <see cref="IJobExecutionContext"/> a worker passes to the dispatcher from a received
/// <see cref="JobEnvelope"/>. Shared by every processing host (the RabbitMQ consumer, the Google Cloud Tasks push
/// callback, …) so the progress-channel selection stays identical across engines.
/// </summary>
public static class JobExecutionContextFactory
{
    public static IJobExecutionContext Create(IPushNotificationManager pushNotificationManager, JobEnvelope envelope, string jobId)
    {
        IJobProgress progress = string.IsNullOrEmpty(envelope.ProgressNotificationId)
            ? NoOpJobProgress.Instance
            : new PushNotificationJobProgress(pushNotificationManager, envelope.ProgressNotificationId!, envelope.UserName);

        return new JobExecutionContext(jobId, progress, envelope.Headers);
    }
}
