using System;
using VirtoCommerce.BackgroundJobs.Core.Models;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.PushNotifications;

namespace VirtoCommerce.BackgroundJobs;

/// <summary>
/// Builds the <see cref="IJobExecutionContext"/> a worker passes to the dispatcher from a received
/// <see cref="JobEnvelope"/>. Shared by every processing host (the RabbitMQ consumer, the Google Cloud Tasks push
/// callback, …) so the progress-channel selection stays identical across engines.
/// </summary>
public static class JobExecutionContextFactory
{
    public static IJobExecutionContext Create(IPushNotificationManager pushNotificationManager, JobEnvelope envelope, string jobId)
        => Create(pushNotificationManager, envelope, jobId, x => x);

    /// <summary>As <see cref="Create(IPushNotificationManager, JobEnvelope, string)"/>, with the context's progress wrapped by <paramref name="wrapProgress"/>; the context keeps what it knows about the attempt.</summary>
    public static IJobExecutionContext Create(
        IPushNotificationManager pushNotificationManager,
        JobEnvelope envelope,
        string jobId,
        Func<IJobProgress, IJobProgress> wrapProgress)
    {
        ArgumentNullException.ThrowIfNull(wrapProgress);

        IJobProgress progress = string.IsNullOrEmpty(envelope.ProgressNotificationId)
            ? NoOpJobProgress.Instance
            : new PushNotificationJobProgress(pushNotificationManager, envelope, jobId);

        return new JobExecutionContext(jobId, wrapProgress(progress), envelope.Headers) { IsFinalAttempt = envelope.IsFinalAttempt() };
    }
}
