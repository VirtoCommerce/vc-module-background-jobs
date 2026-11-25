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

    /// <summary>
    /// As <see cref="Create(IPushNotificationManager, JobEnvelope, string)"/>, with the progress the context carries
    /// replaced by <paramref name="wrapProgress"/> of it (an engine decorating it, e.g. with its own console). The
    /// context is built once, so what it knows about the attempt (whether it is the final one) survives the wrapping.
    /// </summary>
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
