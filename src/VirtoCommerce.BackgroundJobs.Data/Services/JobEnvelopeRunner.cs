#nullable enable
using System;
using System.Threading;
using System.Threading.Tasks;
using VirtoCommerce.BackgroundJobs.Core.Models;
using VirtoCommerce.BackgroundJobs;
using VirtoCommerce.Platform.Core.PushNotifications;

namespace VirtoCommerce.BackgroundJobs.Data.Services;

/// <summary>
/// Default <see cref="IJobEnvelopeRunner"/>: rebuilds the execution context (progress) from the envelope via the
/// shared <see cref="JobExecutionContextFactory"/> and runs the handler through <see cref="IJobDispatcher"/> — the
/// same execution path the Hangfire executor and RabbitMQ consumer use. Stateless; registered as a singleton.
/// </summary>
public sealed class JobEnvelopeRunner(
    IJobDispatcher dispatcher,
    IPushNotificationManager pushNotificationManager) : IJobEnvelopeRunner
{
    public Task Run(JobEnvelope envelope, string jobId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        var context = JobExecutionContextFactory.Create(pushNotificationManager, envelope, jobId);
        return dispatcher.Dispatch(envelope, context, cancellationToken);
    }
}
