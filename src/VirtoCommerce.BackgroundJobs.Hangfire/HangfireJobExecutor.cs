#nullable enable
using System;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Server;
using Newtonsoft.Json;
using VirtoCommerce.BackgroundJobs.Core.Models;
using VirtoCommerce.BackgroundJobs.Core.Services;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.PushNotifications;

namespace VirtoCommerce.BackgroundJobs.Hangfire;

/// <summary>
/// The Hangfire-invoked body for a message job: rebuilds the execution context (progress) from the envelope and
/// runs the shared <see cref="IJobDispatcher"/>. <see cref="PerformContext"/> and <see cref="CancellationToken"/>
/// are substituted by Hangfire at execution time.
/// </summary>
public sealed class HangfireJobExecutor(IJobDispatcher dispatcher, IPushNotificationManager pushNotificationManager)
{
    public async Task Execute(string envelopeJson, PerformContext? performContext, CancellationToken cancellationToken)
    {
        var envelope = JsonConvert.DeserializeObject<JobEnvelope>(envelopeJson)
            ?? throw new InvalidOperationException("Failed to deserialize the background-job envelope.");

        var jobId = performContext?.BackgroundJob?.Id ?? string.Empty;

        IJobProgress progress = string.IsNullOrEmpty(envelope.ProgressNotificationId)
            ? NoOpJobProgress.Instance
            : new PushNotificationJobProgress(pushNotificationManager, envelope.ProgressNotificationId!, envelope.UserName);

        var context = new JobExecutionContext(jobId, progress, envelope.Headers);

        await dispatcher.Dispatch(envelope, context, cancellationToken);
    }
}
