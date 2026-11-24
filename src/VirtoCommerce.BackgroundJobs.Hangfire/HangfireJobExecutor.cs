#nullable enable
using System;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Server;
using Newtonsoft.Json;
using VirtoCommerce.BackgroundJobs;
using VirtoCommerce.BackgroundJobs.Core.Models;
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
    [HandlerJobDisplayName]
    public async Task Execute(string envelopeJson, PerformContext? performContext, CancellationToken cancellationToken)
    {
        var envelope = JsonConvert.DeserializeObject<JobEnvelope>(envelopeJson)
            ?? throw new InvalidOperationException("Failed to deserialize the background-job envelope.");

        var jobId = performContext?.BackgroundJob?.Id ?? string.Empty;

        // Hangfire keeps the retry bookkeeping, not the envelope: RetryCount is how many retries this job has already
        // had (read from the parameters snapshot taken when the worker picked it up, as AutomaticRetryAttribute does),
        // so this run is attempt RetryCount + 1. A job requeued after it exhausted its retries starts from the spent
        // count and so counts as its last attempt, which is what makes a failure there close the notification.
        var enriched = performContext?.BackgroundJob is null
            ? envelope
            : envelope with
            {
                Attempt = performContext.GetJobParameter<int>(PerJobRetryFilterAttribute.RetryCountParameter, allowStale: true) + 1,
                TotalAttempts = HangfireGlobalRetry.AttemptsFor(envelope) + 1,
            };

        // Shared factory builds the progress context, so the notification title is threaded consistently across engines.
        // The context and the dispatcher must see the same envelope: the notification shows the attempt the context was built with.
        var context = JobExecutionContextFactory.Create(pushNotificationManager, enriched, jobId);

        await dispatcher.Dispatch(enriched, context, cancellationToken);
    }
}
