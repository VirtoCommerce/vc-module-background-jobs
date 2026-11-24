#nullable enable
using System;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.States;
using Hangfire.Storage;
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
    private static readonly ILog _logger = LogProvider.For<HangfireJobExecutor>();

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

        try
        {
            await dispatcher.Dispatch(enriched, context, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // The dispatcher keeps the notification open here: a requested cancel, a host shutdown and an ownership
            // change all trip this token, and only Hangfire's stored state tells them apart.
            await CloseIfDeleted(performContext, context, jobId);

            throw;
        }
    }

    // A cancel is a move to Deleted, and Hangfire trips a running job's token only after it has read a state other
    // than Processing, so a cancel reads Deleted here. A shutdown still reads Processing, an ownership change reads
    // Processing on another server and a requeue reads Enqueued: Hangfire runs the job again in each case, so the
    // notification stays open. A failure to read the state is treated as "not deleted": an open notification for a
    // canceled job is the lesser harm than a Canceled one for a job that runs again.
    private static async Task CloseIfDeleted(PerformContext? performContext, IJobExecutionContext context, string jobId)
    {
        try
        {
            if (context.Progress is not IJobProgressComplete complete || performContext?.Storage is null || jobId.Length == 0)
            {
                return;
            }

            string? stateName;
            using (var connection = performContext.Storage.GetConnection())
            {
                stateName = connection.GetStateData(jobId)?.Name;
            }

            if (stateName == DeletedState.StateName)
            {
                await complete.Complete(JobOutcome.Canceled, null);
            }
        }
        catch (Exception ex)
        {
            _logger.WarnException($"Could not close the progress notification of the canceled job '{jobId}'; it stays open.", ex);
        }
    }
}
