#nullable enable
using System;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.States;
using Hangfire.Storage;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Newtonsoft.Json;
using VirtoCommerce.BackgroundJobs;
using VirtoCommerce.BackgroundJobs.Core.Models;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.PushNotifications;
using HangfireJob = Hangfire.Common.Job;

namespace VirtoCommerce.BackgroundJobs.Hangfire;

/// <summary>
/// The Hangfire-invoked body for a message job: rebuilds the execution context (progress) from the envelope and
/// runs the shared <see cref="IJobDispatcher"/>. <see cref="PerformContext"/> and <see cref="CancellationToken"/>
/// are substituted by Hangfire at execution time.
/// </summary>
public sealed class HangfireJobExecutor(
    IJobDispatcher dispatcher,
    IPushNotificationManager pushNotificationManager,
    ILogger<HangfireJobExecutor> logger)
{
    private static readonly ILog _logger = LogProvider.For<HangfireJobExecutor>();

    public HangfireJobExecutor(IJobDispatcher dispatcher, IPushNotificationManager pushNotificationManager)
        : this(dispatcher, pushNotificationManager, NullLogger<HangfireJobExecutor>.Instance)
    {
    }

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
            : HangfireGlobalRetry.WithAttemptBudget(
                envelope,
                performContext.GetJobParameter<int>(PerJobRetryFilterAttribute.RetryCountParameter, allowStale: true));

        // Shared factory builds the progress context, so the notification title is threaded consistently across engines.
        // The context and the dispatcher must see the same envelope: the notification shows the attempt the context was built with.
        var innerContext = JobExecutionContextFactory.Create(pushNotificationManager, enriched, jobId);

        // The console wrapper sits around the push-notification progress and NoOpJobProgress alike, so the dispatcher's
        // outcome and the cancel close below both reach the console and the wrapped progress through it.
        var context = new JobExecutionContext(
            innerContext.JobId,
            new ConsoleJobProgress(innerContext.Progress, performContext, logger),
            innerContext.Headers);

        try
        {
            await dispatcher.Dispatch(enriched, context, cancellationToken);
        }
        catch (Exception ex) when (cancellationToken.IsCancellationRequested && (ex is OperationCanceledException || !enriched.IsFinalAttempt()))
        {
            // The dispatcher keeps the notification open here: a requested cancel, a host shutdown and an ownership
            // change all trip this token, and only Hangfire's stored state tells them apart. Any exception counts, not
            // only OperationCanceledException: a handler may fail with another one once its token trips, and the failed
            // attempt then looks like a retry is pending although the job was deleted and no retry will follow.
            // On the final attempt that holds only for OperationCanceledException: any other exception was already
            // closed Failed with its error by the dispatcher, and that error is the only record of why the job ended.
            await CloseIfDeleted(performContext, context, jobId);

            throw;
        }
    }

    /// <summary>
    /// The envelope of a job whose body is <see cref="Execute"/>, or null for any other job or an envelope that cannot
    /// be read. The engine enqueues exactly one shape of job, <c>Execute(envelopeJson, context, token)</c>, so the
    /// filters that inspect a Hangfire job read it here.
    /// </summary>
    internal static JobEnvelope? TryReadEnvelope(HangfireJob? job)
    {
        if (job?.Type != typeof(HangfireJobExecutor) || job.Args.Count == 0 || job.Args[0] is not string envelopeJson)
        {
            return null;
        }

        try
        {
            return JsonConvert.DeserializeObject<JobEnvelope>(envelopeJson);
        }
        catch (JsonException)
        {
            return null;
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
            if (performContext?.Storage is null || jobId.Length == 0)
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
                await context.Progress.Complete(JobOutcome.Canceled);
            }
        }
        catch (Exception ex)
        {
            _logger.WarnException($"Could not close the progress notification of the canceled job '{jobId}'; it stays open.", ex);
        }
    }
}
