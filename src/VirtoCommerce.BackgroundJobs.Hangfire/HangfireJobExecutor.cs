#nullable enable
using System;
using System.Threading;
using System.Threading.Tasks;
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

        // Hangfire keeps the retry count, not the envelope: this run is attempt RetryCount + 1.
        var enriched = performContext?.BackgroundJob is null
            ? envelope
            : HangfireGlobalRetry.WithAttemptBudget(
                envelope,
                performContext.GetJobParameter<int>(PerJobRetryFilterAttribute.RetryCountParameter, allowStale: true));

        // Shared factory builds the progress context, so the notification title is threaded consistently across engines.
        var context = JobExecutionContextFactory.Create(
            pushNotificationManager, enriched, jobId, x => new ConsoleJobProgress(x, performContext, logger));

        try
        {
            await dispatcher.Dispatch(enriched, context, cancellationToken);
        }
        catch (Exception ex) when (cancellationToken.IsCancellationRequested && (ex is OperationCanceledException || !enriched.IsFinalAttempt()))
        {
            // Not only OperationCanceledException: a handler may throw another once its token trips — except on the final attempt, where the dispatcher's Failed close stands.
            await CloseIfDeleted(performContext, context, jobId);

            throw;
        }
    }

    /// <summary>
    /// The envelope of a job whose body is <see cref="Execute"/>, or null for any other job or an unreadable envelope.
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

    // Only a stored Deleted state is a cancel; a shutdown, an ownership change or a requeue runs the job again. A failed read counts as not deleted: better an open notification than Canceled on a job that reruns.
    private async Task CloseIfDeleted(PerformContext? performContext, IJobExecutionContext context, string jobId)
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
            logger.LogWarning(ex, "Could not close the progress notification of the canceled job {JobId}; it stays open.", jobId);
        }
    }
}
