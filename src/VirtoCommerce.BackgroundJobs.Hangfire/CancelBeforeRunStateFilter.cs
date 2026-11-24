#nullable enable
using System;
using Hangfire;
using Hangfire.Common;
using Hangfire.Logging;
using Hangfire.States;
using Hangfire.Storage;
using Newtonsoft.Json;
using VirtoCommerce.BackgroundJobs.Core.Models;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.PushNotifications;

namespace VirtoCommerce.BackgroundJobs.Hangfire;

/// <summary>
/// Closes the progress notification of a job that is moved to <c>Deleted</c> without ever being executed, or while it
/// waits for a retry. <see cref="HangfireJobExecutor"/> closes a canceled job it is running; a job that is only queued,
/// scheduled or awaiting has no executor, so nothing else would close its notification and the admin UI would show it
/// open for good.
/// <para>
/// A job deleted while <c>Processing</c> is left alone on purpose: the executor owns it, and
/// <c>AutomaticRetryAttribute</c> with <c>OnAttemptsExceeded = Delete</c> also moves a <c>Processing</c> job to
/// <c>Deleted</c> after the dispatcher has already closed it <c>Failed</c>.
/// </para>
/// <para>
/// Runs inside Hangfire's state transition, so it sends synchronously and never throws: a notification that cannot be
/// closed must not fail the transition.
/// </para>
/// </summary>
public sealed class CancelBeforeRunStateFilter(IPushNotificationManager pushNotificationManager) : JobFilterAttribute, IApplyStateFilter
{
    private static readonly ILog _logger = LogProvider.For<CancelBeforeRunStateFilter>();

    public void OnStateApplied(ApplyStateContext context, IWriteOnlyTransaction transaction)
    {
        try
        {
            if (context?.NewState is not DeletedState || !HasNotRun(context.OldStateName))
            {
                return;
            }

            var backgroundJob = context.BackgroundJob;
            var job = backgroundJob?.Job;
            if (backgroundJob is null || job?.Type != typeof(HangfireJobExecutor) || job.Args is not { Count: > 0 } args || args[0] is not string envelopeJson)
            {
                return;
            }

            var envelope = ReadEnvelope(envelopeJson);
            if (envelope is null || string.IsNullOrEmpty(envelope.ProgressNotificationId))
            {
                return;
            }

            // The envelope does not know which attempt was waiting, and its defaults (attempt 1, no total) would
            // overwrite the numbers earlier attempts wrote. The stored value is serialized, as PerformContext reads it;
            // none is stored until the job has failed once.
            var retryCount = SerializationHelper.Deserialize<int>(
                context.Connection.GetJobParameter(backgroundJob.Id, PerJobRetryFilterAttribute.RetryCountParameter),
                SerializationOption.User);

            // The reporter decides whether the notification is the job's to close (a shared map/reduce bar is not).
            // Carrying the Hangfire job id keeps the JobId earlier attempts wrote: storage replaces a notification by id.
            new PushNotificationJobProgress(pushNotificationManager, HangfireGlobalRetry.WithAttemptBudget(envelope, retryCount), backgroundJob.Id)
                .Complete(JobOutcome.Canceled)
                .GetAwaiter()
                .GetResult();
        }
        catch (Exception ex)
        {
            _logger.ErrorException(
                $"Failed to close the progress notification of the job '{context?.BackgroundJob?.Id}' canceled before it ran.",
                ex);
        }
    }

    public void OnStateUnapplied(ApplyStateContext context, IWriteOnlyTransaction transaction)
    {
    }

    // Enqueued: not picked up yet. Scheduled: a delayed job or a retry wait. Awaiting: a continuation not yet released.
    private static bool HasNotRun(string? oldStateName)
        => oldStateName == EnqueuedState.StateName || oldStateName == ScheduledState.StateName || oldStateName == AwaitingState.StateName;

    private static JobEnvelope? ReadEnvelope(string envelopeJson)
    {
        try
        {
            return JsonConvert.DeserializeObject<JobEnvelope>(envelopeJson);
        }
        catch (JsonException)
        {
            // An envelope we cannot read names no notification to close.
            return null;
        }
    }
}
