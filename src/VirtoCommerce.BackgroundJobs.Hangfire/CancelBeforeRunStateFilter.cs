#nullable enable
using System;
using System.Linq;
using Hangfire;
using Hangfire.Common;
using Hangfire.Logging;
using Hangfire.States;
using Hangfire.Storage;
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

    /// <summary>
    /// Enqueued: not picked up yet. Scheduled: a delayed job or a retry wait. Awaiting: a continuation not yet released.
    /// </summary>
    internal static readonly string[] NotYetRunStates = [EnqueuedState.StateName, ScheduledState.StateName, AwaitingState.StateName];

    public void OnStateApplied(ApplyStateContext context, IWriteOnlyTransaction transaction)
    {
        try
        {
            if (context?.NewState is not DeletedState || !HasNotRun(context.OldStateName))
            {
                return;
            }

            var backgroundJob = context.BackgroundJob;

            // An envelope we cannot read names no notification to close.
            var envelope = HangfireJobExecutor.TryReadEnvelope(backgroundJob?.Job);
            if (backgroundJob is null || envelope is null || string.IsNullOrEmpty(envelope.ProgressNotificationId))
            {
                return;
            }

            // The envelope does not know which attempt was waiting, and its defaults (attempt 1, no total) would
            // overwrite the numbers earlier attempts wrote. None is stored until the job has failed once.
            var retryCount = context.GetJobParameter<int>(PerJobRetryFilterAttribute.RetryCountParameter);

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

    private static bool HasNotRun(string? oldStateName)
        => oldStateName is not null && NotYetRunStates.Contains(oldStateName);
}
