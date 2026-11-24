#nullable enable
using System;
using Hangfire;
using Hangfire.Common;
using Hangfire.Logging;
using Hangfire.States;
using Hangfire.Storage;
using Newtonsoft.Json;
using VirtoCommerce.BackgroundJobs.Core.Models;
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

            var job = context.BackgroundJob?.Job;
            if (job?.Type != typeof(HangfireJobExecutor) || job.Args is not { Count: > 0 } args || args[0] is not string envelopeJson)
            {
                return;
            }

            var envelope = ReadEnvelope(envelopeJson);
            if (envelope is null)
            {
                return;
            }

            var closing = PushNotificationJobProgress.BuildClosingForJobThatDidNotRun(pushNotificationManager, envelope, JobOutcome.Canceled);
            if (closing is not null)
            {
                pushNotificationManager.Send(closing);
            }
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
