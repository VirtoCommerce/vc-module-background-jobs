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
/// Closes the progress notification of a job deleted before it ran or while it waited for a retry, which no executor
/// will close. Never throws: a failed close must not fail Hangfire's state transition.
/// </summary>
public sealed class CancelBeforeRunStateFilter(IPushNotificationManager pushNotificationManager) : JobFilterAttribute, IApplyStateFilter
{
    private static readonly ILog _logger = LogProvider.For<CancelBeforeRunStateFilter>();

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

            var envelope = HangfireJobExecutor.TryReadEnvelope(backgroundJob?.Job);
            if (backgroundJob is null || envelope is null || string.IsNullOrEmpty(envelope.ProgressNotificationId))
            {
                return;
            }

            var retryCount = context.GetJobParameter<int>(PerJobRetryFilterAttribute.RetryCountParameter);

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
