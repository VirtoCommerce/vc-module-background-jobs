#nullable enable
using System;
using System.Linq;
using Hangfire;
using Hangfire.Common;
using Hangfire.States;
using VirtoCommerce.BackgroundJobs.Core.Models;

namespace VirtoCommerce.BackgroundJobs.Hangfire;

/// <summary>
/// Honors the per-job <see cref="JobEnvelope.MaxRetryAttempts"/> (set through <c>EnqueueOptions.MaxRetryAttempts</c>)
/// on top of the engine-wide <see cref="global::Hangfire.AutomaticRetryAttribute"/>. Hangfire has no per-job retry
/// count: the global filter decides for every job alike, and an attribute cannot be attached to a single enqueue. So
/// this filter runs <b>after</b> it (filter order 30 vs 20) and corrects what it elected — the replacement for
/// Hangfire's <c>[AutomaticRetry(Attempts = N)]</c>: it puts the failure back when the global filter would retry past
/// the job's limit, and elects a retry when the global filter gave up (Failed, or Deleted after a failure) before it.
/// <para>
/// Only jobs enqueued by this engine with a per-job value are inspected; anything else keeps the global behavior.
/// </para>
/// </summary>
public sealed class PerJobRetryFilterAttribute : JobFilterAttribute, IElectStateFilter
{
    /// <summary>
    /// Job parameter <see cref="global::Hangfire.AutomaticRetryAttribute"/> keeps the attempt number in; it has
    /// already been incremented for this failure by the time this filter runs. Hangfire's own name — asserted against
    /// the shipped assembly by a unit test, because a silent rename upstream would turn this filter into a no-op.
    /// </summary>
    public const string RetryCountParameter = "RetryCount";

    public PerJobRetryFilterAttribute()
    {
        // After AutomaticRetryAttribute (20), so CandidateState is the reschedule it elected and can be overridden.
        Order = 30;
    }

    public void OnStateElection(ElectStateContext context)
    {
        // Enqueued counts as a retry only after a traversed failure: an ordinary enqueue produces it too.
        var failureTraversed = context.TraversedStates.Any(x => x is FailedState);
        var retryElected = context.CandidateState is ScheduledState
            || (context.CandidateState is EnqueuedState && failureTraversed);
        var failed = context.CandidateState is FailedState;

        var deletedAfterFailure = context.CandidateState is DeletedState && failureTraversed;

        if (!retryElected && !failed && !deletedAfterFailure)
        {
            return;
        }

        var maxRetryAttempts = HangfireJobExecutor.TryReadEnvelope(context.BackgroundJob?.Job)?.MaxRetryAttempts;
        if (maxRetryAttempts is null)
        {
            // No per-job override — the global AutomaticRetryAttribute governs, as before.
            return;
        }

        var limit = Math.Max(0, maxRetryAttempts.Value);

        // allowStale: false on purpose. When AutomaticRetryAttribute elected a retry it wrote the incremented count
        // through the storage connection moments ago; the job's parameter snapshot still holds the value from before
        // this failure.
        var retryCount = context.GetJobParameter<int>(RetryCountParameter, allowStale: false);

        if (retryElected)
        {
            if (retryCount > limit)
            {
                RestoreFailure(context, limit);
            }
        }
        else if (retryCount < limit)
        {
            RaiseRetryLimit(context, limit);
        }
    }

    /// <summary>
    /// Put back the failure <see cref="global::Hangfire.AutomaticRetryAttribute"/> replaced, so the job lands in Failed
    /// with its real exception rather than being retried past the limit this job asked for.
    /// </summary>
    private static void RestoreFailure(ElectStateContext context, int limit)
    {
        var failedState = context.TraversedStates.OfType<FailedState>().FirstOrDefault();

        context.CandidateState = failedState
            ?? new FailedState(new InvalidOperationException(
                $"Job failed and exceeded its per-job retry limit of {limit}."));
    }

    /// <summary>
    /// Elects the retry the global filter would have elected with this job's limit, through a copy of the registered
    /// attribute so delays, exception filters and <c>RetryCount</c> bookkeeping stay Hangfire's own.
    /// </summary>
    private static void RaiseRetryLimit(ElectStateContext context, int limit)
    {
        var global = HangfireGlobalRetry.Current();
        if (global is null)
        {
            return;
        }

        if (context.CandidateState is DeletedState)
        {
            // The last failure traversed is the one the global filter deleted the job for.
            context.CandidateState = context.TraversedStates.OfType<FailedState>().Last();
        }

        // Every settable member of the attribute (Hangfire 1.8.23) is copied explicitly: a member left out would fall
        // back to the constructor default and silently change what this job's retry does.
        var raised = new AutomaticRetryAttribute
        {
            Attempts = limit,
            DelaysInSeconds = global.DelaysInSeconds,
            DelayInSecondsByAttemptFunc = global.DelayInSecondsByAttemptFunc,
            OnAttemptsExceeded = global.OnAttemptsExceeded,
            LogEvents = global.LogEvents,
            OnlyOn = global.OnlyOn,
            ExceptOn = global.ExceptOn,
            Order = global.Order,
        };

        raised.OnStateElection(context);
    }
}
