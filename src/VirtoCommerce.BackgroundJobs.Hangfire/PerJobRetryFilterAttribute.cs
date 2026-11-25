#nullable enable
using System;
using System.Linq;
using Hangfire;
using Hangfire.Common;
using Hangfire.States;
using Newtonsoft.Json;
using VirtoCommerce.BackgroundJobs.Core.Models;

namespace VirtoCommerce.BackgroundJobs.Hangfire;

/// <summary>
/// Honors the per-job <see cref="JobEnvelope.MaxRetryAttempts"/> (set through <c>EnqueueOptions.MaxRetryAttempts</c>)
/// on top of the engine-wide <see cref="global::Hangfire.AutomaticRetryAttribute"/>. Hangfire has no per-job retry
/// count: the global filter decides for every job alike, and an attribute cannot be attached to a single enqueue. So
/// this filter runs <b>after</b> it (filter order 30 vs 20) and corrects what it elected — the replacement for
/// Hangfire's <c>[AutomaticRetry(Attempts = N)]</c>:
/// <list type="bullet">
/// <item><description><b>Lower</b>: the global filter rescheduled the job (a <see cref="ScheduledState"/>, or an
/// <see cref="EnqueuedState"/> when the retry has no delay) past the job's limit — the original failure is put back.</description></item>
/// <item><description><b>Raise</b>: the global filter gave up (<see cref="FailedState"/>) before the job's limit — a copy
/// of the global attribute with the job's limit is applied, so Hangfire's own delay formula and <c>RetryCount</c>
/// bookkeeping schedule the retry.</description></item>
/// </list>
/// <para>
/// Only jobs enqueued by this engine (whose body is <see cref="HangfireJobExecutor"/>) are inspected, and only those
/// with a per-job value; anything else keeps the global behavior untouched. The cheap checks (the candidate state)
/// come first, and the envelope is deserialized only when a retry was elected or the job failed, never on the success
/// path.
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
        // A retry the global filter elected: a ScheduledState, or an EnqueuedState when its delay is zero. The Enqueued
        // candidate also arises from an ordinary enqueue, so it counts only when a failure was traversed to reach it.
        var retryElected = context.CandidateState is ScheduledState
            || (context.CandidateState is EnqueuedState && context.TraversedStates.Any(x => x is FailedState));
        var failed = context.CandidateState is FailedState;

        // Anything else (Succeeded, Deleted, Processing, ...) is not a retry decision.
        if (!retryElected && !failed)
        {
            return;
        }

        var maxRetryAttempts = GetMaxRetryAttempts(context.BackgroundJob?.Job);
        if (maxRetryAttempts is null)
        {
            // No per-job override — the global AutomaticRetryAttribute governs, as before.
            return;
        }

        var limit = Math.Max(0, maxRetryAttempts.Value);

        // allowStale: false on purpose. When AutomaticRetryAttribute elected a retry it wrote the incremented count
        // through the storage connection moments ago; the job's parameter snapshot still holds the value from before
        // this failure. When it gave up it wrote nothing, and storage and snapshot agree.
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
    /// The global filter gave up before this job's limit: elect the retry it would have elected had its count been the
    /// job's. A copy of the registered attribute does it, from the failure alone, so the delays, the exception filters
    /// and the <c>RetryCount</c> bookkeeping are Hangfire's own.
    /// </summary>
    private static void RaiseRetryLimit(ElectStateContext context, int limit)
    {
        var global = HangfireGlobalRetry.Current();
        if (global is null)
        {
            // No retry filter is registered, so there is no backoff to copy: the failure stands.
            return;
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

    private static int? GetMaxRetryAttempts(Job? job)
    {
        // The engine enqueues exactly one shape of job: HangfireJobExecutor.Execute(envelopeJson, context, token).
        if (job?.Type != typeof(HangfireJobExecutor) || job.Args.Count == 0 || job.Args[0] is not string envelopeJson)
        {
            return null;
        }

        try
        {
            return JsonConvert.DeserializeObject<JobEnvelope>(envelopeJson)?.MaxRetryAttempts;
        }
        catch (JsonException)
        {
            // An envelope we cannot read is not a reason to change the retry decision — fall back to the global one.
            return null;
        }
    }
}
