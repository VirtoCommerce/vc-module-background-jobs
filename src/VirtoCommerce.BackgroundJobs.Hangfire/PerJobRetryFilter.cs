#nullable enable
using System;
using System.Linq;
using Hangfire.Common;
using Hangfire.States;
using Newtonsoft.Json;
using VirtoCommerce.BackgroundJobs.Core.Models;

namespace VirtoCommerce.BackgroundJobs.Hangfire;

/// <summary>
/// Honors the per-job <see cref="JobEnvelope.MaxRetryAttempts"/> (set through <c>EnqueueOptions.MaxRetryAttempts</c>)
/// on top of the engine-wide <see cref="global::Hangfire.AutomaticRetryAttribute"/>. Hangfire has no per-job retry
/// count: the global filter decides for every job alike, and an attribute cannot be attached to a single enqueue. So
/// this filter runs <b>after</b> it (filter order 30 vs 20) and undoes a reschedule the job did not ask for,
/// putting the original failure back — the replacement for Hangfire's <c>[AutomaticRetry(Attempts = N)]</c>.
/// <para>
/// Only jobs enqueued by this engine (whose body is <see cref="HangfireJobExecutor"/>) are inspected; anything else
/// keeps the global behavior untouched. The envelope is deserialized only when a retry was actually elected, i.e. on
/// failure, never on the success path.
/// </para>
/// </summary>
public sealed class PerJobRetryFilter : JobFilterAttribute, IElectStateFilter
{
    // Job parameter AutomaticRetryAttribute writes the current attempt number to; it has already been incremented for
    // this failure by the time this filter runs.
    private const string _retryAttemptParameter = "RetryAttempt";

    public PerJobRetryFilter()
    {
        // After AutomaticRetryAttribute (20), so CandidateState is the reschedule it elected and can be overridden.
        Order = 30;
    }

    public void OnStateElection(ElectStateContext context)
    {
        // A reschedule is the only thing worth overriding: any other election (Failed, Succeeded, Deleted) already
        // matches "do not retry".
        if (context.CandidateState is not ScheduledState)
        {
            return;
        }

        var maxRetryAttempts = GetMaxRetryAttempts(context.BackgroundJob?.Job);
        if (maxRetryAttempts is null)
        {
            // No per-job override — the global AutomaticRetryAttribute governs, as before.
            return;
        }

        var retryAttempt = context.GetJobParameter<int>(_retryAttemptParameter);
        if (retryAttempt <= maxRetryAttempts)
        {
            return;
        }

        // Restore the failure AutomaticRetryAttribute replaced, so the job lands in Failed with its real exception
        // rather than being retried past the limit this job asked for.
        var failedState = context.TraversedStates.OfType<FailedState>().FirstOrDefault();

        context.CandidateState = failedState
            ?? new FailedState(new InvalidOperationException(
                $"Job failed and exceeded its per-job retry limit of {maxRetryAttempts}."));
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
