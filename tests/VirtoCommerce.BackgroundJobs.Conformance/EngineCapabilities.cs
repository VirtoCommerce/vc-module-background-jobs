namespace VirtoCommerce.BackgroundJobs.Conformance;

/// <summary>
/// Declares which optional parts of the background-job standard an engine supports, so the conformance suite asserts
/// the documented behavior for supported features and the documented fallback (or skips) for the rest. Required
/// behavior — message enqueue, handler dispatch, payload fidelity, user context, progress, "a failing job does not
/// kill the worker" — is verified for every engine and is not represented here.
/// </summary>
public sealed record EngineCapabilities
{
    /// <summary><c>GetStatus</c> returns a meaningful, queryable status for an enqueued job (Hangfire, Google Cloud
    /// Tasks). When false the engine keeps no job ledger and must never falsely report completion (RabbitMQ).</summary>
    public bool SupportsStatusQuery { get; init; }

    /// <summary><c>Delete</c> can cancel/remove a known job (Hangfire, Google Cloud Tasks). All engines must return
    /// false for an unknown id regardless of this flag.</summary>
    public bool SupportsDelete { get; init; }

    /// <summary>Re-enqueuing with the same <c>EnqueueOptions.UniqueKey</c> collapses to a single execution (Google
    /// Cloud Tasks). Skipped when unsupported.</summary>
    public bool SupportsUniqueKeyDedup { get; init; }

    /// <summary>A job enqueued onto a non-default queue is still processed on this instance (the worker drains the
    /// custom queue). Skipped when unsupported.</summary>
    public bool SupportsQueueRouting { get; init; }

    /// <summary>A handler that throws is retried (up to <c>MaxRetryAttempts</c>) so a transient failure eventually
    /// succeeds. Skipped when unsupported.</summary>
    public bool SupportsRetry { get; init; }

    /// <summary>The engine registers an <c>IRecurringJobScheduler</c> (native, like Hangfire, or the in-process
    /// generic scheduler). When false the recurring-scheduler smoke test is skipped.</summary>
    public bool SupportsRecurringScheduler { get; init; }
}
