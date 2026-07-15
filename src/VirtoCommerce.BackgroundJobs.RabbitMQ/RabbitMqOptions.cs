using System;
using System.Collections.Generic;

namespace VirtoCommerce.BackgroundJobs.RabbitMQ;

/// <summary>
/// Connection and consumer settings for the RabbitMQ background-job engine.
/// Bound from the top-level configuration section <c>VirtoCommerce:RabbitMQ</c> (provider-specific section, kept
/// separate from the engine-agnostic <c>VirtoCommerce:BackgroundJobs</c> selector).
/// </summary>
public class RabbitMqOptions
{
    /// <summary>
    /// Full AMQP connection string, e.g. <c>amqp://guest:guest@localhost:5672/</c>.
    /// When set, it takes precedence over the individual host/port/credential properties below.
    /// </summary>
    public string? Uri { get; set; }

    public string HostName { get; set; } = "localhost";

    public int Port { get; set; } = 5672;

    public string UserName { get; set; } = "guest";

    public string Password { get; set; } = "guest";

    public string VirtualHost { get; set; } = "/";

    /// <summary>
    /// Friendly connection name shown in the RabbitMQ management UI.
    /// </summary>
    public string ClientProvidedName { get; set; } = "VirtoCommerce.BackgroundJobs";

    /// <summary>
    /// URL of the RabbitMQ management UI, surfaced as a developer tool in the platform admin. When not set it
    /// defaults to <c>http://{HostName}:15672</c> (the management plugin's default endpoint).
    /// </summary>
    public string? ManagementUri { get; set; }

    /// <summary>
    /// Number of unacknowledged messages a single consumer prefetches (QoS) — also the throughput gate, since the
    /// broker never delivers more than this many unacked messages at once. <b>Left at the default 0 (or any value
    /// &lt;= 0) it auto-scales</b> to the CPU count the process sees (<c>ProcessorCount × <see cref="ConcurrencyPerCore"/></c>,
    /// clamped to <see cref="MaxAutoConcurrency"/>), so a worker self-sizes to its Kubernetes pod with no per-environment
    /// config. Set a positive value to pin it explicitly.
    /// </summary>
    public int PrefetchCount { get; set; }

    /// <summary>
    /// Number of consumer callbacks the client dispatches IN PARALLEL (RabbitMQ.Client's
    /// <c>ConsumerDispatchConcurrency</c>). This — together with <see cref="PrefetchCount"/> — is what makes jobs run
    /// concurrently on one instance: prefetch caps how many unacked messages the broker delivers, and this caps how
    /// many the client hands to the handler at once. <b>Left at the default 0 (or any value &lt;= 0) it auto-scales</b>,
    /// following the effective <see cref="PrefetchCount"/>, so raising prefetch alone increases parallelism as expected.
    /// Set a positive value to decouple the two (e.g. a high prefetch for throughput but a bounded number of concurrent
    /// handlers).
    /// </summary>
    public int ConsumerDispatchConcurrency { get; set; }

    /// <summary>
    /// Additional queues the in-process consumer drains. The engine-agnostic default queue
    /// (<c>VirtoCommerce:BackgroundJobs:DefaultQueue</c>) is always consumed; list extra queues here when jobs are
    /// enqueued onto non-default queues via <c>EnqueueOptions.Queue</c>.
    /// </summary>
    public List<string> Queues { get; set; } = [];

    /// <summary>
    /// When true (default), a job that exhausts its retries is routed to a dead-letter queue instead of being
    /// dropped, so it can be inspected or replayed. The dead-letter queue is the work queue name plus
    /// <see cref="DeadLetterQueueSuffix"/>.
    /// </summary>
    public bool UseDeadLetterQueue { get; set; } = true;

    /// <summary>Suffix appended to the work queue name to form its dead-letter queue (default <c>.dlq</c>).</summary>
    public string DeadLetterQueueSuffix { get; set; } = ".dlq";

    /// <summary>
    /// Fixed delay before a failed job is retried, in seconds (default 5). The retry is parked in a per-queue TTL
    /// "delay" queue (<c>{queue}.retry.{n}s</c>) that dead-letters back to the work queue when it expires, so a burst
    /// of poison messages backs off instead of hot-looping. Set 0 to retry immediately (re-publish straight to the
    /// work queue).
    /// </summary>
    public int RetryDelaySeconds { get; set; } = 5;

    /// <summary>Target concurrent handlers per CPU used when <see cref="PrefetchCount"/> auto-scales (i.e. is
    /// &lt;= 0). Default 10 suits IO-bound jobs; use ~1–2 for CPU-bound work.</summary>
    public int ConcurrencyPerCore { get; set; } = 10;

    /// <summary>Upper clamp for the auto-derived concurrency (guards against very large machines). Default 200.</summary>
    public int MaxAutoConcurrency { get; set; } = 200;

    /// <summary>
    /// The prefetch (QoS) count to apply: the explicit <see cref="PrefetchCount"/> when it is &gt; 0, otherwise
    /// (the default, or any value &lt;= 0) the auto-scaled <c>ProcessorCount × ConcurrencyPerCore</c> clamped to
    /// <see cref="MaxAutoConcurrency"/>. Never below 1; capped at <see cref="ushort.MaxValue"/>.
    /// </summary>
    public ushort EffectivePrefetchCount()
    {
        // Explicit value wins and is honored as-is (only bounded to the ushort wire range); MaxAutoConcurrency
        // only clamps the auto-derived value.
        if (PrefetchCount > 0)
        {
            return (ushort)Math.Clamp(PrefetchCount, 1, ushort.MaxValue);
        }

        var scaled = Math.Clamp(Environment.ProcessorCount * ConcurrencyPerCore, 1, Math.Min(MaxAutoConcurrency, ushort.MaxValue));
        return (ushort)scaled;
    }

    /// <summary>The parallel-dispatch count to apply: the explicit <see cref="ConsumerDispatchConcurrency"/> when it is
    /// &gt; 0, otherwise (the default, or any value &lt;= 0) it auto-scales by following
    /// <see cref="EffectivePrefetchCount"/>. Never below 1.</summary>
    public ushort EffectiveDispatchConcurrency()
    {
        if (ConsumerDispatchConcurrency > 0)
        {
            return (ushort)Math.Clamp(ConsumerDispatchConcurrency, 1, ushort.MaxValue);
        }

        return Math.Max((ushort)1, EffectivePrefetchCount());
    }
}
