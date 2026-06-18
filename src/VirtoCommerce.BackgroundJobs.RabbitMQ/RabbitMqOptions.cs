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
    /// Number of unacknowledged messages a single consumer prefetches (QoS). Defaults to 1 for fair dispatch.
    /// </summary>
    public ushort PrefetchCount { get; set; } = 1;

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
}
