namespace VirtoCommerce.BackgroundJobs.Core;

/// <summary>Role this platform instance plays for background processing (set via configuration).</summary>
public enum BackgroundJobsMode
{
    /// <summary>Only enqueues jobs; does not process them (no Hangfire server / no RabbitMQ consumer).</summary>
    Producer,

    /// <summary>Only processes jobs (Hangfire background-job server / in-process RabbitMQ consumer).</summary>
    Worker,

    /// <summary>Enqueues and processes jobs (default).</summary>
    Both,
}

/// <summary>
/// Engine-agnostic background-processing options, bound from <c>VirtoCommerce:BackgroundJobs</c>.
/// Provider-specific settings live in their own sections (<c>VirtoCommerce:Hangfire</c>, <c>VirtoCommerce:RabbitMQ</c>).
/// </summary>
public sealed class BackgroundJobsOptions
{
    /// <summary>Active engine for this instance: <c>Hangfire</c> (default) or <c>RabbitMQ</c>. One per instance.</summary>
    public string Provider { get; set; } = "Hangfire";

    /// <summary>Instance role: Producer, Worker, or Both (default).</summary>
    public BackgroundJobsMode Mode { get; set; } = BackgroundJobsMode.Both;

    /// <summary>Default queue used when an enqueue doesn't specify one.</summary>
    public string DefaultQueue { get; set; } = "default";

    /// <summary>Default maximum automatic retry attempts on failure.</summary>
    public int MaxRetryAttempts { get; set; } = 3;
}
