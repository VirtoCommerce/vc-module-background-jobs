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
    /// <summary>Active engine for this instance: <c>Hangfire</c> (default) or <c>RabbitMQ</c>. One per instance.
    /// This selects the engine behind the agnostic <see cref="Platform.Core.Jobs.IBackgroundJob"/> facade (and
    /// map/reduce + the platform's recurring jobs). Legacy modules that call Hangfire directly are governed by
    /// <see cref="EnableLegacyHangfire"/>, not this property.</summary>
    public string Provider { get; set; } = BackgroundJobsProviders.Hangfire;

    /// <summary>
    /// When <c>true</c> (default), Hangfire's infrastructure (storage, <c>IBackgroundJobClient</c>/
    /// <c>IRecurringJobManager</c>, the processing server on Worker/Both, the <c>/hangfire</c> dashboard and schema)
    /// is initialized <b>even when <see cref="Provider"/> is not Hangfire</b>, so modules that use the Hangfire API
    /// directly keep working alongside another active engine (e.g. RabbitMQ). Set <c>false</c> for a pure
    /// non-Hangfire instance (no server, no dashboard, no schema). When <see cref="Provider"/> is Hangfire this has
    /// no effect — Hangfire is always initialized as the active engine.
    /// </summary>
    public bool EnableLegacyHangfire { get; set; } = true;

    /// <summary>Instance role: Producer, Worker, or Both (default).</summary>
    public BackgroundJobsMode Mode { get; set; } = BackgroundJobsMode.Both;

    /// <summary>Default queue used when an enqueue doesn't specify one.</summary>
    public string DefaultQueue { get; set; } = "default";

    /// <summary>Default maximum automatic retry attempts on failure.</summary>
    public int MaxRetryAttempts { get; set; } = 3;
}
