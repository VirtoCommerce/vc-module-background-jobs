#nullable enable
using System;

namespace VirtoCommerce.BackgroundJobs.Core.Admin;

/// <summary>A registered background-job handler, as shown in the admin troubleshooting view.</summary>
public sealed record RegisteredJobInfo
{
    /// <summary>Friendly, addressable name (used by the execute-by-name endpoint).</summary>
    public required string Name { get; init; }

    /// <summary>Handler type full name.</summary>
    public required string HandlerType { get; init; }

    /// <summary>Payload contract type full name.</summary>
    public required string PayloadType { get; init; }

    /// <summary>Whether this job can be triggered on demand by name via the execute endpoint (false for internal plumbing).</summary>
    public bool Triggerable { get; init; }
}

/// <summary>A recurring/scheduled job with its effective schedule and last/next run, for the admin view.</summary>
public sealed record RecurringJobInfo
{
    /// <summary>Stable recurring-job id.</summary>
    public required string Id { get; init; }

    /// <summary>Effective cron expression (fixed, or resolved from settings); null when unset/disabled.</summary>
    public string? Cron { get; init; }

    /// <summary>Whether the schedule is currently enabled.</summary>
    public bool Enabled { get; init; }

    /// <summary>True when the schedule (enabler + cron) is driven by module settings rather than a fixed cron.</summary>
    public bool SettingDriven { get; init; }

    /// <summary>IANA/Windows time-zone id the cron is evaluated in.</summary>
    public string TimeZone { get; init; } = "UTC";

    /// <summary>Handler type name the schedule runs, when known.</summary>
    public string? HandlerType { get; init; }

    /// <summary>Payload type name the schedule enqueues, when known.</summary>
    public string? PayloadType { get; init; }

    /// <summary>Last enqueued occurrence (UTC), when the engine's state store tracks it; null otherwise.</summary>
    public DateTime? LastRunUtc { get; init; }

    /// <summary>Next computed occurrence (UTC) for the effective cron; null when disabled or cron invalid.</summary>
    public DateTime? NextRunUtc { get; init; }
}
