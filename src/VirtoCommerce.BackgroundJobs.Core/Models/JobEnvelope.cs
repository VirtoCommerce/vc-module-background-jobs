using System.Collections.Generic;

namespace VirtoCommerce.BackgroundJobs.Core.Models;

/// <summary>
/// The serialized unit of work that crosses the engine/queue boundary. Built by the enqueue facade from a job
/// payload and consumed by the dispatcher on the worker side.
/// </summary>
public sealed record JobEnvelope
{
    /// <summary>Assembly-qualified name of the handler payload (base) type — used to resolve <c>IBackgroundJob&lt;T&gt;</c>.</summary>
    public required string JobType { get; init; }

    /// <summary>Assembly-qualified name of the concrete payload type (may be an AbstractTypeFactory-derived type).</summary>
    public required string PayloadType { get; init; }

    /// <summary>JSON-serialized payload.</summary>
    public required string PayloadJson { get; init; }

    public string? Queue { get; init; }

    /// <summary>De-duplication key.</summary>
    public string? UniqueKey { get; init; }

    /// <summary>Push-notification id to report progress against, or null for fire-and-forget without progress.</summary>
    public string? ProgressNotificationId { get; init; }

    /// <summary>User name that enqueued the job (for the worker's user context).</summary>
    public string? UserName { get; init; }

    public int Attempt { get; init; } = 1;

    public IReadOnlyDictionary<string, string> Headers { get; init; } = new Dictionary<string, string>();
}
