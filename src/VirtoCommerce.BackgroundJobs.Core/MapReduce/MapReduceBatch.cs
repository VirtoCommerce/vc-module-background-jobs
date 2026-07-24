using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs.Core.MapReduce;

/// <summary>
/// Immutable per-batch metadata recorded once at enqueue and read by the coordinators. The item/result/state type
/// names let the coordinators resolve the user handlers (by closed <c>IMapJobHandler&lt;,&gt;</c> /
/// <c>IReduceJobHandler&lt;,&gt;</c>) and (de)serialize payloads without repeating type info on every map message.
/// </summary>
public sealed class MapReduceBatch
{
    public string BatchId { get; set; } = string.Empty;

    /// <summary>Number of map items fanned out.</summary>
    public int Total { get; set; }

    public string ItemType { get; set; } = string.Empty;

    public string ResultType { get; set; } = string.Empty;

    public string StateType { get; set; } = string.Empty;

    /// <summary>Assembly-qualified name of the concrete map handler to run (handler-explicit enqueue). When set, the
    /// map coordinator resolves this type; otherwise it resolves <c>IMapJobHandler&lt;ItemType, ResultType&gt;</c>.</summary>
    public string? MapHandlerType { get; set; }

    /// <summary>Assembly-qualified name of the concrete reduce handler to run. When set, the reduce coordinator
    /// resolves this type; otherwise it resolves <c>IReduceJobHandler&lt;StateType, ResultType&gt;</c>.</summary>
    public string? ReduceHandlerType { get; set; }

    /// <summary>Serialized reduce state passed to the reduce handler.</summary>
    public string StateJson { get; set; } = string.Empty;

    public string? Queue { get; set; }

    /// <summary>Progress-notification title, re-applied when the reduce step marks the batch finished.</summary>
    public string? Title { get; set; }

    public FailurePolicy FailurePolicy { get; set; }

    /// <summary>Shared progress-notification id when the batch reports progress; null otherwise.</summary>
    public string? ProgressNotificationId { get; set; }

    public string? UserName { get; set; }
}
