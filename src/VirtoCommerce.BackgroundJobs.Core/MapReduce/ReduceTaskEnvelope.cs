namespace VirtoCommerce.BackgroundJobs.Core.MapReduce;

/// <summary>
/// Payload of the single reduce task, enqueued (handled by <c>ReduceCoordinator</c>) exactly once after every map
/// item reaches a terminal state.
/// </summary>
public sealed class ReduceTaskEnvelope
{
    public string BatchId { get; set; } = string.Empty;
}
