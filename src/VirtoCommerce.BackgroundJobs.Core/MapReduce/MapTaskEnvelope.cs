namespace VirtoCommerce.BackgroundJobs.Core.MapReduce;

/// <summary>
/// Payload of one map task. Enqueued as an ordinary background job (handled by <c>MapCoordinator</c>), so it rides
/// the active engine like any other message. Carries only the batch id, the item's index, and the serialized item —
/// shared metadata lives in <see cref="MapReduceBatch"/>.
/// </summary>
public sealed class MapTaskEnvelope
{
    public string BatchId { get; set; } = string.Empty;

    public int Index { get; set; }

    /// <summary>Assembly-qualified concrete type of the item.</summary>
    public string ItemType { get; set; } = string.Empty;

    public string ItemJson { get; set; } = string.Empty;
}
