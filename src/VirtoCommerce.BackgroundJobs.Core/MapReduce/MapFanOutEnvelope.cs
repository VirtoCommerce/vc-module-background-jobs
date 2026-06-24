namespace VirtoCommerce.BackgroundJobs.Core.MapReduce;

/// <summary>
/// Payload of the single fan-out task (handled by <c>FanOutCoordinator</c>): on a worker it reads the stored items
/// for the batch and enqueues one map task per item. Keeps the enqueueing request fast for large batches.
/// </summary>
public sealed class MapFanOutEnvelope
{
    public string BatchId { get; set; } = string.Empty;
}
