namespace VirtoCommerce.BackgroundJobs.Core.MapReduce;

/// <summary>
/// One serialized map item, stored by <see cref="IMapReduceBatchStore"/> at enqueue time so the fan-out can run on a
/// worker (the request doesn't enqueue every map task itself). The item's index is its position in the stored list.
/// </summary>
public sealed class MapItem
{
    public string ItemType { get; set; } = string.Empty;

    public string ItemJson { get; set; } = string.Empty;
}
