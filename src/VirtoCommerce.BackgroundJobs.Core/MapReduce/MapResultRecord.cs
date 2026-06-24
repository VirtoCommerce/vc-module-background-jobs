namespace VirtoCommerce.BackgroundJobs.Core.MapReduce;

/// <summary>
/// The persisted, type-erased form of a single map item's outcome (stored by <see cref="IMapReduceBatchStore"/> and
/// re-hydrated into <see cref="MapResult{TResult}"/> at reduce time). Keyed within a batch by <see cref="Index"/>, so
/// a redelivered map task overwrites rather than duplicates — making the join idempotent under at-least-once delivery.
/// </summary>
public sealed class MapResultRecord
{
    public int Index { get; set; }

    public bool Succeeded { get; set; }

    /// <summary>Serialized map result JSON when <see cref="Succeeded"/>; null otherwise.</summary>
    public string? ResultJson { get; set; }

    public string? Error { get; set; }
}
