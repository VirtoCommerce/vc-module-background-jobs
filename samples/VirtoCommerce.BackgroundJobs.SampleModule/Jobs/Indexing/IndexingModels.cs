namespace VirtoCommerce.BackgroundJobs.SampleModule.Jobs.Indexing;

/// <summary>One unit of map work: a page of document ids to index. Fan out over pages (not individual documents) to
/// keep the map-task count and result size small.</summary>
public sealed record IndexPage(string DocumentType, string[] DocumentIds);

/// <summary>Map result — kept tiny: per-page counts, not the documents themselves.</summary>
public sealed record IndexPageResult(int Indexed, string[] FailedIds);

/// <summary>Reduce state carried across the whole batch.</summary>
public sealed record IndexSummary(string DocumentType, long StartedTicksUtc);
