using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using VirtoCommerce.BackgroundJobs.Core.MapReduce;
using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs.SampleModule.Jobs.Indexing;

/// <summary>
/// MAP: indexes one page, in parallel with every other page, on any worker. A real handler would call
/// <c>IIndexingManager.IndexDocuments(...)</c>; this stand-in simulates indexing (and treats ids prefixed "bad-" as
/// failures) so the sample stays self-contained — no dependency on the Search module.
/// </summary>
public sealed class IndexPageHandler : IMapJobHandler<IndexPage, IndexPageResult>
{
    public Task<IndexPageResult> Map(IndexPage page, IJobExecutionContext context, CancellationToken cancellationToken = default)
    {
        var failed = page.DocumentIds.Where(id => id.StartsWith("bad-", StringComparison.OrdinalIgnoreCase)).ToArray();
        var indexed = page.DocumentIds.Length - failed.Length;

        // (real work would go here: indexer.IndexDocuments(page.DocumentType, page.DocumentIds, ct))
        return Task.FromResult(new IndexPageResult(indexed, failed));
    }
}
