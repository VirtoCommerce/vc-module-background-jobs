using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs.SampleModule.Jobs.Indexing;

/// <summary>
/// MAP: indexes one page, in parallel with every other page, on any worker. A real handler would call
/// <c>IIndexingManager.IndexDocuments(...)</c>; this stand-in simulates indexing (and treats ids prefixed "bad-" as
/// failures) so the sample stays self-contained — no dependency on the Search module.
/// </summary>
public sealed class IndexPageHandler(ILogger<IndexPageHandler> logger) : IMapJobHandler<IndexPage, IndexPageResult>
{
    // Number of map handlers running concurrently on THIS instance. Bounded by the engine's worker concurrency
    // (Hangfire WorkerCount, default ~ProcessorCount*5; RabbitMQ PrefetchCount, default 1) — map tasks share the
    // same worker pool as every other background job. Shared across all (transient) handler instances.
    private static int _running;

    public async Task<IndexPageResult> Map(IndexPage page, IJobExecutionContext context, CancellationToken cancellationToken = default)
    {
        var running = Interlocked.Increment(ref _running);
        logger.LogInformation(
            "HANDLE START job {JobId} ({Count} ids) — {Running} map handler(s) running in parallel on this instance.",
            context.JobId, page.DocumentIds.Length, running);

        try
        {
            var failed = page.DocumentIds.Where(id => id.StartsWith("bad-", StringComparison.OrdinalIgnoreCase)).ToArray();
            var indexed = page.DocumentIds.Length - failed.Length;

            await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken); // simulate work (async — never block a worker thread)

            // (real work would go here: indexer.IndexDocuments(page.DocumentType, page.DocumentIds, ct))
            return new IndexPageResult(indexed, failed);
        }
        finally
        {
            var remaining = Interlocked.Decrement(ref _running);
            logger.LogInformation("HANDLE END   job {JobId} — {Remaining} still running on this instance.",
                context.JobId, remaining);
        }
    }
}
