using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using VirtoCommerce.BackgroundJobs.Core.MapReduce;
using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs.SampleModule.Jobs.Indexing;

/// <summary>
/// REDUCE: runs once after every page reached a terminal state. Aggregates the per-page counts and the failed ids.
/// A real handler might swap an index alias, warm a cache, or enqueue a targeted re-index of just the failed ids.
/// </summary>
public sealed class IndexSummaryReducer(ILogger<IndexSummaryReducer> logger)
    : IReduceJobHandler<IndexSummary, IndexPageResult>
{
    public Task Reduce(IndexSummary state, IReadOnlyCollection<MapResult<IndexPageResult>> results,
        IJobExecutionContext context, CancellationToken cancellationToken = default)
    {
        var indexed = results.Where(r => r.Succeeded).Sum(r => r.Value!.Indexed);
        var failedIds = results
            .Where(r => r.Succeeded)
            .SelectMany(r => r.Value!.FailedIds)
            .Concat(results.Where(r => !r.Succeeded).Select(r => $"page#{r.Index}"))
            .ToArray();

        var elapsed = TimeSpan.FromTicks(DateTime.UtcNow.Ticks - state.StartedTicksUtc);
        logger.LogInformation("Indexed {Type}: {Indexed} ok, {Failed} failed, across {Pages} pages in {Elapsed}.",
            state.DocumentType, indexed, failedIds.Length, results.Count, elapsed);

        return Task.CompletedTask;
    }
}
