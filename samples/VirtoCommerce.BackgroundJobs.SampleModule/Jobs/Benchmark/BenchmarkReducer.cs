using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs.SampleModule.Jobs.Benchmark;

/// <summary>
/// REDUCE: runs once after every map item reached a terminal state. Aggregates a COMPACT summary (counts + elapsed)
/// and "persists" it (here: a log line + the local registry). This models the recommendation — reducers write a small
/// outcome to DB/blob, never stuff large per-item results back into Redis.
/// </summary>
public sealed class BenchmarkReducer(ILogger<BenchmarkReducer> logger, BenchmarkRunRegistry registry)
    : IReduceJobHandler<BenchmarkState, BenchmarkMapResult>
{
    public Task Reduce(BenchmarkState state, IReadOnlyCollection<MapResult<BenchmarkMapResult>> results,
        IJobExecutionContext context, CancellationToken cancellationToken = default)
    {
        var succeeded = results.Count(r => r.Succeeded);
        var failed = results.Count - succeeded;
        var elapsed = TimeSpan.FromTicks(DateTime.UtcNow.Ticks - state.StartedTicksUtc);

        logger.LogInformation(
            "Benchmark map/reduce run {RunId}: {Succeeded} ok, {Failed} failed across {Total} items in {Elapsed}.",
            state.RunId, succeeded, failed, results.Count, elapsed);

        // Local convenience only (see BenchmarkRunRegistry) — canonical metrics come from Application Insights.
        for (var i = 0; i < succeeded; i++)
        {
            registry.Complete(state.RunId, success: true);
        }

        for (var i = 0; i < failed; i++)
        {
            registry.Complete(state.RunId, success: false);
        }

        return Task.CompletedTask;
    }
}
