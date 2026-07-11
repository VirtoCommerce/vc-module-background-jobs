using System;
using System.Threading;
using System.Threading.Tasks;
using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs.SampleModule.Jobs.Benchmark;

/// <summary>
/// MAP: runs the tunable workload for one item, in parallel with every other item across workers. Throwing on a
/// simulated failure exercises the map/reduce failure path (with <c>ContinueOnError</c> the batch records a failed
/// <c>MapResult</c> and still reaches reduce).
/// </summary>
public sealed class BenchmarkMapHandler : IMapJobHandler<BenchmarkMapItem, BenchmarkMapResult>
{
    public async Task<BenchmarkMapResult> Map(BenchmarkMapItem item, IJobExecutionContext context, CancellationToken cancellationToken = default)
    {
        await BenchmarkWork.RunAsync(item.Kind, item.DelayMs, item.CpuIterations, cancellationToken);

        if (BenchmarkWork.ShouldFail(item.Seed, item.Index, item.FailRatePercent))
        {
            throw new InvalidOperationException($"Simulated map failure (index {item.Index}).");
        }

        return new BenchmarkMapResult(item.Index, BenchmarkWork.MakeFiller(item.ResultBytes));
    }
}
