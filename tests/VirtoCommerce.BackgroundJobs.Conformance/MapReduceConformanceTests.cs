using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using VirtoCommerce.BackgroundJobs.Core.MapReduce;
using Xunit;

namespace VirtoCommerce.BackgroundJobs.Conformance;

/// <summary>
/// Map/reduce conformance — part of <see cref="JobEngineConformanceTests{TFixture}"/>. Map/reduce rides on the
/// engine (fan-out, each map task, and the reduce are ordinary message jobs), so if the core scenarios pass these
/// prove the full fan-out → aggregate flow works on the engine end-to-end (single-instance, using the in-memory
/// batch store the shared container registers).
/// </summary>
public abstract partial class JobEngineConformanceTests<TFixture>
{
    private async Task<ConformanceProbe.ReduceObservation> RunBatchAsync(string correlationId, ConformanceMapItem[] items, MapReduceOptions? options)
    {
        using var scope = Fixture.Services.CreateScope();
        var mapReduce = scope.ServiceProvider.GetRequiredService<IMapReduceJob>();

        await mapReduce.Enqueue<ConformanceMapItem, ConformanceMapResult, ConformanceReduceState>(
            items,
            new ConformanceReduceState { CorrelationId = correlationId },
            options,
            TestContext.Current.CancellationToken);

        await Fixture.Probe.WaitAsync(correlationId, ConformanceConstants.DefaultTimeout, TestContext.Current.CancellationToken);

        var reduce = Fixture.Probe.Reduce(correlationId);
        Assert.NotNull(reduce);
        return reduce!;
    }

    // 13 — the whole batch fans out, every item maps, and reduce runs once with the aggregated results.
    [Fact]
    public async Task MapReduce_FanOut_Then_Reduce_Aggregates_All_Results()
    {
        RequireEngine();
        var id = NewId();

        var reduce = await RunBatchAsync(id, [new ConformanceMapItem(1), new ConformanceMapItem(2), new ConformanceMapItem(3)], options: null);

        Assert.Equal(3, reduce.ResultCount);
        Assert.Equal(0, reduce.FailureCount);
        Assert.Equal(1 + 4 + 9, reduce.Total);
    }

    // 14 — ContinueOnError records the failed item and still runs reduce with the full result set.
    [Fact]
    public async Task MapReduce_ContinueOnError_Runs_Reduce_With_Failures_Included()
    {
        RequireEngine();
        var id = NewId();

        var reduce = await RunBatchAsync(
            id,
            [new ConformanceMapItem(2), new ConformanceMapItem(-1), new ConformanceMapItem(4)], // -1 makes the map handler throw
            new MapReduceOptions { FailurePolicy = FailurePolicy.ContinueOnError });

        Assert.Equal(3, reduce.ResultCount);
        Assert.Equal(1, reduce.FailureCount);
        Assert.Equal(4 + 16, reduce.Total); // failed item excluded from the aggregate
    }
}
