using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using VirtoCommerce.Platform.Core.Jobs;
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
    // The title MapReduceJob gives the batch's shared notification; the scenario finds its notification by it.
    private static readonly string _mapReduceTitle = $"Map/reduce: {nameof(ConformanceMapItem)}";

    private async Task<ConformanceProbe.ReduceObservation> RunBatchAsync(string correlationId, ConformanceMapItem[] items, MapReduceOptions? options)
    {
        using var scope = Fixture.Services.CreateScope();
        var mapReduce = scope.ServiceProvider.GetRequiredService<IMapReduceJob>();

        await mapReduce.Enqueue<ConformanceMapHandler, ConformanceReduceHandler>(
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

    // 17 — a reduce step that fails on its final attempt closes the batch's shared progress notification Failed: the map
    // tasks never close the bar and the dispatcher does not close it for the reduce job, so nobody else would, and
    // the bar would run forever. Attempts before the last leave it open.
    [Fact]
    public async Task MapReduce_Reduce_Failure_On_Final_Attempt_Closes_Notification_Failed()
    {
        RequireEngine();
        Assert.SkipUnless(
            Fixture.Capabilities.SupportsRetry && Fixture.Capabilities.ReportsAttemptBudget,
            "engine does not retry failed jobs or does not report its attempt budget.");
        var id = NewId();

        using (var scope = Fixture.Services.CreateScope())
        {
            await scope.ServiceProvider.GetRequiredService<IMapReduceJob>().Enqueue<ConformanceMapHandler, ConformanceReduceHandler>(
                [new ConformanceMapItem(1), new ConformanceMapItem(2)],
                new ConformanceReduceState { CorrelationId = id, FailAlways = true },
                new MapReduceOptions { ReportProgress = true },
                TestContext.Current.CancellationToken);
        }

        // The batch's notification is the only one with this title: the other map/reduce scenarios do not report progress.
        var notificationId = Fixture.ProgressNotifications.First(x => x.Title == _mapReduceTitle).Id;
        var closing = await Fixture.WaitForProgressAsync(
            notificationId, x => x.Outcome is not null, ConformanceConstants.DefaultTimeout);

        Assert.Equal(nameof(JobOutcome.Failed), closing.Outcome);
        Assert.NotNull(closing.Finished);
        Assert.Contains("Conformance-induced reduce failure", closing.Description);

        // The close came with the last attempt, not an earlier one, and nothing closes the bar a second time. The
        // absence of a second close has no signal, so the check can only wait.
        Assert.Equal(Fixture.RetryBudget + 1, Fixture.Probe.ExecutionCount(id));
        await Task.Delay(_noCloseObservation, TestContext.Current.CancellationToken);
        Assert.Single(Fixture.ProgressUpdates(notificationId), x => x.Outcome is not null);
    }
}
