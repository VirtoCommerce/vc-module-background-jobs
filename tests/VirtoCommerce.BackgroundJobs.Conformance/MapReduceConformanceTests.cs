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

    // All reporting batches share one title, so the batch just enqueued is the notification that was not there before.
    private async Task<string> EnqueueReportingBatchAsync<TReduce>(ConformanceReduceState state)
        where TReduce : class
    {
        var known = Fixture.ProgressNotifications.Where(x => x.Title == _mapReduceTitle).Select(x => x.Id).ToHashSet();

        using (var scope = Fixture.Services.CreateScope())
        {
            await scope.ServiceProvider.GetRequiredService<IMapReduceJob>().Enqueue<ConformanceMapHandler, TReduce>(
                [new ConformanceMapItem(1), new ConformanceMapItem(2)],
                state,
                new MapReduceOptions { ReportProgress = true },
                TestContext.Current.CancellationToken);
        }

        return Fixture.ProgressNotifications.Where(x => x.Title == _mapReduceTitle && !known.Contains(x.Id)).Select(x => x.Id).Distinct().Single();
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

    // 17 — a reduce that fails on its final attempt closes the batch's shared notification Failed; earlier attempts
    // leave it open.
    [Fact]
    public async Task MapReduce_Reduce_Failure_On_Final_Attempt_Closes_Notification_Failed()
    {
        RequireEngine();
        Assert.SkipUnless(
            Fixture.Capabilities.SupportsRetry && Fixture.Capabilities.ReportsAttemptBudget,
            "engine does not retry failed jobs or does not report its attempt budget.");
        var id = NewId();

        var notificationId = await EnqueueReportingBatchAsync<ConformanceReduceHandler>(new ConformanceReduceState { CorrelationId = id, FailAlways = true });
        var closing = await Fixture.WaitForProgressAsync(
            notificationId, x => x.Outcome is not null, ConformanceConstants.DefaultTimeout);

        Assert.Equal(nameof(JobOutcome.Failed), closing.Outcome);
        Assert.NotNull(closing.Finished);
        Assert.Contains("Conformance-induced reduce failure", closing.Description);

        // The close came with the last attempt, and nothing closes the bar a second time.
        Assert.Equal(Fixture.RetryBudget + 1, Fixture.Probe.ExecutionCount(id));
        await Task.Delay(_noCloseObservation, TestContext.Current.CancellationToken);
        Assert.Single(Fixture.ProgressUpdates(notificationId), x => x.Outcome is not null);
    }

    // 17 (continued) — the same when the reduce fails before its handler: no reduce handler is registered.
    [Fact]
    public async Task MapReduce_Reduce_Setup_Failure_On_Final_Attempt_Closes_Notification_Failed()
    {
        RequireEngine();
        Assert.SkipUnless(
            Fixture.Capabilities.SupportsRetry && Fixture.Capabilities.ReportsAttemptBudget,
            "engine does not retry failed jobs or does not report its attempt budget.");
        var id = NewId();

        var notificationId = await EnqueueReportingBatchAsync<ConformanceUnregisteredReduceHandler>(new ConformanceReduceState { CorrelationId = id });
        var closing = await Fixture.WaitForProgressAsync(
            notificationId, x => x.Outcome is not null, ConformanceConstants.DefaultTimeout);

        Assert.Equal(nameof(JobOutcome.Failed), closing.Outcome);
        Assert.NotNull(closing.Finished);
        Assert.Contains("No reduce handler", closing.Description);

        await Task.Delay(_noCloseObservation, TestContext.Current.CancellationToken);
        Assert.Single(Fixture.ProgressUpdates(notificationId), x => x.Outcome is not null);
    }
}
