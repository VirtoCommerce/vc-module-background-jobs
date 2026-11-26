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
    private const string _mapReduceTitle = $"Map/reduce: {nameof(ConformanceMapItem)}";

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
    private async Task<string> EnqueueReportingBatchAsync<TReduce>(ConformanceReduceState state, int itemCount = 2, string? gateId = null)
        where TReduce : class
    {
        var known = Fixture.ProgressNotifications.Where(x => x.Title == _mapReduceTitle).Select(x => x.Id).ToHashSet();

        using (var scope = Fixture.Services.CreateScope())
        {
            await scope.ServiceProvider.GetRequiredService<IMapReduceJob>().Enqueue<ConformanceMapHandler, TReduce>(
                Enumerable.Range(1, itemCount).Select(x => new ConformanceMapItem(x, gateId)).ToArray(),
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

    // 18 — a fan-out that fails on its final attempt closes the batch's shared notification Failed.
    [Fact]
    public async Task MapReduce_FanOut_Failure_On_Final_Attempt_Closes_Notification_Failed()
    {
        RequireEngine();
        Assert.SkipUnless(
            Fixture.Capabilities.SupportsRetry && Fixture.Capabilities.ReportsAttemptBudget,
            "engine does not retry failed jobs or does not report its attempt budget.");
        var id = NewId();

        var notificationId = await EnqueueReportingBatchAsync<ConformanceFanOutFailureReduceHandler>(new ConformanceReduceState { CorrelationId = id });
        var closing = await Fixture.WaitForProgressAsync(
            notificationId, x => x.Outcome is not null, ConformanceConstants.DefaultTimeout);

        Assert.Equal(nameof(JobOutcome.Failed), closing.Outcome);
        Assert.NotNull(closing.Finished);
        Assert.Contains("Conformance-induced fan-out failure", closing.Description);

        await Task.Delay(_noCloseObservation, TestContext.Current.CancellationToken);
        Assert.Single(Fixture.ProgressUpdates(notificationId), x => x.Outcome is not null);
    }

    // 19 — the same for the map task that completes the batch and fails before the reduce is enqueued.
    [Fact]
    public async Task MapReduce_Map_Failure_On_Final_Attempt_Closes_Notification_Failed()
    {
        RequireEngine();
        Assert.SkipUnless(
            Fixture.Capabilities.SupportsRetry && Fixture.Capabilities.ReportsAttemptBudget,
            "engine does not retry failed jobs or does not report its attempt budget.");
        var id = NewId();

        var notificationId = await EnqueueReportingBatchAsync<ConformanceMapFailureReduceHandler>(new ConformanceReduceState { CorrelationId = id });
        var closing = await Fixture.WaitForProgressAsync(
            notificationId, x => x.Outcome is not null, ConformanceConstants.DefaultTimeout);

        Assert.Equal(nameof(JobOutcome.Failed), closing.Outcome);
        Assert.NotNull(closing.Finished);
        Assert.Contains("Conformance-induced map failure", closing.Description);

        // Both map tasks may observe the batch complete and fail on the claim: the bar can close twice, always Failed.
        await Task.Delay(_noCloseObservation, TestContext.Current.CancellationToken);
        var updates = Fixture.ProgressUpdates(notificationId);
        Assert.All(updates.Where(x => x.Outcome is not null), x => Assert.Equal(nameof(JobOutcome.Failed), x.Outcome));
        Assert.Equal(nameof(JobOutcome.Failed), updates.Last().Outcome);
    }

    // 20 — a map task that runs after the close of a fan-out that failed after it enqueued map tasks does not reopen the
    // notification. One map task waits in its handler until the close was seen, so one is sure to run after it.
    [Fact]
    public async Task MapReduce_FanOut_Failure_After_Enqueuing_Map_Tasks_Keeps_Notification_Failed()
    {
        RequireEngine();
        Assert.SkipUnless(
            Fixture.Capabilities.SupportsRetry && Fixture.Capabilities.ReportsAttemptBudget,
            "engine does not retry failed jobs or does not report its attempt budget.");
        var id = NewId();

        try
        {
            var notificationId = await EnqueueReportingBatchAsync<ConformanceFanOutCheckpointFailureReduceHandler>(
                new ConformanceReduceState { CorrelationId = id }, itemCount: 30, gateId: id);
            var closing = await Fixture.WaitForProgressAsync(
                notificationId, x => x.Outcome is not null, ConformanceConstants.DefaultTimeout);

            Assert.Equal(nameof(JobOutcome.Failed), closing.Outcome);
            Assert.Contains("Conformance-induced fan-out checkpoint failure", closing.Description);

            Fixture.Probe.ReleaseHeldMapTask(id);
            await Fixture.Probe.WaitHeldMapTaskResumedAsync(id, ConformanceConstants.DefaultTimeout, TestContext.Current.CancellationToken);

            await Task.Delay(_noCloseObservation, TestContext.Current.CancellationToken);
            Assert.Equal(nameof(JobOutcome.Failed), Fixture.ProgressUpdates(notificationId).Last().Outcome);
        }
        finally
        {
            // A failed assertion must not leave the worker blocked for the next scenario.
            Fixture.Probe.ReleaseHeldMapTask(id);
        }
    }

    // 21 — a reduce whose read of the map results fails on every attempt closes the notification Failed on the last.
    [Fact]
    public async Task MapReduce_Reduce_Results_Read_Failure_On_Final_Attempt_Closes_Notification_Failed()
    {
        RequireEngine();
        Assert.SkipUnless(
            Fixture.Capabilities.SupportsRetry && Fixture.Capabilities.ReportsAttemptBudget,
            "engine does not retry failed jobs or does not report its attempt budget.");
        var id = NewId();

        var notificationId = await EnqueueReportingBatchAsync<ConformanceResultsReadFailureReduceHandler>(new ConformanceReduceState { CorrelationId = id });
        var closing = await Fixture.WaitForProgressAsync(
            notificationId, x => x.Outcome is not null, ConformanceConstants.DefaultTimeout);

        Assert.Equal(nameof(JobOutcome.Failed), closing.Outcome);
        Assert.NotNull(closing.Finished);
        Assert.Contains("Conformance-induced results read failure", closing.Description);

        await Task.Delay(_noCloseObservation, TestContext.Current.CancellationToken);
        Assert.Equal(nameof(JobOutcome.Failed), Fixture.ProgressUpdates(notificationId).Last().Outcome);
    }
}
