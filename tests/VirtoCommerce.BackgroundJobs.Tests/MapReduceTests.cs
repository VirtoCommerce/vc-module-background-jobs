#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using VirtoCommerce.BackgroundJobs.Core.MapReduce;
using VirtoCommerce.BackgroundJobs.Core.Services;
using VirtoCommerce.BackgroundJobs.Data.MapReduce;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.PushNotifications;
using VirtoCommerce.Platform.Core.Security;
using Xunit;

namespace VirtoCommerce.BackgroundJobs.Tests;

public class MapReduceTests
{
    public sealed record SquareItem(int Value);
    public sealed record SquareResult(int Square);
    public sealed record SumState(string Label);

    private sealed class SquareHandler : IMapJobHandler<SquareItem, SquareResult>
    {
        public Task<SquareResult> Map(SquareItem item, IJobExecutionContext ctx, CancellationToken ct = default)
            => Task.FromResult(new SquareResult(item.Value * item.Value));
    }

    // Throws for Value == 3 to exercise failure handling.
    private sealed class FlakySquareHandler : IMapJobHandler<SquareItem, SquareResult>
    {
        public Task<SquareResult> Map(SquareItem item, IJobExecutionContext ctx, CancellationToken ct = default)
            => item.Value == 3
                ? throw new InvalidOperationException("boom")
                : Task.FromResult(new SquareResult(item.Value * item.Value));
    }

    // Same (item, result) contract as SquareHandler but different math — used to prove the NAMED map handler runs,
    // not "the one handler registered for the (item, result) type".
    private sealed class DoubleHandler : IMapJobHandler<SquareItem, SquareResult>
    {
        public Task<SquareResult> Map(SquareItem item, IJobExecutionContext ctx, CancellationToken ct = default)
            => Task.FromResult(new SquareResult(item.Value * 2));
    }

    private sealed class SumReducer : IReduceJobHandler<SumState, SquareResult>
    {
        public bool Ran { get; private set; }
        public int Total { get; private set; }
        public IReadOnlyCollection<MapResult<SquareResult>>? Captured { get; private set; }

        public Task Reduce(SumState state, IReadOnlyCollection<MapResult<SquareResult>> results, IJobExecutionContext ctx, CancellationToken ct = default)
        {
            Ran = true;
            Captured = results;
            Total = results.Where(r => r.Succeeded).Sum(r => r.Value!.Square);
            return Task.CompletedTask;
        }
    }

    // Captures enqueued payloads instead of going to a real engine.
    private sealed class CapturingBackgroundJob : IBackgroundJob
    {
        public List<object> Enqueued { get; } = [];

        public Task<string> Enqueue<THandler>(object payload, EnqueueOptions? options = null, CancellationToken ct = default)
            where THandler : class
        {
            Enqueued.Add(payload);
            return Task.FromResult(Guid.NewGuid().ToString("N"));
        }
    }

    // Fails ONLY when the reduce task is enqueued (map fan-out etc. succeed) — used to prove the reduce claim is
    // released when the enqueue throws after the claim is won.
    private sealed class ThrowOnReduceBackgroundJob : IBackgroundJob
    {
        public Task<string> Enqueue<THandler>(object payload, EnqueueOptions? options = null, CancellationToken ct = default)
            where THandler : class
        {
            if (typeof(THandler) == typeof(ReduceCoordinator))
            {
                throw new InvalidOperationException("reduce enqueue failed");
            }
            return Task.FromResult(Guid.NewGuid().ToString("N"));
        }
    }

    private static JobExecutionContext Context() =>
        new("test", NoOpJobProgress.Instance, new Dictionary<string, string>());

    private static (MapReduceJob facade, FanOutCoordinator fanOut, MapCoordinator map, ReduceCoordinator reduce, CapturingBackgroundJob bus, InMemoryMapReduceBatchStore store, SumReducer reducer)
        BuildHarness(IMapJobHandler<SquareItem, SquareResult> mapHandler)
    {
        var reducer = new SumReducer();
        var services = new ServiceCollection();
        services.AddSingleton(mapHandler);
        services.AddSingleton(mapHandler.GetType(), mapHandler);   // handler-explicit: resolve the map handler by concrete type
        services.AddSingleton<IReduceJobHandler<SumState, SquareResult>>(reducer);
        services.AddSingleton(reducer);                            // handler-explicit: resolve the reducer by concrete type
        var sp = services.BuildServiceProvider();

        var serializer = new JsonJobPayloadSerializer();
        var store = new InMemoryMapReduceBatchStore();
        var bus = new CapturingBackgroundJob();

        var userResolver = new Mock<IUserNameResolver>();
        userResolver.Setup(x => x.GetCurrentUserName()).Returns("tester");

        var facade = new MapReduceJob(bus, store, serializer, userResolver.Object, Mock.Of<IPushNotificationManager>());
        var fanOut = new FanOutCoordinator(store, bus, NullLogger<FanOutCoordinator>.Instance);
        var map = new MapCoordinator(sp, serializer, store, bus, NullLogger<MapCoordinator>.Instance);
        var reduce = new ReduceCoordinator(sp, serializer, store, Mock.Of<IPushNotificationManager>(), NullLogger<ReduceCoordinator>.Instance);

        return (facade, fanOut, map, reduce, bus, store, reducer);
    }

    // Drives the engine inline: run the fan-out (produces the map tasks), then every map task.
    private static async Task PumpMaps(FanOutCoordinator fanOut, MapCoordinator map, CapturingBackgroundJob bus, CancellationToken ct)
    {
        foreach (var fan in bus.Enqueued.OfType<MapFanOutEnvelope>().ToList())
        {
            await fanOut.Execute(fan, Context(), ct);
        }

        foreach (var env in bus.Enqueued.OfType<MapTaskEnvelope>().ToList())
        {
            await map.Execute(env, Context(), ct);
        }
    }

    [Fact]
    public async Task FullSuccess_RunsReduceOnce_WithAggregatedResults()
    {
        var ct = TestContext.Current.CancellationToken;
        var (facade, fanOut, map, reduce, bus, _, reducer) = BuildHarness(new SquareHandler());

        await facade.Enqueue<SquareHandler, SumReducer>(
            [new SquareItem(1), new SquareItem(2), new SquareItem(3)], new SumState("squares"), cancellationToken: ct);

        // Enqueue is fast: it stores items and queues a single fan-out task (not N map tasks).
        Assert.Single(bus.Enqueued.OfType<MapFanOutEnvelope>());

        await PumpMaps(fanOut, map, bus, ct);

        // The fan-out produced one map task per item.
        Assert.Equal(3, bus.Enqueued.OfType<MapTaskEnvelope>().Count());

        // Exactly one reduce task triggered after the last map completed.
        var reduceEnvelopes = bus.Enqueued.OfType<ReduceTaskEnvelope>().ToList();
        Assert.Single(reduceEnvelopes);

        await reduce.Execute(reduceEnvelopes[0], Context(), ct);

        Assert.True(reducer.Ran);
        Assert.Equal(1 + 4 + 9, reducer.Total);
    }

    [Fact]
    public async Task FanOut_RunTwice_EnqueuesMapTasks_Once()
    {
        var ct = TestContext.Current.CancellationToken;
        var (facade, fanOut, _, _, bus, _, _) = BuildHarness(new SquareHandler());

        await facade.Enqueue<SquareHandler, SumReducer>(
            [new SquareItem(1), new SquareItem(2), new SquareItem(3)], new SumState("squares"), cancellationToken: ct);

        var fan = bus.Enqueued.OfType<MapFanOutEnvelope>().Single();

        // Simulate the fan-out job being redelivered/retried: it must NOT enqueue a second set of map tasks.
        await fanOut.Execute(fan, Context(), ct);
        await fanOut.Execute(fan, Context(), ct);

        Assert.Equal(3, bus.Enqueued.OfType<MapTaskEnvelope>().Count());
    }

    [Fact]
    public async Task FailFast_WithFailure_SkipsReduce()
    {
        var ct = TestContext.Current.CancellationToken;
        var (facade, fanOut, map, reduce, bus, _, reducer) = BuildHarness(new FlakySquareHandler());

        await facade.Enqueue<FlakySquareHandler, SumReducer>(
            [new SquareItem(1), new SquareItem(2), new SquareItem(3)], new SumState("squares"),
            new MapReduceOptions { FailurePolicy = FailurePolicy.FailFast }, ct);

        await PumpMaps(fanOut, map, bus, ct);
        await reduce.Execute(bus.Enqueued.OfType<ReduceTaskEnvelope>().Single(), Context(), ct);

        Assert.False(reducer.Ran);
    }

    [Fact]
    public async Task ContinueOnError_RunsReduce_WithFailuresIncluded()
    {
        var ct = TestContext.Current.CancellationToken;
        var (facade, fanOut, map, reduce, bus, _, reducer) = BuildHarness(new FlakySquareHandler());

        await facade.Enqueue<FlakySquareHandler, SumReducer>(
            [new SquareItem(1), new SquareItem(2), new SquareItem(3)], new SumState("squares"),
            new MapReduceOptions { FailurePolicy = FailurePolicy.ContinueOnError }, ct);

        await PumpMaps(fanOut, map, bus, ct);
        await reduce.Execute(bus.Enqueued.OfType<ReduceTaskEnvelope>().Single(), Context(), ct);

        Assert.True(reducer.Ran);
        Assert.Equal(3, reducer.Captured!.Count);
        Assert.Equal(1 + 4, reducer.Total); // failed item (3) excluded

        // The handler throws synchronously; the real message must be recorded, not the reflection wrapper.
        var failed = Assert.Single(reducer.Captured!, r => !r.Succeeded);
        Assert.Equal("boom", failed.Error);
    }

    [Fact]
    public async Task EmptyBatch_RunsReduceImmediately()
    {
        var ct = TestContext.Current.CancellationToken;
        var (facade, _, _, reduce, bus, _, reducer) = BuildHarness(new SquareHandler());

        await facade.Enqueue<SquareHandler, SumReducer>(
            [], new SumState("empty"), cancellationToken: ct);

        Assert.Empty(bus.Enqueued.OfType<MapTaskEnvelope>());
        await reduce.Execute(bus.Enqueued.OfType<ReduceTaskEnvelope>().Single(), Context(), ct);

        Assert.True(reducer.Ran);
        Assert.Equal(0, reducer.Total);
    }

    [Fact]
    public void AddMapReduceJob_Infers_Types_From_Handlers()
    {
        var services = new ServiceCollection();

        services.AddMapReduceJob<SquareHandler, SumReducer>();

        using var provider = services.BuildServiceProvider();
        Assert.IsType<SquareHandler>(provider.GetService<IMapJobHandler<SquareItem, SquareResult>>());
        Assert.IsType<SumReducer>(provider.GetService<IReduceJobHandler<SumState, SquareResult>>());
    }

    [Fact]
    public async Task Store_SaveResult_IsIdempotentByIndex()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = new InMemoryMapReduceBatchStore();
        await store.CreateAsync(new MapReduceBatch { BatchId = "b", Total = 1 }, ct);

        var first = await store.SaveResultAndCountAsync("b", new MapResultRecord { Index = 0, Succeeded = true }, ct);
        var second = await store.SaveResultAndCountAsync("b", new MapResultRecord { Index = 0, Succeeded = true }, ct);

        Assert.Equal(1, first);
        Assert.Equal(1, second); // same index does not double-count
    }

    [Fact]
    public async Task Store_TryBeginReduce_WinsExactlyOnce()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = new InMemoryMapReduceBatchStore();
        await store.CreateAsync(new MapReduceBatch { BatchId = "b", Total = 1 }, ct);

        var results = await Task.WhenAll(Enumerable.Range(0, 16).Select(_ => store.TryBeginReduceAsync("b", ct)));

        Assert.Equal(1, results.Count(won => won));
    }

    // If the map task wins the reduce claim but the reduce enqueue throws, the claim must be RELEASED so a redelivered
    // map task can re-trigger reduce — otherwise the batch is stranded (claim set, no reduce task) until TTL.
    [Fact]
    public async Task ReduceEnqueueFailure_ReleasesReduceClaim()
    {
        var ct = TestContext.Current.CancellationToken;

        var reducer = new SumReducer();
        var services = new ServiceCollection();
        services.AddSingleton(new SquareHandler());
        services.AddSingleton<IReduceJobHandler<SumState, SquareResult>>(reducer);
        services.AddSingleton(reducer);
        var sp = services.BuildServiceProvider();

        var serializer = new JsonJobPayloadSerializer();
        var store = new InMemoryMapReduceBatchStore();
        var map = new MapCoordinator(sp, serializer, store, new ThrowOnReduceBackgroundJob(), NullLogger<MapCoordinator>.Instance);

        await store.CreateAsync(new MapReduceBatch
        {
            BatchId = "b",
            Total = 1,
            ItemType = typeof(SquareItem).AssemblyQualifiedName!,
            ResultType = typeof(SquareResult).AssemblyQualifiedName!,
            StateType = typeof(SumState).AssemblyQualifiedName!,
            MapHandlerType = typeof(SquareHandler).AssemblyQualifiedName!,
            ReduceHandlerType = typeof(SumReducer).AssemblyQualifiedName!,
        }, ct);

        var (itemType, itemJson) = serializer.Serialize(new SquareItem(2));
        var env = new MapTaskEnvelope { BatchId = "b", Index = 0, ItemType = itemType, ItemJson = itemJson };

        // Map succeeds and completes the batch, but enqueuing reduce throws → Execute rethrows.
        await Assert.ThrowsAsync<InvalidOperationException>(() => map.Execute(env, Context(), ct));

        // The claim was released, so it can be won again (a redelivered map task would re-enqueue reduce).
        Assert.True(await store.TryBeginReduceAsync("b", ct));
    }

    // The same (item, result, state) set drives TWO different map handlers — naming the handler at enqueue picks which
    // one runs, mirroring the "one payload, several handlers" behaviour of IBackgroundJob.
    [Fact]
    public async Task SameTypes_RunOnTheNamedMapHandler()
    {
        var ct = TestContext.Current.CancellationToken;

        var reducer = new SumReducer();
        var services = new ServiceCollection();
        services.AddSingleton(new SquareHandler()); // registered by concrete type
        services.AddSingleton(new DoubleHandler());
        services.AddSingleton<IReduceJobHandler<SumState, SquareResult>>(reducer);
        services.AddSingleton(reducer);
        var sp = services.BuildServiceProvider();

        var serializer = new JsonJobPayloadSerializer();
        var store = new InMemoryMapReduceBatchStore();
        var bus = new CapturingBackgroundJob();
        var userResolver = new Mock<IUserNameResolver>();
        userResolver.Setup(x => x.GetCurrentUserName()).Returns("tester");

        var facade = new MapReduceJob(bus, store, serializer, userResolver.Object, Mock.Of<IPushNotificationManager>());
        var fanOut = new FanOutCoordinator(store, bus, NullLogger<FanOutCoordinator>.Instance);
        var map = new MapCoordinator(sp, serializer, store, bus, NullLogger<MapCoordinator>.Instance);
        var reduce = new ReduceCoordinator(sp, serializer, store, Mock.Of<IPushNotificationManager>(), NullLogger<ReduceCoordinator>.Instance);

        // Batch A — SquareHandler: 1,2,3 -> 1,4,9 = 14
        await RunSquareBatch<SquareHandler>(facade, fanOut, map, reduce, bus, ct);
        Assert.Equal(1 + 4 + 9, reducer.Total);

        // Batch B — same item/result/state, DoubleHandler: 1,2,3 -> 2,4,6 = 12
        bus.Enqueued.Clear();
        await RunSquareBatch<DoubleHandler>(facade, fanOut, map, reduce, bus, ct);
        Assert.Equal(2 + 4 + 6, reducer.Total);
    }

    private static async Task RunSquareBatch<TMap>(
        MapReduceJob facade, FanOutCoordinator fanOut, MapCoordinator map, ReduceCoordinator reduce,
        CapturingBackgroundJob bus, CancellationToken ct)
        where TMap : class
    {
        await facade.Enqueue<TMap, SumReducer>(
            [new SquareItem(1), new SquareItem(2), new SquareItem(3)], new SumState("squares"), cancellationToken: ct);
        await PumpMaps(fanOut, map, bus, ct);
        await reduce.Execute(bus.Enqueued.OfType<ReduceTaskEnvelope>().Single(), Context(), ct);
    }
}
