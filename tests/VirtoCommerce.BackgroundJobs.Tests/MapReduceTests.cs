#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using VirtoCommerce.BackgroundJobs;
using VirtoCommerce.BackgroundJobs.Core;
using VirtoCommerce.BackgroundJobs.Core.MapReduce;
using VirtoCommerce.BackgroundJobs.Core.Models;
using VirtoCommerce.BackgroundJobs.Core.Notifications;
using VirtoCommerce.BackgroundJobs.Data.MapReduce;
using VirtoCommerce.BackgroundJobs.Data.Services;
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
            => Enqueue(typeof(THandler), payload, options, ct);

        public Task<string> Enqueue(Type handlerType, object payload, EnqueueOptions? options = null, CancellationToken ct = default)
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
            => Enqueue(typeof(THandler), payload, options, ct);

        public Task<string> Enqueue(Type handlerType, object payload, EnqueueOptions? options = null, CancellationToken ct = default)
        {
            if (handlerType == typeof(ReduceCoordinator))
            {
                throw new InvalidOperationException("reduce enqueue failed");
            }
            return Task.FromResult(Guid.NewGuid().ToString("N"));
        }
    }

    // Throws on the Nth Enqueue call (simulating a hard worker crash mid-fan-out), then behaves normally.
    private sealed class CrashOnceBackgroundJob(int throwOnCall) : IBackgroundJob
    {
        private int _calls;
        public List<object> Enqueued { get; } = [];

        public Task<string> Enqueue<THandler>(object payload, EnqueueOptions? options = null, CancellationToken ct = default)
            where THandler : class
            => Enqueue(typeof(THandler), payload, options, ct);

        public Task<string> Enqueue(Type handlerType, object payload, EnqueueOptions? options = null, CancellationToken ct = default)
        {
            if (++_calls == throwOnCall)
            {
                throw new InvalidOperationException("simulated crash mid-fan-out");
            }
            Enqueued.Add(payload);
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

    // Corrupt batch (metadata says Total > 0 but no items were stored): fan-out must finalize via reduce instead of
    // enqueuing a short set that never reaches Total and leaves the batch hanging until TTL.
    [Fact]
    public async Task FanOut_ItemCountMismatch_FinalizesInsteadOfHanging()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = new InMemoryMapReduceBatchStore();
        var bus = new CapturingBackgroundJob();
        var fanOut = new FanOutCoordinator(store, bus, NullLogger<FanOutCoordinator>.Instance);

        await store.CreateAsync(new MapReduceBatch { BatchId = "b", Total = 2 }, ct); // expects 2 items; none stored

        await fanOut.Execute(new MapFanOutEnvelope { BatchId = "b" }, Context(), ct);

        Assert.Empty(bus.Enqueued.OfType<MapTaskEnvelope>());       // nothing to map
        Assert.Single(bus.Enqueued.OfType<ReduceTaskEnvelope>());   // finalized rather than stuck
    }

    // A retried fan-out resumes from the stored checkpoint instead of re-dispatching indices already enqueued (which
    // would re-run those map handlers).
    [Fact]
    public async Task FanOut_ResumesFromCheckpoint_SkippingDispatchedIndices()
    {
        var ct = TestContext.Current.CancellationToken;
        var serializer = new JsonJobPayloadSerializer();
        var store = new InMemoryMapReduceBatchStore();
        var bus = new CapturingBackgroundJob();
        var fanOut = new FanOutCoordinator(store, bus, NullLogger<FanOutCoordinator>.Instance);

        await store.CreateAsync(new MapReduceBatch { BatchId = "b", Total = 4 }, ct);
        var items = Enumerable.Range(0, 4)
            .Select(i =>
            {
                var (type, json) = serializer.Serialize(new SquareItem(i));
                return new MapItem { ItemType = type, ItemJson = json };
            })
            .ToList();
        await store.SaveItemsAsync("b", items, ct);

        // Simulate a prior partial fan-out that already dispatched indices 0..1.
        await store.SetFanOutProgressAsync("b", 2, ct);

        await fanOut.Execute(new MapFanOutEnvelope { BatchId = "b" }, Context(), ct);

        var dispatched = bus.Enqueued.OfType<MapTaskEnvelope>().Select(x => x.Index).ToList();
        Assert.Equal([2, 3], dispatched); // resumed from the checkpoint; 0 and 1 not re-dispatched
    }

    // A hard failure mid fan-out must NOT strand the batch: the redelivered fan-out re-dispatches the remaining
    // indices. (The old one-time claim would have been orphaned by the crash and made the retry skip dispatch,
    // leaving the batch below Total forever.)
    [Fact]
    public async Task FanOut_CrashMidDispatch_RetryDispatchesAllIndices()
    {
        var ct = TestContext.Current.CancellationToken;
        var serializer = new JsonJobPayloadSerializer();
        var store = new InMemoryMapReduceBatchStore();
        var bus = new CrashOnceBackgroundJob(throwOnCall: 2); // enqueue index 0, then "crash" on index 1
        var fanOut = new FanOutCoordinator(store, bus, NullLogger<FanOutCoordinator>.Instance);

        await store.CreateAsync(new MapReduceBatch { BatchId = "b", Total = 3 }, ct);
        var items = Enumerable.Range(0, 3)
            .Select(i =>
            {
                var (type, json) = serializer.Serialize(new SquareItem(i));
                return new MapItem { ItemType = type, ItemJson = json };
            })
            .ToList();
        await store.SaveItemsAsync("b", items, ct);

        // First delivery crashes mid-dispatch.
        await Assert.ThrowsAsync<InvalidOperationException>(() => fanOut.Execute(new MapFanOutEnvelope { BatchId = "b" }, Context(), ct));

        // Redelivery must still dispatch every index (not be blocked) so the batch can reach Total.
        await fanOut.Execute(new MapFanOutEnvelope { BatchId = "b" }, Context(), ct);

        var indices = bus.Enqueued.OfType<MapTaskEnvelope>().Select(x => x.Index).Distinct().Order().ToList();
        Assert.Equal([0, 1, 2], indices);
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

    public sealed record BatchPayload(int Value);

    private sealed class FailingMapTaskHandler : IBackgroundJobHandler<BatchPayload>
    {
        public Task Execute(BatchPayload payload, IJobExecutionContext context, CancellationToken cancellationToken = default)
            => payload.Value == 0 ? throw new InvalidOperationException("boom") : Task.CompletedTask;
    }

    // A map task reports into the batch's shared notification; the dispatcher hands completion to every reporter, so the
    // reporter must be the one that refuses to close a bar it does not own.
    [Fact]
    public async Task MapTaskProgress_DoesNotCloseSharedNotification()
    {
        var ct = TestContext.Current.CancellationToken;
        var sent = new List<JobProgressPushNotification>();
        var manager = new Mock<IPushNotificationManager>();
        manager.Setup(x => x.SendAsync(It.IsAny<PushNotification>()))
            .Callback<PushNotification>(x => sent.Add((JobProgressPushNotification)x))
            .Returns(Task.CompletedTask);

        var services = new ServiceCollection();
        services.AddTransient<IBackgroundJobHandler<BatchPayload>, FailingMapTaskHandler>();
        using var provider = services.BuildServiceProvider();

        var serializer = new JsonJobPayloadSerializer();
        var dispatcher = new DefaultJobDispatcher(provider, serializer);

        async Task<Exception?> DispatchMapTask(int value)
        {
            var (payloadType, payloadJson) = serializer.Serialize(new BatchPayload(value));
            var envelope = new JobEnvelope
            {
                JobType = typeof(BatchPayload).AssemblyQualifiedName!,
                PayloadType = payloadType,
                PayloadJson = payloadJson,
                ProgressNotificationId = "batch-1",
                CompletesProgressNotification = false,
                Attempt = 1,
                TotalAttempts = 1,
            };
            var progress = new PushNotificationJobProgress(manager.Object, envelope, "job-1");
            var context = new JobExecutionContext("job-1", progress, new Dictionary<string, string>());

            try
            {
                await dispatcher.Dispatch(envelope, context, ct);
                return null;
            }
            catch (Exception ex)
            {
                return ex;
            }
        }

        Assert.Null(await DispatchMapTask(1));
        Assert.IsType<InvalidOperationException>(await DispatchMapTask(0));

        Assert.All(sent, x =>
        {
            Assert.Null(x.Finished);
            Assert.Null(x.Outcome);
        });
    }

    // The shared notification must keep the batch start time through every replacement: the initial one, each map
    // task's update (the start time travels as a header the fan-out's enqueue stamps) and the reduce close. The harness
    // runs the real facade over a capturing engine so the header path is exercised end to end.
    private sealed class NotifyingHarness
    {
        public List<JobProgressPushNotification> Sent { get; } = [];
        public List<JobEnvelope> Enqueued { get; } = [];
        public InMemoryMapReduceBatchStore Store { get; } = new();
        public JsonJobPayloadSerializer Serializer { get; } = new();
        public Mock<IPushNotificationManager> Manager { get; } = new();
        public MapReduceJob Facade { get; }
        public FanOutCoordinator FanOut { get; }
        public MapCoordinator Map { get; }
        public ReduceCoordinator Reduce { get; }

        public NotifyingHarness(IMapJobHandler<SquareItem, SquareResult> mapHandler)
        {
            Manager.Setup(x => x.SendAsync(It.IsAny<PushNotification>()))
                .Callback<PushNotification>(x => Sent.Add((JobProgressPushNotification)x))
                .Returns(Task.CompletedTask);

            var engine = new Mock<IJobEngine>();
            engine.SetupGet(x => x.ProviderName).Returns("Test");
            engine.Setup(x => x.Enqueue(It.IsAny<JobEnvelope>(), It.IsAny<EnqueueOptions>(), It.IsAny<CancellationToken>()))
                .Callback<JobEnvelope, EnqueueOptions, CancellationToken>((env, _, _) => Enqueued.Add(env))
                .ReturnsAsync("job-1");

            var userResolver = new Mock<IUserNameResolver>();
            userResolver.Setup(x => x.GetCurrentUserName()).Returns("tester");

            var bus = new JobEngineBackgroundJob(
                Serializer,
                Microsoft.Extensions.Options.Options.Create(new BackgroundJobsOptions { DefaultQueue = "default" }),
                Manager.Object,
                userResolver.Object,
                engine.Object);

            var reducer = new SumReducer();
            var services = new ServiceCollection();
            services.AddSingleton(mapHandler.GetType(), mapHandler);
            services.AddSingleton(reducer);
            var sp = services.BuildServiceProvider();

            Facade = new MapReduceJob(bus, Store, Serializer, userResolver.Object, Manager.Object);
            FanOut = new FanOutCoordinator(Store, bus, NullLogger<FanOutCoordinator>.Instance);
            Map = new MapCoordinator(sp, Serializer, Store, bus, NullLogger<MapCoordinator>.Instance);
            Reduce = new ReduceCoordinator(sp, Serializer, Store, Manager.Object, NullLogger<ReduceCoordinator>.Instance);
        }

        // Runs the batch up to the reduce close and returns the notifications sent by the initial send, the map tasks
        // and the reduce, in that order.
        public async Task<(JobProgressPushNotification Initial, IReadOnlyList<JobProgressPushNotification> MapUpdates, JobProgressPushNotification Close)>
            RunAsync<TMap>(SquareItem[] items, CancellationToken ct)
            where TMap : class
        {
            var batchId = await Facade.Enqueue<TMap, SumReducer>(
                items, new SumState("squares"), new MapReduceOptions { ReportProgress = true }, ct);
            var initial = Sent.Single();

            await FanOut.Execute(new MapFanOutEnvelope { BatchId = batchId }, Context(), ct);

            var mapTasks = Enqueued.Where(x => x.JobType == typeof(MapTaskEnvelope).AssemblyQualifiedName).ToList();
            Assert.Equal(items.Length, mapTasks.Count);

            foreach (var mapTask in mapTasks)
            {
                var payload = (MapTaskEnvelope)Serializer.Deserialize(mapTask.PayloadType, mapTask.PayloadJson);
                var progress = new PushNotificationJobProgress(Manager.Object, mapTask, "map-job");
                await Map.Execute(payload, new JobExecutionContext("map-job", progress, new Dictionary<string, string>()), ct);
            }

            var mapUpdates = Sent.Skip(1).ToList();
            await Reduce.Execute(new ReduceTaskEnvelope { BatchId = batchId }, Context(), ct);

            return (initial, mapUpdates, Sent[^1]);
        }
    }

    [Fact]
    public async Task SharedNotification_MapUpdatesAndReduceClose_KeepBatchStartAndCloseSucceeded()
    {
        var ct = TestContext.Current.CancellationToken;
        var harness = new NotifyingHarness(new SquareHandler());

        var (initial, mapUpdates, close) = await harness.RunAsync<SquareHandler>(
            [new SquareItem(1), new SquareItem(2)], ct);

        Assert.NotNull(initial.Started);
        Assert.NotEmpty(mapUpdates);
        Assert.All(mapUpdates, x =>
        {
            Assert.Equal(initial.Id, x.Id);
            Assert.Equal(initial.Started, x.Started);
            Assert.Null(x.Finished);
            Assert.Null(x.Outcome);
        });

        Assert.Equal(initial.Id, close.Id);
        Assert.Equal(initial.Started, close.Started);
        Assert.Null(close.JobId);
        Assert.NotNull(close.Finished);
        Assert.Equal(nameof(JobOutcome.Succeeded), close.Outcome);
        Assert.False(close.IsNew);
        Assert.Equal("Completed (2 items, 0 failed).", close.Description);
    }

    // A FailFast batch with a failed map item never runs the reduce handler; its close must say Failed, not Succeeded.
    [Fact]
    public async Task SharedNotification_FailFastFault_ClosesFailedWithBatchStart()
    {
        var ct = TestContext.Current.CancellationToken;
        var harness = new NotifyingHarness(new FlakySquareHandler());

        var (initial, _, close) = await harness.RunAsync<FlakySquareHandler>(
            [new SquareItem(1), new SquareItem(3)], ct);

        Assert.Equal(initial.Started, close.Started);
        Assert.NotNull(close.Finished);
        Assert.Equal(nameof(JobOutcome.Failed), close.Outcome);
        Assert.Equal("Faulted: 1/2 map items failed.", close.Description);
    }

    // A batch stored before the start time was recorded has no Started: nothing is stamped and the close keeps working.
    [Fact]
    public async Task SharedNotification_BatchWithoutStarted_ClosesWithoutStart()
    {
        var ct = TestContext.Current.CancellationToken;
        var harness = new NotifyingHarness(new SquareHandler());
        await harness.Store.CreateAsync(new MapReduceBatch
        {
            BatchId = "legacy",
            Total = 1,
            ItemType = typeof(SquareItem).AssemblyQualifiedName!,
            ResultType = typeof(SquareResult).AssemblyQualifiedName!,
            StateType = typeof(SumState).AssemblyQualifiedName!,
            StateJson = "{\"Label\":\"legacy\"}",
            MapHandlerType = typeof(SquareHandler).AssemblyQualifiedName!,
            ReduceHandlerType = typeof(SumReducer).AssemblyQualifiedName!,
            ProgressNotificationId = "legacy-notification",
            Title = "legacy",
        }, ct);
        var (_, itemJson) = harness.Serializer.Serialize(new SquareItem(2));
        await harness.Store.SaveItemsAsync("legacy", [new MapItem { ItemType = typeof(SquareItem).AssemblyQualifiedName!, ItemJson = itemJson }], ct);

        await harness.FanOut.Execute(new MapFanOutEnvelope { BatchId = "legacy" }, Context(), ct);

        var mapTask = harness.Enqueued.Single(x => x.JobType == typeof(MapTaskEnvelope).AssemblyQualifiedName);
        Assert.False(mapTask.Headers.ContainsKey(JobHeaders.BatchStartedAt));

        await harness.Reduce.Execute(new ReduceTaskEnvelope { BatchId = "legacy" }, Context(), ct);

        var close = harness.Sent.Single();
        Assert.Equal("legacy-notification", close.Id);
        Assert.Null(close.Started);
        Assert.Equal(nameof(JobOutcome.Succeeded), close.Outcome);
        Assert.NotNull(close.Finished);
    }
}
