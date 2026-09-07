using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using VirtoCommerce.BackgroundJobs;
using VirtoCommerce.Platform.Core.Jobs;
using Xunit;

namespace VirtoCommerce.BackgroundJobs.Conformance;

/// <summary>
/// The engine-port conformance scenarios — one clear scenario per feature of the background-job standard. A new
/// engine certifies itself by deriving a concrete class bound to its <typeparamref name="TFixture"/>:
/// <code>public class MyEngineConformanceTests(MyFixture f) : JobEngineConformanceTests&lt;MyFixture&gt;(f);</code>
/// xUnit discovers the inherited <c>[Fact]</c>s in that derived class. Required behavior is asserted for every
/// engine; optional behavior is gated by <see cref="EngineCapabilities"/> (asserted when supported, asserted as the
/// documented fallback or skipped otherwise).
/// </summary>
public abstract partial class JobEngineConformanceTests<TFixture>(TFixture fixture) : IClassFixture<TFixture>
    where TFixture : JobEngineConformanceFixture
{
    protected TFixture Fixture => fixture;

    private void RequireEngine() => Assert.SkipUnless(fixture.Available, $"Engine not configured: {fixture.UnavailableReason}");

    private static string NewId() => Guid.NewGuid().ToString("N");

    private async Task<string> EnqueueAsync(ConformancePayload payload, EnqueueOptions? options = null)
    {
        using var scope = fixture.Services.CreateScope();
        var jobs = scope.ServiceProvider.GetRequiredService<IBackgroundJob>();
        return await jobs.Enqueue<RecordingConformanceHandler>(payload, options, TestContext.Current.CancellationToken);
    }

    private async Task<string> EnqueueAndWaitAsync(ConformancePayload payload, EnqueueOptions? options = null)
    {
        var jobId = await EnqueueAsync(payload, options);
        await fixture.Probe.WaitAsync(payload.CorrelationId, ConformanceConstants.DefaultTimeout, TestContext.Current.CancellationToken);
        return jobId;
    }

    // 1 — the core of the standard: an enqueued message is dispatched to its handler with the payload intact.
    [Fact]
    public async Task Enqueue_Dispatches_Handler_With_Payload()
    {
        RequireEngine();
        var id = NewId();

        await EnqueueAndWaitAsync(new ConformancePayload { CorrelationId = id, Value = "hello" });

        var job = fixture.Probe.Job(id);
        Assert.NotNull(job);
        Assert.Equal("hello", ((ConformancePayload)job!.Payload).Value);
    }

    // 2 — payload fidelity: an AbstractTypeFactory-derived payload round-trips as the concrete type, not the base.
    [Fact]
    public async Task Enqueue_RoundTrips_Concrete_Payload_Type()
    {
        RequireEngine();
        var id = NewId();

        await EnqueueAndWaitAsync(new ExtendedConformancePayload { CorrelationId = id, Value = "v", ExtraValue = "extra" });

        var job = fixture.Probe.Job(id);
        Assert.NotNull(job);
        Assert.Equal(typeof(ExtendedConformancePayload), job!.RuntimePayloadType);
        Assert.Equal("extra", ((ExtendedConformancePayload)job.Payload).ExtraValue);
    }

    // 3 — the enqueuing user is restored in the handler's scope (engine-agnostic user context).
    [Fact]
    public async Task Restores_Enqueuing_User_In_Handler()
    {
        RequireEngine();
        var id = NewId();
        fixture.UserResolver.SetCurrentUserName("alice");

        await EnqueueAndWaitAsync(new ConformancePayload { CorrelationId = id, Value = "u" });

        Assert.Equal("alice", fixture.Probe.Job(id)!.User);
    }

    // 4 — every enqueue returns a non-empty engine job id.
    [Fact]
    public async Task Enqueue_Returns_NonEmpty_JobId()
    {
        RequireEngine();
        var jobId = await EnqueueAndWaitAsync(new ConformancePayload { CorrelationId = NewId(), Value = "id" });

        Assert.False(string.IsNullOrEmpty(jobId));
    }

    // 5 — a handler that always throws must not stop the worker: a subsequent good job still runs.
    [Fact]
    public async Task Failing_Job_Does_Not_Stop_Worker()
    {
        RequireEngine();

        // Poison job — always throws, exhausts retries in the background.
        await EnqueueAsync(new ConformancePayload { CorrelationId = NewId(), Value = "poison", FailAttempts = int.MaxValue });

        // A good job enqueued afterwards must still be processed.
        var goodId = NewId();
        await EnqueueAndWaitAsync(new ConformancePayload { CorrelationId = goodId, Value = "good" });

        Assert.NotNull(fixture.Probe.Job(goodId));
    }

    // 6 — GetStatus contract: status-capable engines track the job; otherwise the engine must never falsely report completion.
    [Fact]
    public async Task GetStatus_Follows_Contract()
    {
        RequireEngine();
        var id = NewId();
        var jobId = await EnqueueAndWaitAsync(new ConformancePayload { CorrelationId = id, Value = "status" });

        var status = await fixture.Services.GetRequiredService<IJobEngine>().GetStatus(jobId, TestContext.Current.CancellationToken);

        if (fixture.Capabilities.SupportsStatusQuery)
        {
            Assert.NotNull(status);
        }
        else
        {
            Assert.True(status is null || !status.Completed,
                "an engine without a job ledger must report Unknown/not-completed, never a false completion.");
        }
    }

    // 7 — Delete of an unknown id: a status-tracking engine reports false (nothing to remove). A ledgerless
    // cooperative-cancel engine (RabbitMQ) cannot tell a known id from an unknown one — it just records a cancel
    // request — so it accepts unconditionally; the stray flag never matches a job and TTLs out.
    [Fact]
    public async Task Delete_Unknown_Job_Returns_False()
    {
        RequireEngine();
        var deleted = await fixture.Services.GetRequiredService<IJobEngine>()
            .Delete("conformance-missing-" + NewId(), TestContext.Current.CancellationToken);

        if (fixture.Capabilities.SupportsCancellation && !fixture.Capabilities.SupportsStatusQuery)
        {
            Assert.True(deleted, "a ledgerless cooperative-cancel engine accepts a cancel request for any id.");
        }
        else
        {
            Assert.False(deleted, "a status-tracking engine has nothing to remove for an unknown id.");
        }
    }

    // 8 — unique-key de-duplication: two enqueues with the same key collapse to one execution (when supported).
    [Fact]
    public async Task UniqueKey_Dedupes_When_Supported()
    {
        RequireEngine();
        Assert.SkipUnless(fixture.Capabilities.SupportsUniqueKeyDedup, "engine does not de-duplicate by UniqueKey.");

        var id = NewId();
        var key = "conformance-uk-" + id;
        await EnqueueAsync(new ConformancePayload { CorrelationId = id, Value = "dedup" }, new EnqueueOptions { UniqueKey = key });
        await EnqueueAsync(new ConformancePayload { CorrelationId = id, Value = "dedup" }, new EnqueueOptions { UniqueKey = key });

        await fixture.Probe.WaitAsync(id, ConformanceConstants.DefaultTimeout, TestContext.Current.CancellationToken);
        await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken); // allow a (wrongful) second execution to land

        Assert.Equal(1, fixture.Probe.ExecutionCount(id));
    }

    // 10 — queue routing: a job on a non-default queue is still processed (when the engine supports queues).
    [Fact]
    public async Task Custom_Queue_Is_Drained_When_Supported()
    {
        RequireEngine();
        Assert.SkipUnless(fixture.Capabilities.SupportsQueueRouting, "engine does not support queue routing.");

        var id = NewId();
        await EnqueueAndWaitAsync(
            new ConformancePayload { CorrelationId = id, Value = "queued" },
            new EnqueueOptions { Queue = ConformanceConstants.CustomQueue });

        Assert.NotNull(fixture.Probe.Job(id));
    }

    // 11 — retry: a handler failing (MaxRetryAttempts-1) times eventually succeeds (when the engine retries).
    [Fact]
    public async Task Failing_Job_Is_Retried_When_Supported()
    {
        RequireEngine();
        Assert.SkipUnless(fixture.Capabilities.SupportsRetry, "engine does not retry failed jobs.");

        var id = NewId();
        await EnqueueAndWaitAsync(new ConformancePayload { CorrelationId = id, Value = "retry", FailAttempts = 2 });

        Assert.Equal(3, fixture.Probe.ExecutionCount(id)); // 2 failures + 1 success
    }

    // 12 — progress: when requested, handler progress surfaces on the job's notification.
    [Fact]
    public async Task Reports_Progress_When_Requested()
    {
        RequireEngine();
        var id = NewId();

        await EnqueueAndWaitAsync(
            new ConformancePayload { CorrelationId = id, Value = "progress", ReportProgress = true },
            new EnqueueOptions { ReportProgress = true, Title = id });

        Assert.Contains(fixture.ProgressNotifications, n => n.Title == id && n.Description == ConformanceConstants.ProgressMessage);
    }

    // 13 — cancellation: a running, cancellable job stops when cancelled (natively or cooperatively), tripping the
    // handler's CancellationToken. Skipped for engines that do not support cancellation.
    [Fact]
    public async Task Running_Job_Is_Cancelled_When_Supported()
    {
        RequireEngine();
        Assert.SkipUnless(fixture.Capabilities.SupportsCancellation, "engine does not support cancellation.");

        var engine = fixture.Services.GetRequiredService<IJobEngine>();
        Assert.True(engine.SupportsCancellation, "engine declared SupportsCancellation in its capabilities but reports false.");

        var id = NewId();
        var jobId = await EnqueueAsync(new ConformancePayload { CorrelationId = id, Value = "cancel", BlockUntilCancelled = true });

        // Wait until the handler is actually running, then request cancellation.
        await fixture.Probe.WaitStartedAsync(id, ConformanceConstants.DefaultTimeout, TestContext.Current.CancellationToken);

        var accepted = await engine.Delete(jobId, TestContext.Current.CancellationToken);
        Assert.True(accepted, "the engine must accept the cancellation request for a known, running job.");

        // The handler's token must trip within a bounded window (cooperative engines poll on an interval).
        await fixture.Probe.WaitCancelledAsync(id, ConformanceConstants.CancellationTimeout, TestContext.Current.CancellationToken);
    }
}
