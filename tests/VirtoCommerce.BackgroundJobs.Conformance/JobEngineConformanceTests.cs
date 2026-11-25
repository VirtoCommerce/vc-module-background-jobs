using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using VirtoCommerce.BackgroundJobs;
using VirtoCommerce.BackgroundJobs.Core.Notifications;
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

    // The owned notification the facade creates at enqueue (ReportProgress, no ProgressNotificationId) is the one the
    // worker closes. Its id is generated there, so a scenario finds it by the unique title it enqueued with; the facade
    // sends it before Enqueue returns, so it is already captured.
    private string NotificationIdOf(string title)
        => fixture.ProgressNotifications.First(x => x.Title == title).Id;

    private async Task<(string JobId, string NotificationId)> EnqueueWithNotificationAsync(ConformancePayload payload)
    {
        var jobId = await EnqueueAsync(payload, new EnqueueOptions { ReportProgress = true, Title = payload.CorrelationId });

        return (jobId, NotificationIdOf(payload.CorrelationId));
    }

    // 14 — a failed attempt with a retry pending keeps the notification open and says which attempt of how many failed.
    // An engine that does not report its attempt budget treats every attempt as final: the first failure closes it.
    [Fact]
    public async Task Retry_Pending_Keeps_Notification_Open()
    {
        RequireEngine();
        Assert.SkipUnless(fixture.Capabilities.SupportsRetry, "engine does not retry failed jobs.");
        var id = NewId();

        var (_, notificationId) = await EnqueueWithNotificationAsync(
            new ConformancePayload { CorrelationId = id, Value = "retry-pending", FailAttempts = 1, ReportProgress = true });

        if (fixture.Capabilities.ReportsAttemptBudget)
        {
            var total = fixture.RetryBudget + 1;
            var update = await fixture.WaitForProgressAsync(
                notificationId,
                x => x.Description is not null && x.Description.StartsWith($"Attempt 1 of {total} failed, retrying"),
                ConformanceConstants.DefaultTimeout);

            Assert.Null(update.Finished);
            Assert.Null(update.Outcome);
            Assert.Equal(1, update.Attempt);
            Assert.Equal(total, update.TotalAttempts);
        }
        else
        {
            var update = await fixture.WaitForProgressAsync(
                notificationId, x => x.Outcome == nameof(JobOutcome.Failed), ConformanceConstants.DefaultTimeout);

            Assert.NotNull(update.Finished);
        }
    }

    // 15 — the failure that exhausts the attempts closes the notification Failed, with the error and the finish time.
    [Fact]
    public async Task Final_Failure_Closes_Notification_Failed()
    {
        RequireEngine();
        var id = NewId();

        var (_, notificationId) = await EnqueueWithNotificationAsync(
            new ConformancePayload { CorrelationId = id, Value = "final-failure", FailAlways = true, ReportProgress = true });

        var closing = await fixture.WaitForProgressAsync(
            notificationId, x => x.Outcome is not null, ConformanceConstants.DefaultTimeout);

        Assert.Equal(nameof(JobOutcome.Failed), closing.Outcome);
        Assert.False(string.IsNullOrEmpty(closing.Error));
        Assert.NotNull(closing.Finished);

        if (fixture.Capabilities.ReportsAttemptBudget)
        {
            Assert.NotNull(closing.TotalAttempts);
            Assert.Equal(closing.TotalAttempts, closing.Attempt);
        }
    }

    // 16 — a job that returns closes its notification Succeeded.
    [Fact]
    public async Task Success_Closes_Notification_Succeeded()
    {
        RequireEngine();
        var id = NewId();

        var (_, notificationId) = await EnqueueWithNotificationAsync(
            new ConformancePayload { CorrelationId = id, Value = "success", ReportProgress = true });

        var closing = await fixture.WaitForProgressAsync(
            notificationId, x => x.Outcome is not null, ConformanceConstants.DefaultTimeout);

        Assert.Equal(nameof(JobOutcome.Succeeded), closing.Outcome);
        Assert.Null(closing.Error);
        Assert.NotNull(closing.Finished);
    }

    // How long a scenario watches for a closing send an engine is documented NOT to make. Nothing signals the absence
    // of an event, so the check can only wait; it runs after the cancel was observed, never in place of that wait.
    private static readonly TimeSpan _noCloseObservation = TimeSpan.FromSeconds(1);

    // 17 — a requested cancel of a running job closes its notification Canceled. An engine that cannot tell a
    // requested cancel from a shutdown (ClosesCanceledNotification false) leaves it open: the documented fallback.
    [Fact]
    public async Task Running_Job_Cancel_Closes_Notification_Canceled()
    {
        RequireEngine();
        Assert.SkipUnless(fixture.Capabilities.SupportsCancellation, "engine does not support cancellation.");
        var id = NewId();

        var (jobId, notificationId) = await EnqueueWithNotificationAsync(
            new ConformancePayload { CorrelationId = id, Value = "cancel-notification", BlockUntilCancelled = true, ReportProgress = true });
        await fixture.Probe.WaitStartedAsync(id, ConformanceConstants.DefaultTimeout, TestContext.Current.CancellationToken);

        var accepted = await fixture.Services.GetRequiredService<IJobEngine>().Delete(jobId, TestContext.Current.CancellationToken);
        Assert.True(accepted, "the engine must accept the cancellation request for a known, running job.");

        await AssertCancelOutcomeAsync(id, notificationId, observeHandlerCancel: true);
    }

    // 18 — the same for a job that never ran: it is canceled while still queued, so no worker will ever close it.
    [Fact]
    public async Task Queued_Job_Cancel_Closes_Notification_Canceled()
    {
        RequireEngine();
        Assert.SkipUnless(fixture.Capabilities.CanHoldQueuedJob, "engine cannot hold a job queued.");
        Assert.SkipUnless(fixture.Capabilities.SupportsDelete, "engine does not support delete.");
        var id = NewId();

        var jobId = await EnqueueAsync(
            new ConformancePayload { CorrelationId = id, Value = "cancel-queued" },
            new EnqueueOptions { ReportProgress = true, Title = id, Queue = fixture.UnservedQueue });
        var notificationId = NotificationIdOf(id);

        var accepted = await fixture.Services.GetRequiredService<IJobEngine>().Delete(jobId, TestContext.Current.CancellationToken);
        Assert.True(accepted, "the engine must accept the cancellation request for a known, queued job.");

        await AssertCancelOutcomeAsync(id, notificationId, observeHandlerCancel: false);
        Assert.Equal(0, fixture.Probe.ExecutionCount(id));
    }

    // 19 — a cancel survives a handler that then fails with another exception: the failed attempt has a retry pending,
    // but the job was canceled, so the notification closes Canceled and the handler does not run again. In-process
    // engines and RabbitMQ close at the attempt after the failed one, Hangfire at the failed one (the job is already Deleted).
    [Fact]
    public async Task Running_Job_Cancel_Then_Handler_Failure_Closes_Canceled()
    {
        RequireEngine();
        Assert.SkipUnless(
            fixture.Capabilities.SupportsCancellation && fixture.Capabilities.SupportsRetry && fixture.Capabilities.ClosesCanceledNotification,
            "engine cannot cancel a retryable job and close it Canceled.");
        var id = NewId();

        var (jobId, notificationId) = await EnqueueWithNotificationAsync(
            new ConformancePayload { CorrelationId = id, Value = "cancel-then-fail", BlockUntilCancelled = true, FailOnCancel = true, ReportProgress = true });
        await fixture.Probe.WaitStartedAsync(id, ConformanceConstants.DefaultTimeout, TestContext.Current.CancellationToken);

        var accepted = await fixture.Services.GetRequiredService<IJobEngine>().Delete(jobId, TestContext.Current.CancellationToken);
        Assert.True(accepted, "the engine must accept the cancellation request for a known, running job.");

        var closing = await fixture.WaitForProgressAsync(
            notificationId, x => x.Outcome == nameof(JobOutcome.Canceled), ConformanceConstants.DefaultTimeout);

        Assert.NotNull(closing.Finished);
        Assert.Null(closing.Error);

        if (fixture.Capabilities.ReportsAttemptBudget)
        {
            Assert.Equal(fixture.RetryBudget + 1, closing.TotalAttempts);
            Assert.True(closing.Attempt >= 1, "the close carries the attempt it was made at.");
        }

        // No further attempt runs the handler: the absence of a run has no signal, so the check can only wait.
        await Task.Delay(_noCloseObservation, TestContext.Current.CancellationToken);

        Assert.Equal(1, fixture.Probe.ExecutionCount(id));
    }

    // 20 — the same on the final attempt is a failure: no retry was pending for the cancel to stop, so the error the
    // dispatcher closed Failed with is the only record of why the job ended and no engine replaces it with Canceled.
    [Fact]
    public async Task Running_Job_Cancel_Then_Handler_Failure_On_Final_Attempt_Closes_Failed()
    {
        RequireEngine();
        Assert.SkipUnless(
            fixture.Capabilities.SupportsCancellation && fixture.Capabilities.ClosesCanceledNotification && fixture.Capabilities.ReportsAttemptBudget,
            "engine cannot cancel a job, close it Canceled or report its attempt budget.");
        var id = NewId();

        // No retries: attempt 1 is the final attempt.
        var jobId = await EnqueueAsync(
            new ConformancePayload { CorrelationId = id, Value = "cancel-then-fail-final", BlockUntilCancelled = true, FailOnCancel = true, ReportProgress = true },
            new EnqueueOptions { ReportProgress = true, Title = id, MaxRetryAttempts = 0 });
        var notificationId = NotificationIdOf(id);
        await fixture.Probe.WaitStartedAsync(id, ConformanceConstants.DefaultTimeout, TestContext.Current.CancellationToken);

        var accepted = await fixture.Services.GetRequiredService<IJobEngine>().Delete(jobId, TestContext.Current.CancellationToken);
        Assert.True(accepted, "the engine must accept the cancellation request for a known, running job.");

        var closing = await fixture.WaitForProgressAsync(
            notificationId, x => x.Outcome == nameof(JobOutcome.Failed) && !string.IsNullOrEmpty(x.Error), ConformanceConstants.DefaultTimeout);

        Assert.NotNull(closing.Finished);

        // Nothing replaces the Failed close with Canceled afterwards: the absence of an update has no signal, so the check can only wait.
        await Task.Delay(_noCloseObservation, TestContext.Current.CancellationToken);

        Assert.DoesNotContain(fixture.ProgressUpdates(notificationId), x => x.Outcome == nameof(JobOutcome.Canceled));
        Assert.Equal(1, fixture.Probe.ExecutionCount(id));
    }

    // 21 — a cancel of a job that already finished changes nothing: the engine refuses it and the job keeps its state,
    // so a late cancel cannot rewrite a finished job's history.
    [Fact]
    public async Task Delete_Of_Succeeded_Job_Returns_False_And_Keeps_State()
    {
        RequireEngine();
        Assert.SkipUnless(fixture.Capabilities.SupportsStatusQuery && fixture.Capabilities.SupportsDelete, "engine cannot report status or delete.");
        var engine = fixture.Services.GetRequiredService<IJobEngine>();

        var jobId = await EnqueueAndWaitAsync(new ConformancePayload { CorrelationId = NewId(), Value = "delete-finished" });
        await fixture.WaitForStatusAsync(jobId, x => x.State == "Succeeded", ConformanceConstants.DefaultTimeout);

        var deleted = await engine.Delete(jobId, TestContext.Current.CancellationToken);

        Assert.False(deleted, "a finished job has nothing left to cancel.");
        var status = await engine.GetStatus(jobId, TestContext.Current.CancellationToken);
        Assert.NotNull(status);
        Assert.Equal("Succeeded", status.State);
    }

    // 22 — a per-job retry limit above the engine's global count runs the job limit + 1 times. Hangfire has no per-job
    // retry count, so this is the case its filter has to make work: the global filter gives up first. The wait is for
    // the notification's closing Failed, which an engine that does not report its attempt budget sends at the first failure.
    [Fact]
    public async Task MaxRetryAttempts_Above_Global_Runs_N_Plus_One()
    {
        RequireEngine();
        Assert.SkipUnless(
            fixture.Capabilities.SupportsRetry && fixture.Capabilities.ReportsAttemptBudget,
            "engine does not retry failed jobs or does not report its attempt budget.");
        var id = NewId();
        var limit = fixture.RetryBudget + 2;

        await EnqueueAsync(
            new ConformancePayload { CorrelationId = id, Value = "retry-above-global", FailAlways = true, ReportProgress = true },
            new EnqueueOptions { ReportProgress = true, Title = id, MaxRetryAttempts = limit });
        var closing = await fixture.WaitForProgressAsync(
            NotificationIdOf(id), x => x.Outcome is not null, ConformanceConstants.DefaultTimeout);

        Assert.Equal(nameof(JobOutcome.Failed), closing.Outcome);
        Assert.Equal(limit + 1, closing.TotalAttempts);
        Assert.Equal(fixture.RetryBudget + 3, fixture.Probe.ExecutionCount(id));
    }

    // 23 — a per-job retry limit below the engine's global count stops the job early: limit + 1 runs.
    [Fact]
    public async Task MaxRetryAttempts_Below_Global_Runs_N_Plus_One()
    {
        RequireEngine();
        Assert.SkipUnless(
            fixture.Capabilities.SupportsRetry && fixture.Capabilities.ReportsAttemptBudget,
            "engine does not retry failed jobs or does not report its attempt budget.");
        var id = NewId();
        const int limit = 1;
        Assert.True(limit < fixture.RetryBudget, "the scenario needs a global count above the per-job limit.");

        await EnqueueAsync(
            new ConformancePayload { CorrelationId = id, Value = "retry-below-global", FailAlways = true, ReportProgress = true },
            new EnqueueOptions { ReportProgress = true, Title = id, MaxRetryAttempts = limit });
        var closing = await fixture.WaitForProgressAsync(
            NotificationIdOf(id), x => x.Outcome is not null, ConformanceConstants.DefaultTimeout);

        Assert.Equal(nameof(JobOutcome.Failed), closing.Outcome);
        Assert.Equal(limit + 1, closing.TotalAttempts);
        Assert.Equal(limit + 1, fixture.Probe.ExecutionCount(id));
    }

    // 24 — one job id: the worker's context carries the id Enqueue returned, so the progress page finds its job.
    // An engine that passes a different id (a short name for a full one) breaks it with no error anywhere.
    [Fact]
    public async Task Worker_JobId_Equals_Enqueue_Result()
    {
        RequireEngine();
        var id = NewId();

        var jobId = await EnqueueAndWaitAsync(new ConformancePayload { CorrelationId = id, Value = "job-id" });

        Assert.Equal(jobId, fixture.Probe.Job(id)!.JobId);
    }

    // 25 — the same id on every attempt: a retry re-runs the handler, and neither the context nor the worker's
    // notification updates may switch to another id. The first send is the facade's "Queued" one, which carries no job
    // id by design; every send after it comes from a worker.
    [Fact]
    public async Task Worker_JobId_Is_Stable_Across_Retries()
    {
        RequireEngine();
        Assert.SkipUnless(fixture.Capabilities.SupportsRetry, "engine does not retry failed jobs.");
        var id = NewId();

        var (jobId, notificationId) = await EnqueueWithNotificationAsync(
            new ConformancePayload { CorrelationId = id, Value = "job-id-retry", FailAttempts = 1, ReportProgress = true });
        await fixture.Probe.WaitAsync(id, ConformanceConstants.DefaultTimeout, TestContext.Current.CancellationToken);
        await fixture.WaitForProgressAsync(
            notificationId, x => x.Outcome == nameof(JobOutcome.Succeeded), ConformanceConstants.DefaultTimeout);

        Assert.Equal([jobId, jobId], fixture.Probe.ExecutionJobIds(id));

        var workerUpdates = fixture.ProgressUpdates(notificationId).Skip(1).ToList();
        Assert.NotEmpty(workerUpdates);
        Assert.All(workerUpdates, x => Assert.Equal(jobId, x.JobId));
    }

    private async Task AssertCancelOutcomeAsync(string correlationId, string notificationId, bool observeHandlerCancel)
    {
        if (fixture.Capabilities.ClosesCanceledNotification)
        {
            var closing = await fixture.WaitForProgressAsync(
                notificationId, x => x.Outcome is not null, ConformanceConstants.DefaultTimeout);

            Assert.Equal(nameof(JobOutcome.Canceled), closing.Outcome);
            Assert.NotNull(closing.Finished);
            Assert.Null(closing.Error);

            if (fixture.Capabilities.ReportsAttemptBudget)
            {
                // The close must not overwrite the attempt numbers earlier sends wrote: a running job closes with the
                // numbers its last update carried; a job canceled before it ran shows the first attempt of the total.
                if (observeHandlerCancel)
                {
                    // By reference: notifications of one id compare equal.
                    var previous = fixture.ProgressUpdates(notificationId).TakeWhile(x => !ReferenceEquals(x, closing)).Last();

                    Assert.NotNull(previous.TotalAttempts);
                    Assert.Equal(previous.Attempt, closing.Attempt);
                    Assert.Equal(previous.TotalAttempts, closing.TotalAttempts);
                }
                else
                {
                    Assert.Equal(1, closing.Attempt);
                    Assert.Equal(fixture.RetryBudget + 1, closing.TotalAttempts);
                }
            }
        }
        else
        {
            if (observeHandlerCancel)
            {
                await fixture.Probe.WaitCancelledAsync(correlationId, ConformanceConstants.CancellationTimeout, TestContext.Current.CancellationToken);
            }

            // An absence of a close has no engine-agnostic signal, so the check can only wait.
            await Task.Delay(_noCloseObservation, TestContext.Current.CancellationToken);

            Assert.DoesNotContain(fixture.ProgressUpdates(notificationId), x => x.Outcome is not null);
        }
    }
}
