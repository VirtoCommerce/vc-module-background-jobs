#nullable enable
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Hangfire;
using Hangfire.States;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using VirtoCommerce.BackgroundJobs;
using VirtoCommerce.BackgroundJobs.Conformance;
using VirtoCommerce.BackgroundJobs.Hangfire;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Hangfire.Extensions;
using Xunit;

namespace VirtoCommerce.BackgroundJobs.Tests.Conformance;

/// <summary>
/// Certifies the real Hangfire engine running in-process with a real Hangfire server. Storage is Hangfire's
/// in-memory storage by default (still the real engine + server, no external DB); point
/// <c>VirtoCommerce:Hangfire:JobStorageType</c> at a database via configuration to certify against persistent
/// storage.
/// </summary>
public sealed class HangfireConformanceFixture : JobEngineConformanceFixture
{
    public override EngineCapabilities Capabilities => new()
    {
        SupportsStatusQuery = true,
        SupportsDelete = true,
        SupportsUniqueKeyDedup = false,
        SupportsQueueRouting = true,
        SupportsRetry = true,
        SupportsRecurringScheduler = true,
        SupportsCancellation = true,
        ReportsAttemptBudget = true,
        ClosesCanceledNotification = true,
        CanHoldQueuedJob = true,
    };

    public override int RetryBudget => 3;

    protected override bool TryConfigureEngine(IServiceCollection services, out string? unavailableReason)
    {
        unavailableReason = null;

        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["VirtoCommerce:BackgroundJobs:Provider"] = "Hangfire",
                ["VirtoCommerce:BackgroundJobs:MaxRetryAttempts"] = RetryBudget.ToString(CultureInfo.InvariantCulture),
            })
            .Build();

        // The real module Hangfire wiring (in-memory storage when no JobStorageType is configured).
        services.AddHangfire(config);
        services.AddTransient<HangfireJobExecutor>();
        services.AddSingleton<IJobEngine, HangfireJobEngine>();
        services.AddTransient<IRecurringJobInvoker, RecurringJobInvoker>();
        services.AddSingleton<IRecurringJobScheduler, HangfireRecurringJobScheduler>();

        // Hangfire's default retry backoff is far too slow for a test; replace it with instant retries so the retry
        // scenario is deterministic and fast.
        var retryFilters = GlobalJobFilters.Filters.Where(f => f.Instance is AutomaticRetryAttribute).Select(f => f.Instance).ToList();
        foreach (var filter in retryFilters)
        {
            GlobalJobFilters.Filters.Remove(filter);
        }

        // Production registers two: Hangfire's own default (10 attempts) and, after it, the module's configured one
        // (AddHangfire does not remove the default). Hangfire keeps the later of two filters of a type, so the fixture
        // mirrors that [default, configured] order — a reader that picks the first one would read 10 and show a total
        // the job never reaches.
        GlobalJobFilters.Filters.Add(new AutomaticRetryAttribute());
        GlobalJobFilters.Filters.Add(new AutomaticRetryAttribute { Attempts = RetryBudget, DelaysInSeconds = new int[RetryBudget], OnAttemptsExceeded = AttemptsExceededAction.Fail });

        // Run the real Hangfire server in-process; drain the default + conformance queues.
        services.AddHangfireServer(options => options.Queues = ["default", ConformanceConstants.CustomQueue.ToLowerInvariant()]);
        return true;
    }

    protected override async Task StartWorkerAsync(IServiceProvider services, CancellationToken cancellationToken)
    {
        // The engine's GetStatus / Delete / expression enqueue use Hangfire's static JobStorage.Current; point it at
        // the same storage instance the DI-hosted server uses so they agree.
        JobStorage.Current = services.GetRequiredService<JobStorage>();

        // This fixture never runs UseHangfire, which is where production installs the filters that need the container;
        // register them from this fixture's provider, replacing any left by an earlier fixture in the process.
        HangfireJobFilterRegistration.RegisterServiceFilters(services);

        await base.StartWorkerAsync(services, cancellationToken);
    }
}

public class HangfireConformanceTests(HangfireConformanceFixture fixture)
    : JobEngineConformanceTests<HangfireConformanceFixture>(fixture)
{
    // A Failed Hangfire job can be requeued (the dashboard's Requeue button, or an operator's retry): it runs again
    // with its RetryCount already spent. That run is the job's last, so its failure must close the notification
    // rather than leave it open on "attempt N of M failed, retrying" for a retry that never comes.
    [Fact]
    public async Task Requeued_Exhausted_Job_Closes_Failed()
    {
        var title = Guid.NewGuid().ToString("N");
        var correlationId = Guid.NewGuid().ToString("N");
        var total = Fixture.RetryBudget + 1;

        string jobId;
        using (var scope = Fixture.Services.CreateScope())
        {
            var jobs = scope.ServiceProvider.GetRequiredService<IBackgroundJob>();
            jobId = await jobs.Enqueue<RecordingConformanceHandler>(
                new ConformancePayload { CorrelationId = correlationId, FailAlways = true },
                new EnqueueOptions { ReportProgress = true, Title = title },
                TestContext.Current.CancellationToken);
        }

        var notificationId = Fixture.ProgressNotifications.First(x => x.Title == title).Id;

        // The first run: every attempt fails and the job lands in Failed with its retries spent.
        await Fixture.WaitForProgressAsync(notificationId, x => x.Outcome is not null, ConformanceConstants.DefaultTimeout);
        await Fixture.WaitForStatusAsync(jobId, x => x.State == "Failed", ConformanceConstants.DefaultTimeout);
        Assert.Equal(total, Fixture.Probe.ExecutionCount(correlationId));

        // Requeue it through the Hangfire client and wait for the extra run to end Failed again.
        Assert.True(Fixture.Services.GetRequiredService<IBackgroundJobClient>().Requeue(jobId), "Hangfire refused to requeue the failed job.");
        await Fixture.WaitForStatusAsync(jobId, x => x.State == "Failed", ConformanceConstants.DefaultTimeout);
        Assert.Equal(total + 1, Fixture.Probe.ExecutionCount(correlationId));

        var last = Fixture.ProgressUpdates(notificationId).Last();
        Assert.Equal(nameof(JobOutcome.Failed), last.Outcome);
        Assert.NotNull(last.Finished);
        Assert.Equal(total, last.TotalAttempts);
        Assert.Equal(last.TotalAttempts, last.Attempt);
    }

    // The Hangfire dashboard's Delete moves the job to Deleted through the client; no engine method is involved, so
    // nothing but the executor's own read of the stored state can tell this cancel from a shutdown.
    [Fact]
    public async Task Running_Job_Deleted_Without_Engine_Closes_Canceled()
    {
        var title = Guid.NewGuid().ToString("N");
        var correlationId = Guid.NewGuid().ToString("N");

        var jobId = await EnqueueBlockingJobAsync(title, correlationId);
        var notificationId = Fixture.ProgressNotifications.First(x => x.Title == title).Id;
        await Fixture.Probe.WaitStartedAsync(correlationId, ConformanceConstants.DefaultTimeout, TestContext.Current.CancellationToken);

        Assert.True(
            Fixture.Services.GetRequiredService<IBackgroundJobClient>().ChangeState(jobId, new DeletedState(), ProcessingState.StateName),
            "Hangfire refused to move the running job to Deleted.");

        var closing = await Fixture.WaitForProgressAsync(notificationId, x => x.Outcome is not null, ConformanceConstants.DefaultTimeout);

        Assert.Equal(nameof(JobOutcome.Canceled), closing.Outcome);
        Assert.NotNull(closing.Finished);
    }

    // A requeue (the dashboard's Requeue button, or a worker lost to another server) trips the running job's token
    // exactly as a cancel does, but Hangfire runs the job again: the notification must stay open for that second run.
    [Fact]
    public async Task Running_Job_Requeued_Stays_Open()
    {
        var title = Guid.NewGuid().ToString("N");
        var correlationId = Guid.NewGuid().ToString("N");

        var jobId = await EnqueueBlockingJobAsync(title, correlationId);
        var notificationId = Fixture.ProgressNotifications.First(x => x.Title == title).Id;
        await Fixture.Probe.WaitStartedAsync(correlationId, ConformanceConstants.DefaultTimeout, TestContext.Current.CancellationToken);

        try
        {
            Assert.True(Fixture.Services.GetRequiredService<IBackgroundJobClient>().Requeue(jobId), "Hangfire refused to requeue the running job.");

            // A requeued job is picked up again at once, while the first run is still blocked: it is the first run's
            // token tripping (Hangfire notices the state change on its next poll) that ends it, and that is the moment
            // its state gets classified. So wait for the second execution and for the first to observe the cancel.
            await WaitForExecutionCountAsync(correlationId, 2);
            await Fixture.Probe.WaitCancelledAsync(correlationId, ConformanceConstants.CancellationTimeout, TestContext.Current.CancellationToken);

            // Nothing signals that a send did not happen, so watch for a wrong close for a bounded time; the classifier
            // runs right after the handler rethrows, well inside it.
            await Task.Delay(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken);

            Assert.DoesNotContain(Fixture.ProgressUpdates(notificationId), x => x.Outcome is not null);
        }
        finally
        {
            // Release the job still blocked in its second run, so it does not outlive the test.
            Fixture.Services.GetRequiredService<IBackgroundJobClient>().Delete(jobId);
        }
    }

    private async Task<string> EnqueueBlockingJobAsync(string title, string correlationId)
    {
        using var scope = Fixture.Services.CreateScope();
        var jobs = scope.ServiceProvider.GetRequiredService<IBackgroundJob>();

        return await jobs.Enqueue<RecordingConformanceHandler>(
            new ConformancePayload { CorrelationId = correlationId, BlockUntilCancelled = true },
            new EnqueueOptions { ReportProgress = true, Title = title },
            TestContext.Current.CancellationToken);
    }

    private async Task WaitForExecutionCountAsync(string correlationId, int count)
    {
        var deadline = DateTime.UtcNow + ConformanceConstants.DefaultTimeout;
        while (Fixture.Probe.ExecutionCount(correlationId) < count)
        {
            Assert.True(DateTime.UtcNow < deadline, $"Timed out waiting for execution {count} of the job; saw {Fixture.Probe.ExecutionCount(correlationId)}.");
            await Task.Delay(TimeSpan.FromMilliseconds(25), TestContext.Current.CancellationToken);
        }
    }
}
