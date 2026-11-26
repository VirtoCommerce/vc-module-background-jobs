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

        // Production's order: Hangfire's default (10 attempts) first, the configured one after it. A reader of the first one would show 10.
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

        // Production installs these in UseHangfire, which this fixture never runs.
        HangfireJobFilterRegistration.RegisterServiceFilters(services);

        await base.StartWorkerAsync(services, cancellationToken);
    }
}

public class HangfireConformanceTests(HangfireConformanceFixture fixture)
    : JobEngineConformanceTests<HangfireConformanceFixture>(fixture)
{
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

        await Fixture.WaitForProgressAsync(notificationId, x => x.Outcome is not null, ConformanceConstants.DefaultTimeout);
        await Fixture.WaitForStatusAsync(jobId, x => x.State == "Failed", ConformanceConstants.DefaultTimeout);
        Assert.Equal(total, Fixture.Probe.ExecutionCount(correlationId));

        Assert.True(Fixture.Services.GetRequiredService<IBackgroundJobClient>().Requeue(jobId), "Hangfire refused to requeue the failed job.");
        await Fixture.WaitForStatusAsync(jobId, x => x.State == "Failed", ConformanceConstants.DefaultTimeout);
        Assert.Equal(total + 1, Fixture.Probe.ExecutionCount(correlationId));

        var last = Fixture.ProgressUpdates(notificationId).Last();
        Assert.Equal(nameof(JobOutcome.Failed), last.Outcome);
        Assert.NotNull(last.Finished);
        Assert.Equal(total, last.TotalAttempts);
        Assert.Equal(last.TotalAttempts, last.Attempt);
    }

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

            // The requeued job restarts at once while the first run is still blocked: wait for the second execution and for the first to observe the cancel.
            await WaitForExecutionCountAsync(correlationId, 2);
            await Fixture.Probe.WaitCancelledAsync(correlationId, ConformanceConstants.CancellationTimeout, TestContext.Current.CancellationToken);

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
