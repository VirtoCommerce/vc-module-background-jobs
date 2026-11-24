#nullable enable
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Hangfire;
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
        Assert.Equal(JobOutcome.Failed, last.Outcome);
        Assert.NotNull(last.Finished);
        Assert.Equal(total, last.TotalAttempts);
        Assert.Equal(last.TotalAttempts, last.Attempt);
    }
}
