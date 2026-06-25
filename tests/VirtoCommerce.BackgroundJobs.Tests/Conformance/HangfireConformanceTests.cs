#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Hangfire;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using VirtoCommerce.BackgroundJobs.Conformance;
using VirtoCommerce.BackgroundJobs.Core.Services;
using VirtoCommerce.BackgroundJobs.Hangfire;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Hangfire.Extensions;

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
        SupportsExpressionEnqueue = true,
        SupportsUniqueKeyDedup = false,
        SupportsQueueRouting = true,
        SupportsRetry = true,
        SupportsRecurringScheduler = true,
    };

    protected override bool TryConfigureEngine(IServiceCollection services, out string? unavailableReason)
    {
        unavailableReason = null;

        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["VirtoCommerce:BackgroundJobs:Provider"] = "Hangfire",
                ["VirtoCommerce:BackgroundJobs:MaxRetryAttempts"] = "3",
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
        GlobalJobFilters.Filters.Add(new AutomaticRetryAttribute { Attempts = 3, DelaysInSeconds = [0, 0, 0], OnAttemptsExceeded = AttemptsExceededAction.Fail });

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
    : JobEngineConformanceTests<HangfireConformanceFixture>(fixture);
