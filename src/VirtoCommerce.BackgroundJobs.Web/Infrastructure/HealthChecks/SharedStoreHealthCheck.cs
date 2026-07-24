#nullable enable
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using VirtoCommerce.BackgroundJobs.Core;
using VirtoCommerce.BackgroundJobs.Core.MapReduce;
using VirtoCommerce.BackgroundJobs.Core.Recurring;
using VirtoCommerce.BackgroundJobs.Data.MapReduce;
using VirtoCommerce.BackgroundJobs.Data.Recurring;

namespace VirtoCommerce.BackgroundJobs.Web.Infrastructure.HealthChecks;

/// <summary>
/// Reports <b>Degraded</b> when a queue-backed engine (RabbitMQ / any non-Hangfire provider) is using the
/// <b>in-memory</b> map/reduce or recurring-state store. That state is per-process, so under a Producer/Worker split
/// or 2+ instances map/reduce fan-out silently breaks (the reduce never fires) and cron occurrences re-fire per
/// instance. Configure Redis (<c>ConnectionStrings:RedisConnectionString</c>) for those topologies. Healthy for a
/// Redis-backed store, a custom store, or a single-instance Hangfire deployment.
/// </summary>
public sealed class SharedStoreHealthCheck(
    IConfiguration configuration,
    IMapReduceBatchStore mapReduceStore,
    // Optional: only registered by the in-process recurring scheduler (non-Hangfire engines).
    IRecurringJobStateStore? recurringStore = null) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var provider = configuration.GetBackgroundJobsProvider();
        var isHangfire = string.Equals(provider, BackgroundJobsProviders.Hangfire, StringComparison.OrdinalIgnoreCase);

        var mapInMemory = mapReduceStore is InMemoryMapReduceBatchStore;
        var recurringInMemory = recurringStore is InMemoryRecurringJobStateStore;

        HealthCheckResult result;
        if (isHangfire || (!mapInMemory && !recurringInMemory))
        {
            result = HealthCheckResult.Healthy($"Background-job shared-state store OK (provider '{provider}').");
        }
        else
        {
            var inMemory = string.Join(" + ", new[]
            {
                mapInMemory ? "map/reduce" : null,
                recurringInMemory ? "recurring" : null,
            }.Where(x => x is not null));

            result = HealthCheckResult.Degraded(
                $"Provider '{provider}' is queue-backed but the {inMemory} store is in-memory (per-process). This is NOT " +
                "safe for multi-instance / Producer-Worker topologies — map/reduce reduce may never fire and cron " +
                "occurrences re-fire per instance. Configure Redis via ConnectionStrings:RedisConnectionString.");
        }

        return Task.FromResult(result);
    }
}
