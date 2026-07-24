#nullable enable
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using StackExchange.Redis;
using VirtoCommerce.BackgroundJobs.Core.Recurring;
using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs.Data.Recurring;

/// <summary>
/// DI helper for engines without a native recurring scheduler (RabbitMQ and custom queue-based engines). Registers
/// the in-process cron scheduler (<see cref="GenericRecurringJobScheduler"/>) as the active
/// <see cref="IRecurringJobScheduler"/> plus its hosted timer, backed by a shared occurrence-marker store
/// (<see cref="RedisRecurringJobStateStore"/> when Redis is configured, otherwise
/// <see cref="InMemoryRecurringJobStateStore"/>). A custom engine module can call this from its
/// <c>IPlatformStartup.ConfigureServices</c> to get cron recurring without reimplementing it.
/// <para>
/// Fleet-safe <b>only when Redis is configured</b>; the in-memory fallback is per-process and safe for a single
/// instance only. The "Background jobs store" health check reports Degraded if a queue-backed engine falls back to
/// in-memory (see <c>SharedStoreHealthCheck</c>).
/// </para>
/// </summary>
public static class RecurringSchedulerServiceCollectionExtensions
{
    public static IServiceCollection AddInProcessRecurringScheduler(this IServiceCollection services)
    {
        // TryAdd so a custom engine may supply its own state store before calling this helper.
        services.TryAddSingleton<IRecurringJobStateStore>(serviceProvider =>
        {
            var connection = serviceProvider.GetService<IConnectionMultiplexer>();
            return connection is not null
                ? new RedisRecurringJobStateStore(connection)
                : new InMemoryRecurringJobStateStore();
        });

        services.TryAddSingleton<GenericRecurringJobScheduler>();
        services.AddSingleton<IRecurringJobScheduler>(sp => sp.GetRequiredService<GenericRecurringJobScheduler>());
        services.AddSingleton<IHostedService>(sp => sp.GetRequiredService<GenericRecurringJobScheduler>());

        return services;
    }
}
