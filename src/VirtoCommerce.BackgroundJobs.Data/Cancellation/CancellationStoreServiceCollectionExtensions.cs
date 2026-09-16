#nullable enable
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using StackExchange.Redis;
using VirtoCommerce.BackgroundJobs.Core.Cancellation;

namespace VirtoCommerce.BackgroundJobs.Data.Cancellation;

/// <summary>
/// DI helper that registers <see cref="IJobCancellationStore"/> — Redis when an <see cref="IConnectionMultiplexer"/>
/// is configured (fleet-wide cancel), otherwise the single-process in-memory store. TryAdd so a custom engine may
/// supply its own store before calling this. Called once by the host module.
/// </summary>
public static class CancellationStoreServiceCollectionExtensions
{
    public static IServiceCollection AddJobCancellationStore(this IServiceCollection services)
    {
        services.TryAddSingleton<IJobCancellationStore>(serviceProvider =>
        {
            var connection = serviceProvider.GetService<IConnectionMultiplexer>();
            return connection is not null
                ? new RedisJobCancellationStore(connection)
                : new InMemoryJobCancellationStore();
        });

        return services;
    }
}
