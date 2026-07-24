#nullable enable
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using StackExchange.Redis;
using VirtoCommerce.BackgroundJobs.Core.MapReduce;

namespace VirtoCommerce.BackgroundJobs.Data.MapReduce;

/// <summary>
/// DI helper that wires up engine-agnostic map/reduce: the orchestration (facade + coordinators, via
/// <see cref="MapReduceCoreServiceCollectionExtensions.AddMapReduceCore"/>) plus the batch store — Redis when an
/// <see cref="IConnectionMultiplexer"/> is configured (fleet-safe), otherwise in-memory (single instance only). A
/// queue-backed engine on the in-memory store is flagged Degraded by the "Background jobs store" health check, since
/// a batch created in one process is invisible to coordinators elsewhere. Called once by the host module.
/// </summary>
public static class MapReduceStoreServiceCollectionExtensions
{
    public static IServiceCollection AddMapReduce(this IServiceCollection services)
    {
        services.AddMapReduceCore();

        // TryAdd so a custom deployment may register its own store before calling this.
        services.TryAddSingleton<IMapReduceBatchStore>(serviceProvider =>
        {
            var connection = serviceProvider.GetService<IConnectionMultiplexer>();
            return connection is not null
                ? new RedisMapReduceBatchStore(connection)
                : new InMemoryMapReduceBatchStore();
        });

        return services;
    }
}
