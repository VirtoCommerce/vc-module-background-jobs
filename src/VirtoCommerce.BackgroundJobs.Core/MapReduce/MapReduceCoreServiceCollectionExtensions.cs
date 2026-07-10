using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs.Core.MapReduce;

/// <summary>Registers the engine-agnostic map/reduce orchestration that lives in this module.</summary>
public static class MapReduceCoreServiceCollectionExtensions
{
    /// <summary>
    /// Registers the engine-agnostic map/reduce orchestration: the <see cref="IMapReduceJob"/> facade and the two
    /// coordinators (as ordinary background-job handlers). The batch store is registered separately by the Data layer
    /// (<c>AddMapReduce</c>), which calls this. Idempotent.
    /// </summary>
    public static IServiceCollection AddMapReduceCore(this IServiceCollection services)
    {
        services.TryAddScoped<IMapReduceJob, MapReduceJob>();
        services.AddBackgroundJob<FanOutCoordinator, MapFanOutEnvelope>();
        services.AddBackgroundJob<MapCoordinator, MapTaskEnvelope>();
        services.AddBackgroundJob<ReduceCoordinator, ReduceTaskEnvelope>();
        return services;
    }
}
