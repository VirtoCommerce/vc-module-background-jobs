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
        // Internal orchestration handlers: triggerable: false so they're never runnable on demand by name with a
        // caller-crafted envelope (which could corrupt a batch's state or fire a spurious reduce). Map/reduce work is
        // started through IMapReduceJob.Enqueue, not by triggering a coordinator directly.
        services.AddBackgroundJob<FanOutCoordinator, MapFanOutEnvelope>(triggerable: false);
        services.AddBackgroundJob<MapCoordinator, MapTaskEnvelope>(triggerable: false);
        services.AddBackgroundJob<ReduceCoordinator, ReduceTaskEnvelope>(triggerable: false);
        return services;
    }
}
