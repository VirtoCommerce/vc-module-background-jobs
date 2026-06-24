using System;
using System.Linq;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs.Core.MapReduce;

/// <summary>DI helpers for map/reduce jobs.</summary>
public static class MapReduceServiceCollectionExtensions
{
    /// <summary>
    /// Registers a module's map and reduce handlers for one batch type. Call from a module's
    /// <c>Initialize(IServiceCollection)</c>. The engine-agnostic coordinators and the <see cref="IMapReduceJob"/>
    /// facade are registered once by the host module (see <c>AddMapReduceCore</c>).
    /// </summary>
    public static IServiceCollection AddMapReduceJob<TItem, TResult, TState, TMap, TReduce>(this IServiceCollection services)
        where TItem : class
        where TResult : class
        where TState : class
        where TMap : class, IMapJobHandler<TItem, TResult>
        where TReduce : class, IReduceJobHandler<TState, TResult>
    {
        services.AddTransient<IMapJobHandler<TItem, TResult>, TMap>();
        services.AddTransient<IReduceJobHandler<TState, TResult>, TReduce>();
        return services;
    }

    /// <summary>
    /// Registers a module's map and reduce handlers, inferring the item/result/state types from the handler
    /// interfaces — a convenience over the five-type-parameter overload. Validates that the map and reduce handlers
    /// agree on the result type. Use <see cref="AddMapReduceJob{TItem, TResult, TState, TMap, TReduce}"/> when you
    /// prefer to state the types explicitly (compile-time checked, trim/AOT-friendly).
    /// </summary>
    public static IServiceCollection AddMapReduceJob<TMap, TReduce>(this IServiceCollection services)
        where TMap : class
        where TReduce : class
    {
        // GetInterfaces returns the CLOSED generics — register them directly (no MakeGenericType).
        var mapInterface = SingleClosedInterface(typeof(TMap), typeof(IMapJobHandler<,>), "IMapJobHandler");
        var reduceInterface = SingleClosedInterface(typeof(TReduce), typeof(IReduceJobHandler<,>), "IReduceJobHandler");

        // The map produces what the reduce consumes — the TResult of both must match.
        if (mapInterface.GetGenericArguments()[1] != reduceInterface.GetGenericArguments()[1])
        {
            throw new ArgumentException(
                $"{typeof(TMap).Name} and {typeof(TReduce).Name} disagree on the map result type.");
        }

        services.AddTransient(mapInterface, typeof(TMap));
        services.AddTransient(reduceInterface, typeof(TReduce));
        return services;
    }

    private static Type SingleClosedInterface(Type handlerType, Type openGeneric, string friendlyName)
    {
        var matches = handlerType.GetInterfaces()
            .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == openGeneric)
            .ToArray();

        return matches.Length switch
        {
            1 => matches[0],
            0 => throw new ArgumentException($"{handlerType.Name} must implement {friendlyName}<...>."),
            _ => throw new ArgumentException(
                $"{handlerType.Name} implements {friendlyName}<...> for multiple type arguments; use the explicit AddMapReduceJob overload."),
        };
    }

    /// <summary>
    /// Registers the engine-agnostic map/reduce orchestration: the <see cref="IMapReduceJob"/> facade and the two
    /// coordinators (as ordinary background-job handlers). The batch store is registered separately by the Data layer
    /// (<c>AddMapReduce</c>), which calls this. Idempotent.
    /// </summary>
    public static IServiceCollection AddMapReduceCore(this IServiceCollection services)
    {
        services.TryAddScoped<IMapReduceJob, MapReduceJob>();
        services.AddBackgroundJob<MapFanOutEnvelope, FanOutCoordinator>();
        services.AddBackgroundJob<MapTaskEnvelope, MapCoordinator>();
        services.AddBackgroundJob<ReduceTaskEnvelope, ReduceCoordinator>();
        return services;
    }
}
