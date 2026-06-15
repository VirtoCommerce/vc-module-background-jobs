using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using VirtoCommerce.BackgroundJobs.Core.Services;

namespace VirtoCommerce.BackgroundJobs.RabbitMQ.Extensions;

/// <summary>
/// DI registration for the RabbitMQ background-job engine. Called by the module's PlatformStartup when
/// <c>VirtoCommerce:BackgroundJobs:Provider</c> is <c>RabbitMQ</c>.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>Top-level, provider-specific configuration section the RabbitMQ options bind to.</summary>
    public const string ConfigurationSection = "VirtoCommerce:RabbitMQ";

    /// <summary>
    /// Registers the RabbitMQ options, shared connection provider and the RabbitMQ <see cref="IJobEngine"/>
    /// (the producer side — needed on every instance that enqueues).
    /// </summary>
    public static IServiceCollection AddRabbitMqJobEngine(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<RabbitMqOptions>(configuration.GetSection(ConfigurationSection));
        services.TryAddSingleton<IRabbitMqConnectionProvider, RabbitMqConnectionProvider>();
        services.AddSingleton<IJobEngine, RabbitMqJobEngine>();

        return services;
    }

    /// <summary>
    /// Registers the in-process consumer that processes jobs on this instance (Worker/Both modes).
    /// </summary>
    public static IServiceCollection AddRabbitMqJobConsumer(this IServiceCollection services)
    {
        services.AddHostedService<RabbitMqJobConsumer>();

        return services;
    }
}
