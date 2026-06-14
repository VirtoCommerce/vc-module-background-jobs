using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace VirtoCommerce.BackgroundJobs.RabbitMQ.Extensions;

/// <summary>
/// DI registration for the RabbitMQ background-job engine.
/// <para>
/// Provided for future wiring — it is NOT called yet. The module currently runs the Hangfire engine; RabbitMQ
/// becomes selectable once the message-based engine port and config-based engine selection are introduced.
/// </para>
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>Configuration section the RabbitMQ options bind to (provisional).</summary>
    public const string ConfigurationSection = "VirtoCommerce:BackgroundJobs:RabbitMQ";

    public static IServiceCollection AddRabbitMqBackgroundJobs(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<RabbitMqOptions>(configuration.GetSection(ConfigurationSection));
        services.TryAddSingleton<IRabbitMqConnectionProvider, RabbitMqConnectionProvider>();
        services.TryAddSingleton<RabbitMqBackgroundJobEngine>();

        return services;
    }
}
