using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using VirtoCommerce.BackgroundJobs;

namespace VirtoCommerce.BackgroundJobs.GoogleCloudTasks.Extensions;

/// <summary>
/// DI registration for the Google Cloud Tasks engine. Called by <see cref="PlatformStartup"/> when
/// <c>VirtoCommerce:BackgroundJobs:Provider</c> is <c>GoogleCloudTasks</c>.
/// </summary>
public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddGoogleCloudTasksJobEngine(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<GoogleCloudTasksOptions>(configuration.GetSection(GoogleCloudTasksConstants.ConfigurationSection));
        services.TryAddSingleton<IGoogleCloudTasksTokenValidator, GoogleCloudTasksTokenValidator>();
        services.AddSingleton<IJobEngine, GoogleCloudTasksJobEngine>();

        return services;
    }
}
