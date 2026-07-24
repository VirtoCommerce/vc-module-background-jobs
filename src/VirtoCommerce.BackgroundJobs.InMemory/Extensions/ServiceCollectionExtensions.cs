#nullable enable
using Microsoft.Extensions.DependencyInjection;
using VirtoCommerce.BackgroundJobs;

namespace VirtoCommerce.BackgroundJobs.InMemory.Extensions;

/// <summary>
/// DI registration for the in-memory background-job engine. Called by the module's PlatformStartup when
/// <c>VirtoCommerce:BackgroundJobs:Provider</c> is <c>InMemory</c>.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the infrastructure-free <see cref="InMemoryJobEngine"/> as the active <see cref="IJobEngine"/>.
    /// Local development / testing only — non-durable and single-process; pair with the in-process recurring
    /// scheduler for cron support.
    /// </summary>
    public static IServiceCollection AddInMemoryJobEngine(this IServiceCollection services)
    {
        services.AddSingleton<IJobEngine, InMemoryJobEngine>();

        return services;
    }
}
