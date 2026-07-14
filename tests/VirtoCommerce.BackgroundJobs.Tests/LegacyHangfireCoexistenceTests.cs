#nullable enable
using System.Collections.Generic;
using System.Linq;
using Hangfire;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using VirtoCommerce.BackgroundJobs.Core;
using VirtoCommerce.BackgroundJobs.Core.Services;
using VirtoCommerce.BackgroundJobs.RabbitMQ;
using VirtoCommerce.BackgroundJobs.Web;
using Xunit;

namespace VirtoCommerce.BackgroundJobs.Tests;

/// <summary>
/// Verifies the RabbitMQ-primary + legacy-Hangfire coexistence wiring in <see cref="PlatformStartup"/>: RabbitMQ is the
/// active <see cref="IJobEngine"/> while Hangfire's infrastructure (<see cref="IBackgroundJobClient"/> /
/// <see cref="IRecurringJobManager"/>) is bootstrapped or skipped by <c>VirtoCommerce:BackgroundJobs:EnableLegacyHangfire</c>.
/// Assertions inspect the registered <see cref="ServiceDescriptor"/>s (no provider is built), so no broker/SQL is needed.
/// </summary>
public class LegacyHangfireCoexistenceTests
{
    private static IServiceCollection BuildServices(bool enableLegacyHangfire)
    {
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["VirtoCommerce:BackgroundJobs:Provider"] = BackgroundJobsProviders.RabbitMq,
                ["VirtoCommerce:BackgroundJobs:EnableLegacyHangfire"] = enableLegacyHangfire ? "true" : "false",
                // Memory storage keeps the Hangfire bootstrap free of any SQL connection during the test.
                ["VirtoCommerce:Hangfire:JobStorageType"] = "Memory",
            })
            .Build();

        var services = new ServiceCollection();
        services.AddLogging();

        var startup = new PlatformStartup { Logger = NullLogger.Instance };
        startup.ConfigureServices(services, config);

        return services;
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void RabbitMq_Is_The_Active_Engine_Regardless_Of_Legacy_Flag(bool enableLegacyHangfire)
    {
        var services = BuildServices(enableLegacyHangfire);

        var engine = services.LastOrDefault(d => d.ServiceType == typeof(IJobEngine));

        Assert.NotNull(engine);
        Assert.Equal(typeof(RabbitMqJobEngine), engine!.ImplementationType);
    }

    [Fact]
    public void EnableLegacyHangfire_True_Bootstraps_Hangfire_Infrastructure()
    {
        var services = BuildServices(enableLegacyHangfire: true);

        // Hangfire's client/manager are available for legacy modules that call the Hangfire API directly.
        Assert.Contains(services, d => d.ServiceType == typeof(IBackgroundJobClient));
        Assert.Contains(services, d => d.ServiceType == typeof(IRecurringJobManager));
    }

    [Fact]
    public void EnableLegacyHangfire_False_Skips_Hangfire_Infrastructure()
    {
        var services = BuildServices(enableLegacyHangfire: false);

        // Pure non-Hangfire instance: RabbitMQ engine present, no Hangfire client registered.
        Assert.Contains(services, d => d.ServiceType == typeof(IJobEngine));
        Assert.DoesNotContain(services, d => d.ServiceType == typeof(IBackgroundJobClient));
    }
}
