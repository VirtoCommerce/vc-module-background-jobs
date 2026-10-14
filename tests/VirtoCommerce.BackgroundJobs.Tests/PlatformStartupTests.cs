#nullable enable
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using VirtoCommerce.BackgroundJobs.Data.Recurring;
using VirtoCommerce.BackgroundJobs.Web;
using Xunit;

namespace VirtoCommerce.BackgroundJobs.Tests;

// VCST-6287: hosted services registered from Startup.ConfigureServices start BEFORE Startup.Configure registers the
// platform/module settings, so the applier read every setting as its descriptor default. Host-level services
// (ConfigureHostServices) start after Startup.Configure.
public class PlatformStartupTests
{
    private static IConfiguration Config(string provider, string mode) =>
        new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["VirtoCommerce:BackgroundJobs:Provider"] = provider,
                ["VirtoCommerce:BackgroundJobs:Mode"] = mode,
                ["VirtoCommerce:BackgroundJobs:EnableLegacyHangfire"] = "false",
                ["VirtoCommerce:Hangfire:UseHangfireServer"] = "false",
            })
            .Build();

    [Theory]
    [InlineData("Hangfire", "Both")]
    [InlineData("Hangfire", "Producer")]
    [InlineData("RabbitMQ", "Both")]
    [InlineData("RabbitMQ", "Worker")]
    [InlineData("RabbitMQ", "Producer")]
    [InlineData("InMemory", "Both")]
    public void ConfigureHostServices_Registers_RecurringJobsApplier_AsHostedService(string provider, string mode)
    {
        var services = new ServiceCollection();

        new PlatformStartup { Logger = NullLogger.Instance }.ConfigureHostServices(services, Config(provider, mode));

        Assert.Contains(services, x => x.ServiceType == typeof(RecurringJobsApplier));
        Assert.Contains(services, x => x.ServiceType == typeof(IHostedService) && x.ImplementationFactory is not null);
    }

    [Fact]
    public void ConfigureServices_DoesNotRegister_RecurringJobsApplier()
    {
        // InMemory without legacy Hangfire: bootstrapping Hangfire adds filters to the process-wide GlobalJobFilters,
        // which would leak into the Hangfire conformance tests running in the same process.
        var services = new ServiceCollection();

        new PlatformStartup { Logger = NullLogger.Instance }.ConfigureServices(services, Config("InMemory", "Both"));

        Assert.DoesNotContain(services, x => x.ServiceType == typeof(RecurringJobsApplier));
    }
}
