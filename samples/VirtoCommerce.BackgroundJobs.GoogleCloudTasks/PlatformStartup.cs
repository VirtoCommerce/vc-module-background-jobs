using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using VirtoCommerce.BackgroundJobs.Core;
using VirtoCommerce.BackgroundJobs.Data.Recurring;
using VirtoCommerce.BackgroundJobs.GoogleCloudTasks.Extensions;
using VirtoCommerce.Platform.Core.Modularity;

namespace VirtoCommerce.BackgroundJobs.GoogleCloudTasks;

/// <summary>
/// Self-activating engine wiring. Registers the Google Cloud Tasks <see cref="IJobEngine"/> only when this provider
/// is selected; otherwise it is inert, so the module can be installed alongside the built-in engines without
/// affecting them. Implements <see cref="IHasLogger"/> so the platform injects a logger during startup discovery.
/// </summary>
public class PlatformStartup : IPlatformStartup, IHasLogger
{
    public ILogger Logger { get; set; } = null!;

    public void ConfigureAppConfiguration(IConfigurationBuilder builder, IHostEnvironment env)
    {
        // No app configuration sources to add.
    }

    public void ConfigureHostServices(IServiceCollection services, IConfiguration config)
    {
        // Push engine: there is no in-process consumer to start. The push callback controller is the processing
        // host and is auto-discovered on every web instance, so nothing is Mode-gated here. (A Producer-only
        // instance simply never receives callbacks because Cloud Tasks targets the configured CallbackBaseUrl.)
    }

    public void ConfigureServices(IServiceCollection services, IConfiguration config)
    {
        if (!config.IsBackgroundJobsProvider(GoogleCloudTasksConstants.ProviderName))
        {
            return;
        }

        Logger.LogInformation("Background jobs: activating the Google Cloud Tasks engine.");

        services.AddGoogleCloudTasksJobEngine(config);

        // Cloud Tasks has no native recurring scheduler — reuse the engine-agnostic in-process cron scheduler
        // (a cron tick enqueues a Cloud Task per occurrence, fleet-safe via the shared occurrence marker).
        services.AddInProcessRecurringScheduler();
    }

    public void Configure(IApplicationBuilder app, IConfiguration config)
    {
        // Nothing to configure: the callback controller is mapped by the platform's MVC pipeline.
    }
}
