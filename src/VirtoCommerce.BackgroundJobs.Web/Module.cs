using System;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using VirtoCommerce.BackgroundJobs.Core;
using VirtoCommerce.BackgroundJobs.Data.Recurring;
using VirtoCommerce.BackgroundJobs.RabbitMQ;
using VirtoCommerce.Platform.Core;
using VirtoCommerce.Platform.Core.DeveloperTools;
using VirtoCommerce.Platform.Core.Events;
using VirtoCommerce.Platform.Core.Modularity;
using VirtoCommerce.Platform.Core.Security;
using VirtoCommerce.Platform.Core.Settings;
using VirtoCommerce.Platform.Core.Settings.Events;
using VirtoCommerce.Platform.Hangfire.Extensions;

namespace VirtoCommerce.BackgroundJobs.Web;

public class Module : IModule, IHasConfiguration
{
    public ManifestModuleInfo ModuleInfo { get; set; }
    public IConfiguration Configuration { get; set; }

    public void Initialize(IServiceCollection serviceCollection)
    {
    }

    public void PostInitialize(IApplicationBuilder appBuilder)
    {
        var serviceProvider = appBuilder.ApplicationServices;

        // No platform settings: all background-job configuration (provider, mode, default queue, retries) is a
        // deployment-time concern read from appsettings.json (VirtoCommerce:BackgroundJobs) at startup.

        // Register permissions
        var permissionsRegistrar = serviceProvider.GetRequiredService<IPermissionsRegistrar>();
        permissionsRegistrar.RegisterPermissions(ModuleInfo.Id, "BackgroundJobs", ModuleConstants.Security.Permissions.AllPermissions);

        // Initialize Hangfire: create the storage schema, wire the /hangfire dashboard, register job filters, the
        // recurring-job setting watcher and the developer tool. Must run AFTER the platform database is migrated —
        // PostInitialize executes inside the platform's synchronized critical section, after platform migrations.
        // Runs when Hangfire is the active provider OR legacy Hangfire is enabled (default), so modules that use the
        // Hangfire API directly keep working even when another engine (e.g. RabbitMQ) is active. The Hangfire
        // infrastructure it depends on (IGlobalConfiguration etc.) is registered in PlatformStartup under the same
        // condition.
        var isHangfireProvider = Configuration.IsBackgroundJobsProvider(BackgroundJobsProviders.Hangfire);
        var enableLegacyHangfire = Configuration.GetValue(BackgroundJobsConfigurationExtensions.EnableLegacyHangfireKey, true);

        if (isHangfireProvider || enableLegacyHangfire)
        {
            appBuilder.UseHangfire(Configuration);
        }

        // RabbitMQ has no in-platform dashboard; surface its management UI as an external developer tool.
        if (IsRabbitMqProvider())
        {
            RegisterRabbitMqDeveloperTool(serviceProvider);
        }

        // Validate that an engine matching the configured provider is actually active (catches "Provider=X but
        // nothing registered an IJobEngine for X" — e.g. a custom engine module is missing or didn't self-activate).
        ValidateActiveEngine(serviceProvider);

        // Re-apply setting-driven recurring jobs live when their enabler/cron settings change. The in-process bus
        // only dispatches to handlers registered via RegisterEventHandler (not via DI IEventHandler<> resolution),
        // so the applier must be registered here.
        appBuilder.RegisterEventHandler<ObjectSettingChangedEvent, RecurringJobsApplier>();
    }

    public void Uninstall()
    {
        // Nothing to do here
    }

    private void ValidateActiveEngine(IServiceProvider serviceProvider)
    {
        var logger = serviceProvider.GetService<ILoggerFactory>()?.CreateLogger<Module>();
        var configuredProvider = Configuration.GetBackgroundJobsProvider();

        var engine = serviceProvider.GetService<IJobEngine>();
        switch (BackgroundJobsEngineStatusEvaluator.Evaluate(engine, configuredProvider))
        {
            case BackgroundJobsEngineStatus.NoEngine:
                logger?.LogWarning(
                    "Background jobs: provider '{Provider}' is configured but no IJobEngine is registered. " +
                    "Enqueue will fail until an engine module for this provider is installed and self-activates.",
                    configuredProvider);
                break;
            case BackgroundJobsEngineStatus.ProviderMismatch:
                logger?.LogWarning(
                    "Background jobs: configured provider '{Provider}' does not match the active engine '{EngineProvider}'.",
                    configuredProvider, engine!.ProviderName);
                break;
            default:
                logger?.LogInformation("Background jobs: active engine '{EngineProvider}' is ready.", engine!.ProviderName);
                break;
        }
    }

    private bool IsRabbitMqProvider() => Configuration.IsBackgroundJobsProvider(BackgroundJobsProviders.RabbitMq);

    private static void RegisterRabbitMqDeveloperTool(IServiceProvider serviceProvider)
    {
        var options = serviceProvider.GetRequiredService<IOptions<RabbitMqOptions>>().Value;
        var url = string.IsNullOrEmpty(options.ManagementUri)
            ? $"http://{options.HostName}:15672"
            : options.ManagementUri;

        var registrar = serviceProvider.GetRequiredService<IDeveloperToolRegistrar>();
        registrar.RegisterDeveloperTool(new DeveloperToolDescriptor
        {
            Name = "RabbitMQ Management",
            Url = url,
            IsExternal = true,
            SortOrder = 20,
            Permission = PlatformConstants.Security.Permissions.BackgroundJobsManage,
        });
    }
}
