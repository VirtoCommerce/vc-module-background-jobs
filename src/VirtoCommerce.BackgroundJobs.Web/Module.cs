using System;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using VirtoCommerce.BackgroundJobs.Core;
using VirtoCommerce.Platform.Core.Modularity;
using VirtoCommerce.Platform.Core.Security;
using VirtoCommerce.Platform.Core.Settings;
using VirtoCommerce.Platform.Hangfire.Extensions;

namespace VirtoCommerce.BackgroundJobs.Web;

public class Module : IModule, IHasConfiguration
{
    public ManifestModuleInfo ModuleInfo { get; set; }
    public IConfiguration Configuration { get; set; }

    public void Initialize(IServiceCollection serviceCollection)
    {
        // Override models
        //AbstractTypeFactory<OriginalModel>.OverrideType<OriginalModel, ExtendedModel>().MapToType<ExtendedEntity>();
        //AbstractTypeFactory<OriginalEntity>.OverrideType<OriginalEntity, ExtendedEntity>();

        // Register services
        //serviceCollection.AddTransient<IMyService, MyService>();
    }

    public void PostInitialize(IApplicationBuilder appBuilder)
    {
        var serviceProvider = appBuilder.ApplicationServices;

        // Register settings
        var settingsRegistrar = serviceProvider.GetRequiredService<ISettingsRegistrar>();
        settingsRegistrar.RegisterSettings(ModuleConstants.Settings.AllSettings, ModuleInfo.Id);

        // Register permissions
        var permissionsRegistrar = serviceProvider.GetRequiredService<IPermissionsRegistrar>();
        permissionsRegistrar.RegisterPermissions(ModuleInfo.Id, "BackgroundJobs", ModuleConstants.Security.Permissions.AllPermissions);

        // Initialize the Hangfire engine: create the storage schema, wire the dashboard, register job filters,
        // the recurring-job setting watcher and the developer tool. This must run AFTER the platform database is
        // migrated — PostInitialize executes inside the platform's synchronized critical section, after platform
        // migrations, which is exactly where the platform used to call UseHangfire.
        // Only when Hangfire is the active provider: with another provider (e.g. RabbitMQ) the Hangfire services
        // are never registered, so UseHangfire would fail resolving Hangfire.IGlobalConfiguration.
        if (IsHangfireProvider())
        {
            appBuilder.UseHangfire(Configuration);
        }
    }

    public void Uninstall()
    {
        // Nothing to do here
    }

    private bool IsHangfireProvider()
    {
        var provider = Configuration.GetValue<string>("VirtoCommerce:BackgroundJobs:Provider");
        return string.IsNullOrEmpty(provider) || provider.Equals("Hangfire", StringComparison.OrdinalIgnoreCase);
    }
}
