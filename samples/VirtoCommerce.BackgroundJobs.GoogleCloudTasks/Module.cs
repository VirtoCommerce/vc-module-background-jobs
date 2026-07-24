using System;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using VirtoCommerce.BackgroundJobs.Core;
using VirtoCommerce.Platform.Core;
using VirtoCommerce.Platform.Core.DeveloperTools;
using VirtoCommerce.Platform.Core.Modularity;

namespace VirtoCommerce.BackgroundJobs.GoogleCloudTasks;

public class Module : IModule, IHasConfiguration
{
    public ManifestModuleInfo ModuleInfo { get; set; } = null!;
    public IConfiguration Configuration { get; set; } = null!;

    public void Initialize(IServiceCollection serviceCollection)
    {
        // Engine wiring lives in PlatformStartup (it needs IConfiguration to self-activate on the provider name).
    }

    public void PostInitialize(IApplicationBuilder appBuilder)
    {
        if (!Configuration.IsBackgroundJobsProvider(GoogleCloudTasksConstants.ProviderName))
        {
            return;
        }

        // Surface the Cloud Tasks queue in the GCP console as an external developer tool.
        RegisterConsoleDeveloperTool(appBuilder.ApplicationServices);
    }

    public void Uninstall()
    {
        // Nothing to do here.
    }

    private static void RegisterConsoleDeveloperTool(IServiceProvider serviceProvider)
    {
        var options = serviceProvider.GetRequiredService<IOptions<GoogleCloudTasksOptions>>().Value;
        var url = string.IsNullOrEmpty(options.ConsoleUri)
            ? $"https://console.cloud.google.com/cloudtasks/queue/{options.LocationId}/{options.QueueId}/tasks?project={options.ProjectId}"
            : options.ConsoleUri;

        var registrar = serviceProvider.GetRequiredService<IDeveloperToolRegistrar>();
        registrar.RegisterDeveloperTool(new DeveloperToolDescriptor
        {
            Name = "Google Cloud Tasks",
            Url = url,
            IsExternal = true,
            SortOrder = 30,
            Permission = PlatformConstants.Security.Permissions.BackgroundJobsManage,
        });
    }
}
