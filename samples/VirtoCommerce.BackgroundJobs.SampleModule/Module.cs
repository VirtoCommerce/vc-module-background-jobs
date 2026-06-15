using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using VirtoCommerce.BackgroundJobs.SampleModule.Jobs;
using VirtoCommerce.Platform.Core.Common;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.Modularity;

namespace VirtoCommerce.BackgroundJobs.SampleModule;

public class Module : IModule
{
    public ManifestModuleInfo ModuleInfo { get; set; } = null!;

    public void Initialize(IServiceCollection serviceCollection)
    {
        // Make the payload overridable by partner modules (AbstractTypeFactory).
        AbstractTypeFactory<SampleJobPayload>.RegisterType<SampleJobPayload>();

        // Register the handler so the active engine's dispatcher can resolve and run it.
        serviceCollection.AddBackgroundJob<SampleJobPayload, SampleJob>();
    }

    public void PostInitialize(IApplicationBuilder appBuilder)
    {
    }

    public void Uninstall()
    {
    }
}
