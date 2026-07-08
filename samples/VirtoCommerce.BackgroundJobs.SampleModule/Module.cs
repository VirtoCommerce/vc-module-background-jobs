using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using VirtoCommerce.BackgroundJobs.SampleModule.Jobs;
using VirtoCommerce.BackgroundJobs.SampleModule.Jobs.Indexing;
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

        // Register the handler so the active engine's dispatcher can resolve and run it. The payload type is inferred
        // from the handler's IBackgroundJobHandler<TPayload> interface (or use AddBackgroundJob<TPayload, THandler>()).
        serviceCollection.AddBackgroundJob<SampleJob>();

        // A recurring job is just a handler + a schedule. The active engine (Hangfire/RabbitMQ) fires it on cron
        // and runs the handler on a worker — identical code on either engine. The factory overload passes a
        // configured payload (here a label); it runs once per occurrence.
        serviceCollection.AddRecurringJob<SampleRecurringJobPayload, SampleRecurringJob>(
            () => new SampleRecurringJobPayload { Label = "heartbeat" },
            schedule => schedule
                .WithId("BackgroundJobs.Sample.Heartbeat")
                .WithCron("*/5 * * * *"));

        // Map/reduce: register the map + reduce handlers for one batch type. Fan out indexing over pages of ids that
        // run in parallel across workers, then aggregate once. See README "Map/reduce: parallel product indexing".
        serviceCollection.AddMapReduceJob<IndexPageHandler, IndexSummaryReducer>();
    }

    public void PostInitialize(IApplicationBuilder appBuilder)
    {
    }

    public void Uninstall()
    {
    }
}
