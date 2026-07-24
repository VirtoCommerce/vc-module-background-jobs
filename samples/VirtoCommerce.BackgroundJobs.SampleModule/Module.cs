using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using VirtoCommerce.BackgroundJobs.SampleModule.Jobs;
using VirtoCommerce.BackgroundJobs.SampleModule.Jobs.Benchmark;
using VirtoCommerce.BackgroundJobs.SampleModule.Jobs.Indexing;
using VirtoCommerce.Platform.Core.Common;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.Modularity;
using VirtoCommerce.Platform.Core.Settings;

namespace VirtoCommerce.BackgroundJobs.SampleModule;

public class Module : IModule
{
    public ManifestModuleInfo ModuleInfo { get; set; } = null!;

    public void Initialize(IServiceCollection serviceCollection)
    {
        // Make the payload overridable by partner modules (AbstractTypeFactory).
        AbstractTypeFactory<SampleJobPayload>.RegisterType<SampleJobPayload>();

        // Register the handler so the active engine's dispatcher can resolve and run it. The payload type is inferred
        // from the handler's IBackgroundJobHandler<TPayload> interface (or use AddBackgroundJob<THandler, TPayload>()).
        serviceCollection.AddBackgroundJob<SampleJob>();

        // A recurring job is just a handler + a schedule. The active engine (Hangfire/RabbitMQ) fires it on cron
        // and runs the handler on a worker — identical code on either engine. The factory overload passes a
        // configured payload (here a label); it runs once per occurrence. The schedule is setting-driven via
        // FromSettings — the enabler + cron settings are editable in the admin UI (the cron uses the Cron value type,
        // so it is validated and shows a plain-English description) and applied live when either setting changes.
        serviceCollection.AddRecurringJob<SampleRecurringJob, SampleRecurringJobPayload>(
            () => new SampleRecurringJobPayload { Label = "heartbeat" },
            schedule => schedule
                .WithId("BackgroundJobs.Sample.Heartbeat")
                .FromSettings(
                    ModuleConstants.Settings.General.EnableHeartbeat,
                    ModuleConstants.Settings.General.HeartbeatCron));

        // Map/reduce: register the map + reduce handlers for one batch type. Fan out indexing over pages of ids that
        // run in parallel across workers, then aggregate once. See README "Map/reduce: parallel product indexing".
        serviceCollection.AddMapReduceJob<IndexPageHandler, IndexSummaryReducer>();

        // Benchmark workloads (tunable, async, idempotent) used to compare engines under load — driven by the
        // BenchmarkController load endpoints and the NBomber scenarios. See docs/benchmark-plan.md.
        AbstractTypeFactory<BenchmarkPayload>.RegisterType<BenchmarkPayload>();
        serviceCollection.AddSingleton<BenchmarkRunRegistry>();
        serviceCollection.AddBackgroundJob<BenchmarkJob>();
        serviceCollection.AddMapReduceJob<BenchmarkMapHandler, BenchmarkReducer>();
    }

    public void PostInitialize(IApplicationBuilder appBuilder)
    {
        // Register the module's settings so they appear in the admin UI (grouped by GroupName) and resolve at runtime —
        // including the Cron-typed heartbeat schedule the recurring job reads via FromSettings.
        var settingsRegistrar = appBuilder.ApplicationServices.GetRequiredService<ISettingsRegistrar>();
        settingsRegistrar.RegisterSettings(ModuleConstants.Settings.AllSettings, ModuleInfo.Id);
    }

    public void Uninstall()
    {
    }
}
