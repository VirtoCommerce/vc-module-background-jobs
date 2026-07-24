using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using VirtoCommerce.BackgroundJobs.Core;
using VirtoCommerce.BackgroundJobs.Core.Recurring;
using VirtoCommerce.BackgroundJobs;
using VirtoCommerce.BackgroundJobs.Data.MapReduce;
using VirtoCommerce.BackgroundJobs.Data.Recurring;
using VirtoCommerce.BackgroundJobs.Data.Services;
using VirtoCommerce.BackgroundJobs.Hangfire;
using VirtoCommerce.BackgroundJobs.InMemory.Extensions;
using VirtoCommerce.BackgroundJobs.RabbitMQ.Extensions;
using VirtoCommerce.BackgroundJobs.Web.Infrastructure.HealthChecks;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.Modularity;
using VirtoCommerce.Platform.Hangfire.Extensions;

namespace VirtoCommerce.BackgroundJobs.Web;

/// <summary>
/// Owns background-job engine wiring. Discovered by the platform via the &lt;startupType&gt; element in
/// module.manifest. Selects exactly one engine per instance by <c>VirtoCommerce:BackgroundJobs:Provider</c> and
/// runs the processing host (Hangfire server / RabbitMQ consumer) only when <c>Mode</c> is Worker/Both.
/// <para>
/// Hangfire storage schema creation + dashboard middleware run in <see cref="Module.PostInitialize"/>, after the
/// platform database migration inside the platform's synchronized critical section.
/// </para>
/// </summary>
public class PlatformStartup : IPlatformStartup, IHasLogger
{
    /// <summary>
    /// Logger assigned by the platform during startup discovery (the platform injects it because this class
    /// implements <see cref="IHasLogger"/>).
    /// </summary>
    public ILogger Logger { get; set; }

    public void ConfigureAppConfiguration(IConfigurationBuilder builder, IHostEnvironment env)
    {
        // No app configuration sources to add.
    }

    public void ConfigureHostServices(IServiceCollection services, IConfiguration config)
    {
        var options = GetOptions(config);
        var processes = options.Mode != BackgroundJobsMode.Producer;
        if (!processes)
        {
            // Producer-only instance: enqueues but runs no processing host (no Hangfire server, no RabbitMQ consumer).
            return;
        }

        // Hangfire processing server — runs when Hangfire is the active engine OR legacy Hangfire is enabled, so
        // legacy modules' Hangfire jobs are processed alongside the active engine (e.g. RabbitMQ). The legacy
        // VirtoCommerce:Hangfire:UseHangfireServer flag can still disable just this server.
        if (IsHangfire(options) || options.EnableLegacyHangfire)
        {
            if (config.GetValue("VirtoCommerce:Hangfire:UseHangfireServer", true))
            {
                Logger.LogInformation("Background jobs: starting Hangfire processing server (Mode = {Mode}).", options.Mode);

                services.AddHangfireServer(serverOptions =>
                {
                    // Always include the agnostic default queue so a non-"default" DefaultQueue still has a worker
                    // (Hangfire's built-in list is just ["default"]). Lowercased to match how jobs are enqueued.
                    var queues = config.GetSection("VirtoCommerce:Hangfire:Queues").Get<List<string>>() ?? [];
                    queues.Add(options.DefaultQueue);
                    serverOptions.Queues = queues
                        .Where(x => !string.IsNullOrWhiteSpace(x))
                        .Select(x => x.ToLowerInvariant())
                        .Distinct()
                        .ToArray();

                    var workerCount = config.GetValue<int?>("VirtoCommerce:Hangfire:WorkerCount", null);
                    if (workerCount != null)
                    {
                        serverOptions.WorkerCount = workerCount.Value;
                    }
                });
            }
            else
            {
                Logger.LogInformation("Background jobs: Hangfire server disabled by VirtoCommerce:Hangfire:UseHangfireServer.");
            }
        }

        // RabbitMQ in-process consumer — runs when RabbitMQ is the active engine (drains the queue and dispatches
        // handlers on this instance). Coexists with the Hangfire server above under legacy mode; they process
        // disjoint stores (RabbitMQ queue vs Hangfire SQL), so there is no conflict.
        if (IsRabbitMq(options))
        {
            Logger.LogInformation("Background jobs: starting RabbitMQ in-process consumer (Mode = {Mode}).", options.Mode);
            services.AddRabbitMqJobConsumer();
        }
    }

    public void ConfigureServices(IServiceCollection services, IConfiguration config)
    {
        var options = GetOptions(config);
        services.Configure<BackgroundJobsOptions>(config.GetSection("VirtoCommerce:BackgroundJobs"));

        Logger.LogInformation(
            "Background jobs: selected provider '{Provider}', mode '{Mode}', default queue '{DefaultQueue}'.",
            IsHangfire(options) ? BackgroundJobsProviders.Hangfire : options.Provider, options.Mode, options.DefaultQueue);

        // Engine-agnostic services (always registered — producers need to enqueue; the facade's IJobEngine is an
        // optional dependency, so it resolves even with no engine and throws an actionable error on use).
        services.AddSingleton<IJobPayloadSerializer, JsonJobPayloadSerializer>();
        // Engine-agnostic job telemetry. Its TelemetryClient dependency is optional, so this is inert unless the
        // platform's Application Insights module is installed (then jobs/* metrics + JobCompleted events flow to AI).
        services.AddSingleton<JobTelemetry>();
        services.AddSingleton<IJobDispatcher, DefaultJobDispatcher>();
        // Shared "run a pushed envelope in-process" path (build context + dispatch), reused by push engines (GCT).
        services.AddSingleton<IJobEnvelopeRunner, JobEnvelopeRunner>();
        // One facade instance behind both the single-job and bulk producer contracts.
        services.AddScoped<JobEngineBackgroundJob>();
        services.AddScoped<IBackgroundJob>(sp => sp.GetRequiredService<JobEngineBackgroundJob>());
        services.AddScoped<IBulkBackgroundJob>(sp => sp.GetRequiredService<JobEngineBackgroundJob>());

        // The recurring applier is engine-agnostic and always registered; it drives whatever IRecurringJobScheduler
        // the active engine (built-in or custom) registers, and warns when none is present.
        services.AddSingleton<RecurringJobsApplier>();
        services.AddSingleton<IHostedService>(sp => sp.GetRequiredService<RecurringJobsApplier>());

        // Map/reduce orchestration (facade + coordinators + batch store) — engine-agnostic, rides the active engine.
        services.AddMapReduce();

        // Admin read model for the troubleshooting API (list registered handlers + recurring schedules). Engine-agnostic.
        services.AddScoped<Core.Admin.IBackgroundJobsAdminQuery, Data.Admin.BackgroundJobsAdminQuery>();

        // Bootstrap Hangfire INFRASTRUCTURE (storage/DI, IBackgroundJobClient/IRecurringJobManager, and the legacy
        // VirtoCommerce.Platform.Hangfire.IRecurringJobService + executor) whenever Hangfire is the active engine OR
        // legacy Hangfire is enabled — so modules that call the Hangfire API directly keep working even when another
        // engine (e.g. RabbitMQ) is the active IJobEngine. The IJobEngine binding is chosen separately, below.
        var bootstrapHangfire = IsHangfire(options) || options.EnableLegacyHangfire;
        if (bootstrapHangfire)
        {
            services.AddHangfire(config);
            services.AddSingleton<IRecurringJobService, HangfireRecurringJobService>();
            services.AddTransient<HangfireJobExecutor>();
        }

        // Select the ACTIVE engine (what IBackgroundJob / map-reduce / the agnostic recurring applier route to).
        // Built-in engines register here; an unknown provider is left for a custom engine module to satisfy
        // (it self-activates in its own IPlatformStartup and registers IJobEngine).
        if (IsHangfire(options))
        {
            services.AddSingleton<IJobEngine, HangfireJobEngine>();

            Logger.LogInformation("Background jobs: Hangfire engine registered as the active IJobEngine.");
        }
        else if (IsRabbitMq(options))
        {
            services.AddRabbitMqJobEngine(config);

            Logger.LogInformation(
                "Background jobs: RabbitMQ engine registered as the active IJobEngine{Legacy}.",
                options.EnableLegacyHangfire ? " (Hangfire also bootstrapped for legacy modules)" : string.Empty);
        }
        else if (IsInMemory(options))
        {
            services.AddInMemoryJobEngine();

            Logger.LogInformation("Background jobs: in-memory engine registered as the active IJobEngine (local/testing; non-durable, single-process).");
        }
        else
        {
            Logger.LogInformation(
                "Background jobs: provider '{Provider}' is not built-in; expecting a custom engine module to register IJobEngine{Legacy}.",
                options.Provider, options.EnableLegacyHangfire ? " (Hangfire also bootstrapped for legacy modules)" : string.Empty);
        }

        AddRecurringJobScheduler(services, options);

        // Surface background-job readiness on /health: unhealthy when no engine is registered for the configured
        // provider. The check resolves IJobEngine optionally, so it reports the problem instead of failing.
        services.AddHealthChecks()
            .AddCheck<BackgroundJobsHealthCheck>(
                "Background jobs",
                failureStatus: HealthStatus.Unhealthy,
                tags: ["BackgroundJobs"])
            // Degraded (not failed) when a queue-backed engine runs on the in-memory store — safe single-instance,
            // unsafe multi-instance; surfaces the risk without breaking a single-instance deployment's /health.
            .AddCheck<SharedStoreHealthCheck>(
                "Background jobs store",
                failureStatus: HealthStatus.Degraded,
                tags: ["BackgroundJobs"]);
    }

    /// <summary>
    /// Registers the engine's <see cref="IRecurringJobScheduler"/> implementation that the platform's applier drives:
    /// Hangfire uses its native recurring manager; other engines use the in-process cron scheduler backed by a shared
    /// occurrence marker store (Redis when configured, in-memory otherwise). Settings/enable evaluation and applying
    /// the declared recurring jobs are owned by the platform's RecurringJobsApplier.
    /// </summary>
    private void AddRecurringJobScheduler(IServiceCollection services, BackgroundJobsOptions options)
    {
        if (IsHangfire(options))
        {
            // Hangfire schedules recurring jobs natively (persisted, shown in the dashboard).
            services.AddTransient<IRecurringJobInvoker, RecurringJobInvoker>();
            services.AddSingleton<IRecurringJobScheduler, HangfireRecurringJobScheduler>();

            Logger.LogInformation("Background jobs: Hangfire-native recurring scheduler registered.");
        }
        else if (IsRabbitMq(options) || IsInMemory(options))
        {
            // RabbitMQ / in-memory have no native recurring scheduler — reuse the in-process cron scheduler.
            services.AddInProcessRecurringScheduler();

            Logger.LogInformation("Background jobs: in-process recurring scheduler registered.");
        }

        // For a custom (non-built-in) provider, the engine module registers its own IRecurringJobScheduler — or calls
        // AddInProcessRecurringScheduler(). The always-registered RecurringJobsApplier (ConfigureServices) drives it.
    }

    public void Configure(IApplicationBuilder app, IConfiguration config)
    {
        Platform.Core.Jobs.BackgroundJob.Initialize(app.ApplicationServices);
    }

    private static BackgroundJobsOptions GetOptions(IConfiguration config)
    {
        var options = new BackgroundJobsOptions();
        config.GetSection("VirtoCommerce:BackgroundJobs").Bind(options);
        return options;
    }

    private static bool IsHangfire(BackgroundJobsOptions options) =>
        string.IsNullOrEmpty(options.Provider) || options.Provider.Equals(BackgroundJobsProviders.Hangfire, StringComparison.OrdinalIgnoreCase);

    private static bool IsRabbitMq(BackgroundJobsOptions options) =>
        !string.IsNullOrEmpty(options.Provider) && options.Provider.Equals(BackgroundJobsProviders.RabbitMq, StringComparison.OrdinalIgnoreCase);

    private static bool IsInMemory(BackgroundJobsOptions options) =>
        !string.IsNullOrEmpty(options.Provider) && options.Provider.Equals(BackgroundJobsProviders.InMemory, StringComparison.OrdinalIgnoreCase);
}
