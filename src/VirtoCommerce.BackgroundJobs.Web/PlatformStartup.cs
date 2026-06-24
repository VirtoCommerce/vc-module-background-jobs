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
using VirtoCommerce.BackgroundJobs.Web.Infrastructure.HealthChecks;
using VirtoCommerce.BackgroundJobs.Core.Recurring;
using VirtoCommerce.BackgroundJobs.Core.Services;
using VirtoCommerce.BackgroundJobs.Data.MapReduce;
using VirtoCommerce.BackgroundJobs.Data.Recurring;
using VirtoCommerce.BackgroundJobs.Hangfire;
using VirtoCommerce.BackgroundJobs.RabbitMQ.Extensions;
using VirtoCommerce.Platform.Core.Common;
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

        if (IsHangfire(options) && processes)
        {
            // The Hangfire background-job server processes jobs on this instance (Mode = Worker/Both).
            // Back-compat: the legacy VirtoCommerce:Hangfire:UseHangfireServer flag can still disable it.
            if (!config.GetValue("VirtoCommerce:Hangfire:UseHangfireServer", true))
            {
                Logger.LogInformation("Background jobs: Hangfire server disabled by VirtoCommerce:Hangfire:UseHangfireServer; this instance enqueues only.");
                return;
            }

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
        else if (IsRabbitMq(options) && processes)
        {
            // The in-process RabbitMQ consumer drains the queue and dispatches handlers on this instance
            // (Mode = Worker/Both).
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
        services.AddSingleton<IJobDispatcher, DefaultJobDispatcher>();
        services.AddScoped<IBackgroundJob, JobEngineBackgroundJob>();

        // The recurring applier is engine-agnostic and always registered; it drives whatever IRecurringJobScheduler
        // the active engine (built-in or custom) registers, and warns when none is present.
        services.AddSingleton<RecurringJobsApplier>();
        services.AddSingleton<IHostedService>(sp => sp.GetRequiredService<RecurringJobsApplier>());

        // Map/reduce orchestration (facade + coordinators + batch store) — engine-agnostic, rides the active engine.
        services.AddMapReduce();

        // Select the active engine. Built-in engines register here; an unknown provider is left for a custom engine
        // module to satisfy (it self-activates in its own IPlatformStartup and registers IJobEngine).
        if (IsHangfire(options))
        {
            // Hangfire storage/DI (custom extension moved from the platform) + the legacy
            // VirtoCommerce.Platform.Hangfire.IRecurringJobService used by existing modules.
            services.AddHangfire(config);

            services.AddSingleton<IRecurringJobService, HangfireRecurringJobService>();
            services.AddTransient<HangfireJobExecutor>();
            services.AddSingleton<IJobEngine, HangfireJobEngine>();

            Logger.LogInformation("Background jobs: Hangfire engine registered as the active IJobEngine.");
        }
        else if (IsRabbitMq(options))
        {
            services.AddRabbitMqJobEngine(config);

            Logger.LogInformation("Background jobs: RabbitMQ engine registered as the active IJobEngine.");
        }
        else
        {
            Logger.LogInformation(
                "Background jobs: provider '{Provider}' is not built-in; expecting a custom engine module to register IJobEngine.",
                options.Provider);
        }

        AddRecurringJobScheduler(services, options);

        // Surface background-job readiness on /health: unhealthy when no engine is registered for the configured
        // provider. The check resolves IJobEngine optionally, so it reports the problem instead of failing.
        services.AddHealthChecks()
            .AddCheck<BackgroundJobsHealthCheck>(
                "Background jobs",
                failureStatus: HealthStatus.Unhealthy,
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
        else if (IsRabbitMq(options))
        {
            // RabbitMQ has no native recurring scheduler — reuse the in-process cron scheduler.
            services.AddInProcessRecurringScheduler();

            Logger.LogInformation("Background jobs: in-process recurring scheduler registered.");
        }

        // For a custom (non-built-in) provider, the engine module registers its own IRecurringJobScheduler — or calls
        // AddInProcessRecurringScheduler(). The always-registered RecurringJobsApplier (ConfigureServices) drives it.
    }

    public void Configure(IApplicationBuilder app, IConfiguration config)
    {
        // Intentionally empty: Hangfire storage schema creation + dashboard wiring must run AFTER the platform
        // database is migrated. That happens in Module.PostInitialize (UseHangfire), inside the platform's
        // synchronized critical section.
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
}
