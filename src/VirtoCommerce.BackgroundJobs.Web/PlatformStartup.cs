using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using VirtoCommerce.BackgroundJobs.Core;
using VirtoCommerce.BackgroundJobs.Core.Services;
using VirtoCommerce.BackgroundJobs.Hangfire;
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
    private const string Hangfire = "Hangfire";
    private const string RabbitMq = "RabbitMQ";

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
                var queues = config.GetSection("VirtoCommerce:Hangfire:Queues").Get<List<string>>();
                if (!queues.IsNullOrEmpty())
                {
                    queues.Add(options.DefaultQueue);
                    serverOptions.Queues = queues.Select(x => x.ToLower()).Distinct().ToArray();
                }

                var workerCount = config.GetValue<int?>("VirtoCommerce:Hangfire:WorkerCount", null);
                if (workerCount != null)
                {
                    serverOptions.WorkerCount = workerCount.Value;
                }
            });
        }
    }

    public void ConfigureServices(IServiceCollection services, IConfiguration config)
    {
        var options = GetOptions(config);
        services.Configure<BackgroundJobsOptions>(config.GetSection("VirtoCommerce:BackgroundJobs"));

        Logger.LogInformation(
            "Background jobs: selected provider '{Provider}', mode '{Mode}', default queue '{DefaultQueue}'.",
            IsHangfire(options) ? Hangfire : options.Provider, options.Mode, options.DefaultQueue);

        // Engine-agnostic services (always registered — producers need to enqueue).
        services.AddSingleton<IJobPayloadSerializer, JsonJobPayloadSerializer>();
        services.AddSingleton<IJobDispatcher, DefaultJobDispatcher>();
        services.AddScoped<IBackgroundJob, JobEngineBackgroundJob>();

        // Select the active engine.
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
        else if (options.Provider.Equals(RabbitMq, StringComparison.OrdinalIgnoreCase))
        {
            // Wired in step 3b (the Web project will reference the RabbitMQ engine project).
            throw new NotSupportedException(
                "The 'RabbitMQ' background-job provider is not available yet. Set VirtoCommerce:BackgroundJobs:Provider to 'Hangfire'.");
        }
        else
        {
            throw new NotSupportedException(
                $"Unknown background-job provider '{options.Provider}'. Supported values: 'Hangfire'.");
        }
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
        string.IsNullOrEmpty(options.Provider) || options.Provider.Equals(Hangfire, StringComparison.OrdinalIgnoreCase);
}
