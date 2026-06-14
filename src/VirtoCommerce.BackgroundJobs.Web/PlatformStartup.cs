using System.Collections.Generic;
using System.Linq;
using Hangfire;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using VirtoCommerce.BackgroundJobs.Hangfire;
using VirtoCommerce.Platform.Core.Common;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.Modularity;
using VirtoCommerce.Platform.Hangfire.Extensions;

namespace VirtoCommerce.BackgroundJobs.Web;

/// <summary>
/// Owns the Hangfire engine wiring that previously lived in the platform's Program.cs / Startup.cs.
/// Discovered by the platform via the &lt;startupType&gt; element in module.manifest.
/// <para>
/// The actual Hangfire storage schema creation + dashboard middleware run in <see cref="Module.PostInitialize"/>,
/// which executes after the platform database migration inside the platform's synchronized critical section.
/// </para>
/// </summary>
public class PlatformStartup : IPlatformStartup
{
    public void ConfigureAppConfiguration(IConfigurationBuilder builder, IHostEnvironment env)
    {
        // No app configuration sources to add.
    }

    public void ConfigureHostServices(IServiceCollection services, IConfiguration config)
    {
        // Conditionally run the Hangfire background-job server on this instance, so processing can be disabled
        // for web-only/scale-out nodes. (Moved verbatim from the platform's Program.cs.)
        if (config.GetValue("VirtoCommerce:Hangfire:UseHangfireServer", true))
        {
            services.AddHangfireServer(options =>
            {
                var queues = config.GetSection("VirtoCommerce:Hangfire:Queues").Get<List<string>>();
                if (!queues.IsNullOrEmpty())
                {
                    queues.Add("default");
                    options.Queues = queues.Select(x => x.ToLower()).Distinct().ToArray();
                }

                var workerCount = config.GetValue<int?>("VirtoCommerce:Hangfire:WorkerCount", null);
                if (workerCount != null)
                {
                    options.WorkerCount = workerCount.Value;
                }
            });
        }
    }

    public void ConfigureServices(IServiceCollection services, IConfiguration config)
    {
        // Register Hangfire storage/DI (the custom extension moved from the platform) plus the legacy
        // VirtoCommerce.Platform.Hangfire.IRecurringJobService used by existing modules.
        services.AddHangfire(config);

        // Register the platform-facing, engine-agnostic facades backed by Hangfire. These override the
        // platform's NoEngine* fallbacks (which are registered via TryAdd after module initialization).
        services.AddSingleton<IBackgroundJobProcessor, HangfireBackgroundJobProcessor>();
        services.AddSingleton<IRecurringJobService, HangfireRecurringJobService>();
    }

    public void Configure(IApplicationBuilder app, IConfiguration config)
    {
        // Intentionally empty: Hangfire storage schema creation + dashboard wiring must run AFTER the platform
        // database is migrated. That happens in Module.PostInitialize (UseHangfire), inside the platform's
        // synchronized critical section.
    }
}
