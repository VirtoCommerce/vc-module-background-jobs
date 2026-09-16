using Hangfire;
using Hangfire.MemoryStorage;
using Hangfire.MySql;
using Hangfire.PostgreSql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using VirtoCommerce.BackgroundJobs.Core;

namespace VirtoCommerce.Platform.Hangfire.Extensions
{
    public static class ServiceCollectionExtensions
    {
        public static IGlobalConfiguration AddHangfireStorage(this IGlobalConfiguration globalConfiguration, IConfiguration configuration, HangfireOptions hangfireOptions)
        {
            var databaseProvider = configuration.GetValue("DatabaseProvider", "SqlServer");
            var connectionString = configuration.GetConnectionString("VirtoCommerce.Hangfire") ?? configuration.GetConnectionString("VirtoCommerce");

            // Prevent Hangfire to apply migrations (prepare schema) here because database may not exist yet.
            // Migrations will be forced to apply at ApplicationBuilderExtensions.UseHangfire
            switch (databaseProvider)
            {
                case "PostgreSql":
                    globalConfiguration.UsePostgreSqlStorage(
                        configure =>
                        {
                            configure.UseNpgsqlConnection(connectionString);
                        },
                        hangfireOptions.PostgreSqlStorageOptions);
                    break;
                case "MySql":
                    globalConfiguration.UseStorage(new MySqlStorage(connectionString, hangfireOptions.MySqlStorageOptions));
                    break;
                default:
                    globalConfiguration.UseSqlServerStorage(connectionString, hangfireOptions.SqlServerStorageOptions);
                    break;
            }

            return globalConfiguration;
        }

        public static IServiceCollection AddHangfire(this IServiceCollection services, IConfiguration configuration)
        {
            services.AddSingleton<RecurringJobService>();
            services.AddSingleton<IRecurringJobService>(sp => sp.GetRequiredService<RecurringJobService>());

            var section = configuration.GetSection("VirtoCommerce:Hangfire");
            var hangfireOptions = new HangfireOptions();
            section.Bind(hangfireOptions);
            services.AddOptions<HangfireOptions>().Bind(section).ValidateDataAnnotations();

            hangfireOptions.SqlServerStorageOptions.PrepareSchemaIfNecessary = false;
            hangfireOptions.PostgreSqlStorageOptions.PrepareSchemaIfNecessary = false;
            hangfireOptions.MySqlStorageOptions.PrepareSchemaIfNecessary = false;

            // Retry count: prefer the engine-agnostic VirtoCommerce:BackgroundJobs:MaxRetryAttempts when it is
            // explicitly configured (unifies Hangfire with the RabbitMQ engine); otherwise fall back to the legacy
            // Hangfire-only AutomaticRetryCount, so existing deployments that tuned it keep working and the default
            // stays 1 rather than silently becoming 3.
            var backgroundJobsOptions = new BackgroundJobsOptions();
            configuration.GetSection("VirtoCommerce:BackgroundJobs").Bind(backgroundJobsOptions);
            var maxRetryConfigured = configuration["VirtoCommerce:BackgroundJobs:MaxRetryAttempts"] is not null;
            var retryAttempts = maxRetryConfigured ? backgroundJobsOptions.MaxRetryAttempts : hangfireOptions.AutomaticRetryCount;
            GlobalJobFilters.Filters.Add(new AutomaticRetryAttribute { Attempts = retryAttempts });

            // Lets a single enqueue opt out of (or tighten) the global retry count via EnqueueOptions.MaxRetryAttempts.
            // Must be registered after the filter above — it overrides the reschedule that one elects.
            GlobalJobFilters.Filters.Add(new BackgroundJobs.Hangfire.PerJobRetryFilterAttribute());

            if (hangfireOptions.JobStorageType == HangfireJobStorageType.SqlServer ||
                hangfireOptions.JobStorageType == HangfireJobStorageType.Database)
            {
                services.AddHangfire(c => c.SetDataCompatibilityLevel(CompatibilityLevel.Version_180)
                    .UseSimpleAssemblyNameTypeSerializer()
                    .UseRecommendedSerializerSettings()
                    .AddHangfireStorage(configuration, hangfireOptions));
            }
            else
            {
                services.AddHangfire(config => config.UseMemoryStorage());
            }

            return services;
        }
    }
}
