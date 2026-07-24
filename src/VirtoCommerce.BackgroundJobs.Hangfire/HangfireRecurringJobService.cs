using System;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Hangfire;
using VirtoCommerce.Platform.Core.Settings;
using LegacyRecurringJobService = VirtoCommerce.Platform.Hangfire.IRecurringJobService;
using SettingCronJobBuilder = VirtoCommerce.Platform.Hangfire.SettingCronJobBuilder;
using CoreRecurringJobService = VirtoCommerce.Platform.Core.Jobs.IRecurringJobService;

namespace VirtoCommerce.BackgroundJobs.Hangfire
{
    /// <summary>
    /// Hangfire implementation of the platform's engine-agnostic <see cref="CoreRecurringJobService"/> facade.
    /// Delegates setting-driven recurring jobs to the legacy <see cref="LegacyRecurringJobService"/> (so behaviour
    /// matches the previous platform implementation) and explicit cron jobs to Hangfire's <see cref="RecurringJob"/>.
    /// </summary>
    public class HangfireRecurringJobService : CoreRecurringJobService
    {
        private readonly LegacyRecurringJobService _settingDrivenJobs;

        public HangfireRecurringJobService(LegacyRecurringJobService settingDrivenJobs)
        {
            _settingDrivenJobs = settingDrivenJobs;
        }

        public void WatchJobSetting<T>(
            SettingDescriptor enablerSetting,
            SettingDescriptor cronSetting,
            Expression<Func<T, Task>> methodCall,
            string jobId = null,
            TimeZoneInfo timeZone = null,
            string queue = null)
        {
            var builder = new SettingCronJobBuilder()
                .SetEnablerSetting(enablerSetting)
                .SetCronSetting(cronSetting);

            if (!string.IsNullOrEmpty(jobId))
            {
                builder.SetJobId(jobId);
            }

            if (timeZone != null)
            {
                builder.SetTimeZoneInfo(timeZone);
            }

            if (!string.IsNullOrEmpty(queue))
            {
                builder.SetQueueName(queue);
            }

            builder.ToJob(methodCall);

            _settingDrivenJobs.WatchJobSetting(builder.Build());
        }

        public void AddOrUpdate<T>(
            string recurringJobId,
            Expression<Func<T, Task>> methodCall,
            string cronExpression,
            TimeZoneInfo timeZone = null,
            string queue = null)
        {
            var options = new RecurringJobOptions { TimeZone = timeZone ?? TimeZoneInfo.Utc };
            if (!string.IsNullOrEmpty(queue))
            {
                RecurringJob.AddOrUpdate(recurringJobId, queue.ToLowerInvariant(), methodCall, cronExpression, options);
            }
            else
            {
                RecurringJob.AddOrUpdate(recurringJobId, methodCall, cronExpression, options);
            }
        }

        public void RemoveIfExists(string recurringJobId)
        {
            RecurringJob.RemoveIfExists(recurringJobId);
        }
    }
}
