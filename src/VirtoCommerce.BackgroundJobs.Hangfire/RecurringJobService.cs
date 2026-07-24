using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Hangfire;
using VirtoCommerce.Platform.Core.Common;
using VirtoCommerce.Platform.Core.Events;
using VirtoCommerce.Platform.Core.Settings;
using VirtoCommerce.Platform.Core.Settings.Events;

namespace VirtoCommerce.Platform.Hangfire;

public class RecurringJobService : IRecurringJobService, IEventHandler<ObjectSettingChangedEvent>
{
    // Key is the observed setting name, value is the object of setting job
    private static readonly ConcurrentDictionary<string, SettingCronJob> _observedSettingsDict = new();

    private readonly IRecurringJobManager _recurringJobManager;
    private readonly ISettingsManager _settingsManager;

    public RecurringJobService(IRecurringJobManager recurringJobManager, ISettingsManager settingsManager)
    {
        _recurringJobManager = recurringJobManager;
        _settingsManager = settingsManager;
    }

    public void WatchJobSetting<T>(
        SettingDescriptor enablerSetting,
        SettingDescriptor cronSetting,
        Expression<Func<T, Task>> methodCall,
        string jobId,
        TimeZoneInfo timeZoneInfo,
        string queue)
    {
        var settingCronJob = new SettingCronJobBuilder(new SettingCronJob())
            .SetEnablerSetting(enablerSetting)
            .SetCronSetting(cronSetting)
            .SetJobId(jobId)
            .SetQueueName(queue)
            .SetTimeZoneInfo(timeZoneInfo)
            .ToJob(methodCall)
            .Build();

        WatchJobSetting(settingCronJob);
    }

    /// <summary>
    /// Use SettingCronJobBuilder for creating SettingCronJob
    /// </summary>
    public void WatchJobSetting(SettingCronJob settingCronJob)
    {
        Observe(settingCronJob);
        RunOrRemoveJob(settingCronJob);
    }

    /// <summary>
    /// Use SettingCronJobBuilder for creating SettingCronJob
    /// </summary>
    public Task WatchJobSettingAsync(SettingCronJob settingCronJob)
    {
        Observe(settingCronJob);
        return RunOrRemoveJobAsync(settingCronJob);
    }

    private static void Observe(SettingCronJob settingCronJob)
    {
        _observedSettingsDict.AddOrUpdate(settingCronJob.EnableSetting.Name, settingCronJob, (_, _) => settingCronJob);
        _observedSettingsDict.AddOrUpdate(settingCronJob.CronSetting.Name, settingCronJob, (_, _) => settingCronJob);
    }

    public async Task Handle(ObjectSettingChangedEvent message)
    {
        foreach (var settingName in message.ChangedEntries
                     .Where(x => x.EntryState is EntryState.Modified or EntryState.Added)
                     .Select(x => x.NewEntry.Name))
        {
            if (_observedSettingsDict.TryGetValue(settingName, out var settingCronJob))
            {
                await RunOrRemoveJobAsync(settingCronJob);
            }
        }
    }

    // Synchronous path for the (synchronous) IRecurringJobService.WatchJobSetting API — uses the synchronous settings
    // read so it doesn't block on async (no Task.GetAwaiter().GetResult()).
    private void RunOrRemoveJob(SettingCronJob settingCronJob)
    {
        var enableValue = _settingsManager.GetValue<object>(settingCronJob.EnableSetting);

        if (settingCronJob.EnabledEvaluator(enableValue))
        {
            ScheduleJob(settingCronJob, _settingsManager.GetValue<string>(settingCronJob.CronSetting));
        }
        else
        {
            _recurringJobManager.RemoveIfExists(settingCronJob.RecurringJobId);
        }
    }

    private async Task RunOrRemoveJobAsync(SettingCronJob settingCronJob)
    {
        var enableValue = await _settingsManager.GetValueAsync<object>(settingCronJob.EnableSetting);

        if (settingCronJob.EnabledEvaluator(enableValue))
        {
            ScheduleJob(settingCronJob, await _settingsManager.GetValueAsync<string>(settingCronJob.CronSetting));
        }
        else
        {
            _recurringJobManager.RemoveIfExists(settingCronJob.RecurringJobId);
        }
    }

    private void ScheduleJob(SettingCronJob settingCronJob, string cronExpression)
    {
        var options = new RecurringJobOptions
        {
            TimeZone = settingCronJob.TimeZone,
#pragma warning disable CS0618 // Type or member is obsolete
            // Remove when Hangfire.MySqlStorage will be updated to support JobStorageFeatures.JobQueueProperty
            QueueName = settingCronJob.Queue,
#pragma warning restore CS0618 // Type or member is obsolete
        };

        _recurringJobManager.AddOrUpdate(
            settingCronJob.RecurringJobId,
            settingCronJob.Job,
            cronExpression,
            options);
    }
}
