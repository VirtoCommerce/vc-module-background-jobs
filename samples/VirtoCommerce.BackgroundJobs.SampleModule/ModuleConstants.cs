using System.Collections.Generic;
using VirtoCommerce.Platform.Core.Settings;

namespace VirtoCommerce.BackgroundJobs.SampleModule;

public static class ModuleConstants
{
    public static class Settings
    {
        public static class General
        {
            public static readonly SettingDescriptor EnableHeartbeat = new()
            {
                Name = "BackgroundJobs.Sample.EnableHeartbeat",
                GroupName = "Background Jobs Sample|Recurring",
                ValueType = SettingValueType.Boolean,
                DefaultValue = true,
            };

            public static readonly SettingDescriptor HeartbeatCron = new()
            {
                Name = "BackgroundJobs.Sample.HeartbeatCron",
                GroupName = "Background Jobs Sample|Recurring",
                // The Cron value type validates the expression on save and renders a preset picker + a live
                // plain-English description in the admin UI.
                ValueType = SettingValueType.Cron,
                DefaultValue = "*/5 * * * *",
            };

            public static IEnumerable<SettingDescriptor> AllSettings
            {
                get
                {
                    yield return EnableHeartbeat;
                    yield return HeartbeatCron;
                }
            }
        }

        public static IEnumerable<SettingDescriptor> AllSettings => General.AllSettings;
    }
}
