using System.Collections.Generic;
using VirtoCommerce.Platform.Core.Settings;

namespace VirtoCommerce.BackgroundJobs.Core;

public static class ModuleConstants
{
    public static class Security
    {
        public static class Permissions
        {
            public const string Access = "background-jobs:access";
            public const string Create = "background-jobs:create";
            public const string Read = "background-jobs:read";
            public const string Update = "background-jobs:update";
            public const string Delete = "background-jobs:delete";

            public static string[] AllPermissions { get; } =
            [
                Access,
                Create,
                Read,
                Update,
                Delete,
            ];
        }
    }

    public static class Settings
    {
        public static class General
        {
            // NOTE: the active Provider and Mode are read from configuration (VirtoCommerce:BackgroundJobs) at
            // startup — these settings surface them in the admin UI and require a restart to take effect.
            public static SettingDescriptor Provider { get; } = new()
            {
                Name = "BackgroundJobs.Provider",
                GroupName = "BackgroundJobs|General",
                ValueType = SettingValueType.ShortText,
                DefaultValue = "Hangfire",
                AllowedValues = ["Hangfire", "RabbitMQ"],
                RestartRequired = true,
            };

            public static SettingDescriptor DefaultQueue { get; } = new()
            {
                Name = "BackgroundJobs.DefaultQueue",
                GroupName = "BackgroundJobs|General",
                ValueType = SettingValueType.ShortText,
                DefaultValue = "default",
            };

            public static SettingDescriptor MaxRetryAttempts { get; } = new()
            {
                Name = "BackgroundJobs.MaxRetryAttempts",
                GroupName = "BackgroundJobs|General",
                ValueType = SettingValueType.Integer,
                DefaultValue = 3,
            };

            public static IEnumerable<SettingDescriptor> AllGeneralSettings
            {
                get
                {
                    yield return Provider;
                    yield return DefaultQueue;
                    yield return MaxRetryAttempts;
                }
            }
        }

        public static IEnumerable<SettingDescriptor> AllSettings
        {
            get
            {
                return General.AllGeneralSettings;
            }
        }
    }
}
