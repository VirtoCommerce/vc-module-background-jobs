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

            /// <summary>Trigger a registered background job on demand via the admin/integration REST API.</summary>
            public const string Execute = "background-jobs:execute";

            public static string[] AllPermissions { get; } =
            [
                Access,
                Create,
                Read,
                Update,
                Delete,
                Execute,
            ];
        }
    }

    // NOTE: this module intentionally defines NO platform settings. All background-job configuration (provider, mode,
    // default queue, retry attempts) is a deployment-time concern read only from appsettings.json
    // (VirtoCommerce:BackgroundJobs) at startup — not an admin-UI toggle.
}
