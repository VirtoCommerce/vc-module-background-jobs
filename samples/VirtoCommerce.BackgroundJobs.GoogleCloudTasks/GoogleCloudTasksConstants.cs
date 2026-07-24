namespace VirtoCommerce.BackgroundJobs.GoogleCloudTasks;

public static class GoogleCloudTasksConstants
{
    /// <summary>Provider name that activates this engine (<c>VirtoCommerce:BackgroundJobs:Provider</c>).</summary>
    public const string ProviderName = "GoogleCloudTasks";

    /// <summary>Top-level configuration section the options bind to.</summary>
    public const string ConfigurationSection = "VirtoCommerce:GoogleCloudTasks";

    /// <summary>Route the push callback controller listens on; Cloud Tasks POSTs the envelope here.</summary>
    public const string CallbackPath = "/api/background-jobs/google-cloud-tasks/callback";
}
