using System;
using Microsoft.Extensions.Configuration;

namespace VirtoCommerce.BackgroundJobs.Core;

/// <summary>
/// Helpers for reading the engine selector from configuration, so engine modules (built-in or custom) don't each
/// re-implement the provider-name check. A custom engine's <c>IPlatformStartup</c>/<c>IModule</c> calls
/// <see cref="IsBackgroundJobsProvider"/> to self-activate only when its provider is the configured one.
/// </summary>
public static class BackgroundJobsConfigurationExtensions
{
    /// <summary>Configuration key selecting the active engine.</summary>
    public const string ProviderKey = "VirtoCommerce:BackgroundJobs:Provider";

    /// <summary>Configuration key toggling legacy Hangfire bootstrap (see <see cref="BackgroundJobsOptions.EnableLegacyHangfire"/>).</summary>
    public const string EnableLegacyHangfireKey = "VirtoCommerce:BackgroundJobs:EnableLegacyHangfire";

    /// <summary>Provider assumed when the key is empty (back-compat: Hangfire was the platform's only engine).</summary>
    public const string DefaultProvider = BackgroundJobsProviders.Hangfire;

    /// <summary>The configured provider name, or <see cref="DefaultProvider"/> when none is set.</summary>
    public static string GetBackgroundJobsProvider(this IConfiguration configuration)
    {
        var provider = configuration[ProviderKey];
        return string.IsNullOrEmpty(provider) ? DefaultProvider : provider;
    }

    /// <summary>True when the configured engine matches <paramref name="providerName"/> (case-insensitive).</summary>
    public static bool IsBackgroundJobsProvider(this IConfiguration configuration, string providerName) =>
        string.Equals(configuration.GetBackgroundJobsProvider(), providerName, StringComparison.OrdinalIgnoreCase);
}
