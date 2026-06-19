using System;

namespace VirtoCommerce.BackgroundJobs.Core.Services;

/// <summary>Outcome of comparing the active <see cref="IJobEngine"/> against the configured provider.</summary>
public enum BackgroundJobsEngineStatus
{
    /// <summary>No engine is registered — enqueue will fail.</summary>
    NoEngine,

    /// <summary>An engine is registered, but its <see cref="IJobEngine.ProviderName"/> differs from the configured provider.</summary>
    ProviderMismatch,

    /// <summary>An engine matching the configured provider is active.</summary>
    Ready,
}

/// <summary>
/// Single source of truth for the "is a background-job engine wired correctly?" check, so the startup validation
/// (Module) and the health check classify the situation identically instead of duplicating the branching.
/// </summary>
public static class BackgroundJobsEngineStatusEvaluator
{
    public static BackgroundJobsEngineStatus Evaluate(IJobEngine? engine, string configuredProvider)
    {
        if (engine is null)
        {
            return BackgroundJobsEngineStatus.NoEngine;
        }

        return string.Equals(engine.ProviderName, configuredProvider, StringComparison.OrdinalIgnoreCase)
            ? BackgroundJobsEngineStatus.Ready
            : BackgroundJobsEngineStatus.ProviderMismatch;
    }
}
