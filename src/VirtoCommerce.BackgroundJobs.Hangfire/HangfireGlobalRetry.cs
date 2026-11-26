#nullable enable
using System;
using Hangfire;
using VirtoCommerce.BackgroundJobs.Core.Models;

namespace VirtoCommerce.BackgroundJobs.Hangfire;

/// <summary>
/// The engine-wide Hangfire retry count, read from the registered <see cref="AutomaticRetryAttribute"/> so the attempt
/// total shown matches what Hangfire does, whoever installed the attribute.
/// </summary>
internal static class HangfireGlobalRetry
{
    /// <summary>
    /// The last registered <see cref="AutomaticRetryAttribute"/>, which is the one Hangfire applies; null when none is
    /// registered.
    /// </summary>
    public static AutomaticRetryAttribute? Current()
    {
        AutomaticRetryAttribute? current = null;

        foreach (var filter in GlobalJobFilters.Filters)
        {
            if (filter.Instance is AutomaticRetryAttribute attribute)
            {
                current = attribute;
            }
        }

        return current;
    }

    /// <summary>How many retries the job described by <paramref name="envelope"/> gets on top of its first run.</summary>
    public static int AttemptsFor(JobEnvelope envelope)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        var global = Current();
        if (global is null)
        {
            return 0;
        }

        return Math.Max(0, envelope.MaxRetryAttempts ?? global.Attempts);
    }

    /// <summary>
    /// <paramref name="envelope"/> with the attempt a job that spent <paramref name="retryCount"/> retries is about to
    /// run, and its total attempts.
    /// </summary>
    public static JobEnvelope WithAttemptBudget(JobEnvelope envelope, int retryCount)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        return envelope with
        {
            Attempt = retryCount + 1,
            TotalAttempts = AttemptsFor(envelope) + 1,
        };
    }
}
