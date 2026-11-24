#nullable enable
using System;
using Hangfire;
using VirtoCommerce.BackgroundJobs.Core.Models;

namespace VirtoCommerce.BackgroundJobs.Hangfire;

/// <summary>
/// The one place the engine-wide Hangfire retry count is read from, for everything that has to agree on it: the
/// executor (how many attempts a job gets, shown as "attempt N of M") and the per-job retry filter. The count lives on
/// the <see cref="AutomaticRetryAttribute"/> registered in <see cref="GlobalJobFilters.Filters"/>; reading it from there
/// rather than from configuration keeps the shown total equal to what Hangfire actually does, whoever installed the
/// attribute (this module's startup, or a host that replaced it).
/// </summary>
internal static class HangfireGlobalRetry
{
    /// <summary>
    /// The <see cref="AutomaticRetryAttribute"/> Hangfire applies to a job. Hangfire ships one of its own in
    /// <see cref="GlobalJobFilters.Filters"/> and, of several filters of one type, keeps the one registered last
    /// (<c>JobFilterProviderCollection.RemoveDuplicates</c>, which walks from the back) — so the last one registered is
    /// the one in force. Null when none is registered.
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

        return Math.Max(0, envelope.MaxRetryAttempts ?? Current()?.Attempts ?? 0);
    }
}
