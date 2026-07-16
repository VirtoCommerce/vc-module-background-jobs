#nullable enable
using System;
using System.Threading.Tasks;
using Cronos;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.Settings;

namespace VirtoCommerce.BackgroundJobs.Core.Recurring;

/// <summary>
/// Shared resolution of a recurring job's <b>effective</b> schedule (enabled + cron) and its next occurrence, so the
/// applier (which schedules) and the admin read model (which reports) agree on one implementation. Cron parsing
/// matches <see cref="GenericRecurringJobScheduler"/>: 6+ whitespace-separated fields ⇒ seconds-aware.
/// </summary>
public static class RecurringScheduleResolver
{
    /// <summary>
    /// Resolves the effective enabled flag and cron for a registration: a setting-driven schedule reads the enabler +
    /// cron from settings; a fixed-cron schedule uses its own <see cref="RecurringJobRegistration.Enabled"/> flag and
    /// <see cref="RecurringJobRegistration.CronExpression"/>.
    /// </summary>
    public static async Task<(bool Enabled, string? Cron)> ResolveAsync(
        RecurringJobRegistration registration, ISettingsManager settingsManager)
    {
        if (registration.EnablerSetting is not null && registration.CronSetting is not null)
        {
            var enabled = await settingsManager.GetValueAsync<bool>(registration.EnablerSetting);
            var cron = await settingsManager.GetValueAsync<string>(registration.CronSetting);
            return (enabled, cron);
        }

        return (registration.Enabled, registration.CronExpression);
    }

    /// <summary>Parses a cron expression, honoring an optional seconds field (6+ fields ⇒ <see cref="CronFormat.IncludeSeconds"/>).</summary>
    public static CronExpression ParseCron(string cron)
    {
        var fields = cron.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        return CronExpression.Parse(cron, fields.Length >= 6 ? CronFormat.IncludeSeconds : CronFormat.Standard);
    }

    /// <summary>
    /// The next UTC occurrence of <paramref name="cron"/> after <paramref name="fromUtc"/> evaluated in
    /// <paramref name="timeZone"/>, or null when the cron is empty/invalid or has no future occurrence.
    /// </summary>
    public static DateTime? GetNextOccurrence(string? cron, TimeZoneInfo timeZone, DateTime fromUtc)
    {
        if (string.IsNullOrWhiteSpace(cron))
        {
            return null;
        }

        try
        {
            return ParseCron(cron).GetNextOccurrence(fromUtc, timeZone);
        }
        catch (CronFormatException)
        {
            return null;
        }
    }
}
