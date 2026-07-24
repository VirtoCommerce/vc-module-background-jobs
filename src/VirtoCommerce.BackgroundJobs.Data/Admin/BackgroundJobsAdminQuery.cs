#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using VirtoCommerce.BackgroundJobs.Core.Admin;
using VirtoCommerce.BackgroundJobs.Core.Recurring;
using VirtoCommerce.BackgroundJobs.Data.Recurring;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.Settings;

namespace VirtoCommerce.BackgroundJobs.Data.Admin;

/// <summary>
/// Default <see cref="IBackgroundJobsAdminQuery"/>: composes the DI registries with settings + the recurring state
/// store. All inputs are engine-agnostic, so the same view works on any provider.
/// </summary>
public sealed class BackgroundJobsAdminQuery(
    IEnumerable<BackgroundJobDescriptor> descriptors,
    IEnumerable<RecurringJobRegistration> recurringRegistrations,
    ISettingsManager settingsManager,
    // Optional: only registered by engines that use the in-process recurring scheduler (RabbitMQ/InMemory). Under
    // Hangfire the last-run lives in Hangfire storage, so it is simply reported as null here.
    IRecurringJobStateStore? stateStore = null) : IBackgroundJobsAdminQuery
{
    public IReadOnlyList<RegisteredJobInfo> GetRegisteredJobs()
        => descriptors
            // Last() per name to match DI's "last registration wins" (partner override), so the listed/addressed
            // descriptor is the one that actually runs.
            .GroupBy(x => x.Name, StringComparer.OrdinalIgnoreCase)
            .Select(g => g.Last())
            .OrderBy(x => x.Name, StringComparer.OrdinalIgnoreCase)
            .Select(x => new RegisteredJobInfo
            {
                Name = x.Name,
                HandlerType = TypeName(x.HandlerType),
                PayloadType = TypeName(x.PayloadType),
                Triggerable = x.Triggerable,
            })
            .ToList();

    public BackgroundJobDescriptor? FindRegisteredJob(string name)
        => descriptors.LastOrDefault(x => string.Equals(x.Name, name, StringComparison.OrdinalIgnoreCase));

    public async Task<IReadOnlyList<RecurringJobInfo>> GetRecurringJobsAsync(CancellationToken cancellationToken = default)
    {
        var now = DateTime.UtcNow;
        var result = new List<RecurringJobInfo>();

        var unique = recurringRegistrations
            .GroupBy(x => x.Id, StringComparer.OrdinalIgnoreCase)
            .Select(g => g.First());

        foreach (var registration in unique)
        {
            var (enabled, cron) = await RecurringScheduleResolver.ResolveAsync(registration, settingsManager);
            var settingDriven = registration.EnablerSetting is not null && registration.CronSetting is not null;

            var lastRun = stateStore is null
                ? null
                : await stateStore.GetLastOccurrence(registration.Id, cancellationToken);

            // Only compute a next occurrence for an enabled schedule; a disabled job isn't scheduled.
            var nextRun = enabled ? RecurringScheduleResolver.GetNextOccurrence(cron, registration.TimeZone, now) : null;

            result.Add(new RecurringJobInfo
            {
                Id = registration.Id,
                Cron = cron,
                Enabled = enabled,
                SettingDriven = settingDriven,
                TimeZone = registration.TimeZone.Id,
                HandlerType = registration.HandlerTypeName,
                PayloadType = registration.PayloadTypeName,
                LastRunUtc = lastRun,
                NextRunUtc = nextRun,
            });
        }

        return result.OrderBy(x => x.Id, StringComparer.OrdinalIgnoreCase).ToList();
    }

    // Assembly-qualified name → clean type name. Prefer the loaded type's FullName (correct for generics); fall back to
    // trimming the assembly identity only for a simple (non-generic) name so a generic AQN isn't truncated mid-bracket.
    private static string TypeName(string assemblyQualifiedName)
    {
        var loaded = Type.GetType(assemblyQualifiedName);
        if (loaded?.FullName is not null)
        {
            return loaded.FullName;
        }

        // Not loadable: only trim at the top-level comma when there are no generic brackets (else keep it intact).
        return assemblyQualifiedName.Contains('[')
            ? assemblyQualifiedName
            : assemblyQualifiedName.Split(',')[0].Trim();
    }
}
