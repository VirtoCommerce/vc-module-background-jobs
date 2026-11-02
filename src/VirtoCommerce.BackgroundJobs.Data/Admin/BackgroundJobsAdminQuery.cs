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
        => GetCatalog()
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
        => GetCatalog().FirstOrDefault(x => string.Equals(x.Name, name, StringComparison.OrdinalIgnoreCase));

    // One entry per distinct handler/payload (an identical re-registration collapses, last wins). The name is the
    // address, so when distinct handlers share one (e.g. the default type name of LogEntityChangesJobHandler, declared
    // by several modules) each is listed and addressed by its full handler type name instead.
    private IEnumerable<BackgroundJobDescriptor> GetCatalog()
        => descriptors
            .GroupBy(x => (x.HandlerType, x.PayloadType))
            .Select(g => g.Last())
            .GroupBy(x => x.Name, StringComparer.OrdinalIgnoreCase)
            .SelectMany(g => g.Count() == 1 ? g : g.Select(x => x with { Name = QualifiedName(x, g) }));

    // Full handler type name; suffixed with the payload when the same handler is registered for several payloads.
    private static string QualifiedName(BackgroundJobDescriptor descriptor, IEnumerable<BackgroundJobDescriptor> sameName)
    {
        var name = TypeName(descriptor.HandlerType);
        return sameName.Count(x => x.HandlerType == descriptor.HandlerType) == 1
            ? name
            : $"{name}:{TypeName(descriptor.PayloadType)}";
    }

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
