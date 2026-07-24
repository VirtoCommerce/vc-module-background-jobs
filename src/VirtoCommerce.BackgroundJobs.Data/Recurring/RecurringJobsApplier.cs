#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using VirtoCommerce.Platform.Core.Common;
using VirtoCommerce.Platform.Core.Events;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.Settings;
using VirtoCommerce.Platform.Core.Settings.Events;

namespace VirtoCommerce.BackgroundJobs.Data.Recurring;

/// <summary>
/// Applies every recurring job declared via <c>AddRecurringJob</c> (in the platform or any module) to the active
/// engine's <see cref="IRecurringJobScheduler"/>. Resolves the effective cron (fixed or setting-driven) and
/// re-applies live when a watched setting changes. Registered by the background-job engine module, so it runs only
/// when an engine is installed; as a hosted service it starts after module post-initialization, so the engine's
/// recurring store is ready.
/// </summary>
public sealed class RecurringJobsApplier : BackgroundService, IEventHandler<ObjectSettingChangedEvent>
{
    private readonly List<RecurringJobRegistration> _registrations;
    private readonly IRecurringJobScheduler? _scheduler;
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<RecurringJobsApplier> _logger;

    public RecurringJobsApplier(
        IEnumerable<RecurringJobRegistration> registrations,
        IServiceProvider serviceProvider,
        ILogger<RecurringJobsApplier> logger,
        // Optional: supplied by the active engine (Hangfire-native or the in-process cron scheduler). Null when no
        // engine registered one — every declared recurring job is then skipped with an actionable warning.
        IRecurringJobScheduler? scheduler = null)
    {
        _scheduler = scheduler;
        _serviceProvider = serviceProvider;
        _logger = logger;

        // Reject duplicate recurring-job ids: two registrations sharing an id would non-deterministically run one
        // under the other's schedule/payload. Keep the first per id and log an error naming the duplicates so the
        // misconfiguration is visible rather than silent.
        _registrations = registrations
            .GroupBy(x => x.Id, StringComparer.OrdinalIgnoreCase)
            .Select(group =>
            {
                if (group.Count() > 1)
                {
                    _logger.LogError(
                        "Duplicate recurring-job id '{JobId}' registered {Count} times; only the first is applied. Give each AddRecurringJob a unique id.",
                        group.Key, group.Count());
                }

                return group.First();
            })
            .ToList();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_registrations.Count == 0)
        {
            return;
        }

        if (_scheduler is null)
        {
            _logger.LogWarning(
                "Background jobs: {Count} recurring job(s) are declared but no engine registered a recurring scheduler, so none will run. {Message}",
                _registrations.Count, BackgroundJobEngineNotInstalledException.DefaultMessage);
            return;
        }

        using var scope = _serviceProvider.CreateScope();
        var settingsManager = scope.ServiceProvider.GetRequiredService<ISettingsManager>();

        foreach (var registration in _registrations)
        {
            try
            {
                await ApplyAsync(registration, settingsManager, stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to apply recurring job '{JobId}'", registration.Id);
            }
        }
    }

    public async Task Handle(ObjectSettingChangedEvent message)
    {
        if (_scheduler is null)
        {
            return;
        }

        var changed = message.ChangedEntries
            .Where(x => x.EntryState is EntryState.Modified or EntryState.Added)
            .Select(x => x.NewEntry.Name)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        if (changed.Count == 0)
        {
            return;
        }

        var affected = _registrations
            .Where(x => x.EnablerSetting is not null &&
                        (changed.Contains(x.EnablerSetting.Name) || changed.Contains(x.CronSetting!.Name)))
            .ToList();

        if (affected.Count == 0)
        {
            return;
        }

        using var scope = _serviceProvider.CreateScope();
        var settingsManager = scope.ServiceProvider.GetRequiredService<ISettingsManager>();

        foreach (var registration in affected)
        {
            await ApplyAsync(registration, settingsManager, CancellationToken.None);
        }
    }

    private async Task ApplyAsync(RecurringJobRegistration registration, ISettingsManager settingsManager, CancellationToken cancellationToken)
    {
        // Resolve the effective schedule via the shared resolver (a setting-driven job reads its enabler + cron from
        // settings; a fixed-cron job uses its own Enabled flag + CronExpression) so the applier and the admin read
        // model stay in agreement.
        var (enabled, cron) = await RecurringScheduleResolver.ResolveAsync(registration, settingsManager);

        // Both callers (ExecuteAsync / Handle) return early when _scheduler is null, so it is non-null here.
        if (enabled && !string.IsNullOrWhiteSpace(cron))
        {
            await _scheduler!.AddOrUpdate(registration, cron!, cancellationToken);
        }
        else
        {
            await _scheduler!.Remove(registration.Id, cancellationToken);
        }
    }
}
