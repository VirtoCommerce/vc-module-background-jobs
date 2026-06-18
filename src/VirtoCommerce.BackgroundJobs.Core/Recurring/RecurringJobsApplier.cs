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

namespace VirtoCommerce.BackgroundJobs.Core.Recurring;

/// <summary>
/// Applies every recurring job declared via <c>AddRecurringJob</c> (in the platform or any module) to the active
/// engine's <see cref="IRecurringJobScheduler"/>. Resolves the effective cron (fixed or setting-driven) and
/// re-applies live when a watched setting changes. Registered by the background-job engine module, so it runs only
/// when an engine is installed; as a hosted service it starts after module post-initialization, so the engine's
/// recurring store is ready.
/// </summary>
public sealed class RecurringJobsApplier : BackgroundService, IEventHandler<ObjectSettingChangedEvent>
{
    private readonly IReadOnlyList<RecurringJobRegistration> _registrations;
    private readonly IRecurringJobScheduler _scheduler;
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<RecurringJobsApplier> _logger;

    public RecurringJobsApplier(
        IEnumerable<RecurringJobRegistration> registrations,
        IRecurringJobScheduler scheduler,
        IServiceProvider serviceProvider,
        ILogger<RecurringJobsApplier> logger)
    {
        _registrations = registrations.ToList();
        _scheduler = scheduler;
        _serviceProvider = serviceProvider;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_registrations.Count == 0)
        {
            return;
        }

        using var scope = _serviceProvider.CreateScope();
        var settingsManager = scope.ServiceProvider.GetService<ISettingsManager>();

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

    private async Task ApplyAsync(RecurringJobRegistration registration, ISettingsManager? settingsManager, CancellationToken cancellationToken)
    {
        bool enabled;
        string? cron;

        if (registration.EnablerSetting is not null && registration.CronSetting is not null && settingsManager is not null)
        {
            enabled = await settingsManager.GetValueAsync<bool>(registration.EnablerSetting);
            cron = await settingsManager.GetValueAsync<string>(registration.CronSetting);
        }
        else
        {
            enabled = true;
            cron = registration.CronExpression;
        }

        if (enabled && !string.IsNullOrWhiteSpace(cron))
        {
            await _scheduler.AddOrUpdate(registration, cron!, cancellationToken);
        }
        else
        {
            await _scheduler.Remove(registration.Id, cancellationToken);
        }
    }
}
