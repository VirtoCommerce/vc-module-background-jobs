#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Hangfire;
using Microsoft.Extensions.Logging;
using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs.Hangfire;

/// <summary>
/// Default <see cref="IRecurringJobInvoker"/>. Resolved by Hangfire per recurring-job execution (so the scoped
/// <see cref="IBackgroundJob"/> resolves correctly), it finds the registration and enqueues its payload.
/// </summary>
public sealed class RecurringJobInvoker : IRecurringJobInvoker
{
    private readonly IEnumerable<RecurringJobRegistration> _registrations;
    private readonly IBackgroundJob _backgroundJob;
    private readonly IRecurringJobManager _recurringJobManager;
    private readonly ILogger<RecurringJobInvoker> _logger;

    public RecurringJobInvoker(
        IEnumerable<RecurringJobRegistration> registrations,
        IBackgroundJob backgroundJob,
        IRecurringJobManager recurringJobManager,
        ILogger<RecurringJobInvoker> logger)
    {
        _registrations = registrations;
        _backgroundJob = backgroundJob;
        _recurringJobManager = recurringJobManager;
        _logger = logger;
    }

    public async Task Run(string recurringJobId)
    {
        var registration = _registrations.FirstOrDefault(x => string.Equals(x.Id, recurringJobId, StringComparison.OrdinalIgnoreCase));
        if (registration is null)
        {
            // This recurring job is ours (Hangfire scheduled it to call us) but its declaration is gone — a renamed
            // or removed AddRecurringJob. Self-clean: remove the orphaned schedule so it stops firing.
            _logger.LogWarning("Recurring job '{JobId}' fired but its registration no longer exists; removing the orphaned schedule.", recurringJobId);
            _recurringJobManager.RemoveIfExists(recurringJobId);
            return;
        }

        await registration.Trigger(_backgroundJob, CancellationToken.None);
    }
}
