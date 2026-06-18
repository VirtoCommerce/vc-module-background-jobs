#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
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
    private readonly ILogger<RecurringJobInvoker> _logger;

    public RecurringJobInvoker(
        IEnumerable<RecurringJobRegistration> registrations,
        IBackgroundJob backgroundJob,
        ILogger<RecurringJobInvoker> logger)
    {
        _registrations = registrations;
        _backgroundJob = backgroundJob;
        _logger = logger;
    }

    public async Task Run(string recurringJobId)
    {
        var registration = _registrations.FirstOrDefault(x => string.Equals(x.Id, recurringJobId, StringComparison.OrdinalIgnoreCase));
        if (registration is null)
        {
            _logger.LogWarning("Recurring job '{JobId}' fired but no matching registration was found.", recurringJobId);
            return;
        }

        await registration.Trigger(_backgroundJob, CancellationToken.None);
    }
}
