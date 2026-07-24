#nullable enable
using System.Threading;
using System.Threading.Tasks;
using Hangfire;
using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs.Hangfire;

/// <summary>
/// Hangfire implementation of <see cref="IRecurringJobScheduler"/> using Hangfire's native recurring-job manager —
/// so recurring jobs are persisted in Hangfire storage, appear in the dashboard and are scheduled cluster-wide by
/// the Hangfire server. Each occurrence runs <see cref="IRecurringJobInvoker.Run"/>, which enqueues the payload.
/// </summary>
public sealed class HangfireRecurringJobScheduler : IRecurringJobScheduler
{
    private readonly IRecurringJobManager _recurringJobManager;

    public HangfireRecurringJobScheduler(IRecurringJobManager recurringJobManager)
    {
        _recurringJobManager = recurringJobManager;
    }

    public Task AddOrUpdate(RecurringJobRegistration registration, string cronExpression, CancellationToken cancellationToken = default)
    {
        _recurringJobManager.AddOrUpdate<IRecurringJobInvoker>(
            registration.Id,
            invoker => invoker.Run(registration.Id),
            cronExpression,
            new RecurringJobOptions { TimeZone = registration.TimeZone });

        return Task.CompletedTask;
    }

    public Task Remove(string recurringJobId, CancellationToken cancellationToken = default)
    {
        _recurringJobManager.RemoveIfExists(recurringJobId);
        return Task.CompletedTask;
    }
}
