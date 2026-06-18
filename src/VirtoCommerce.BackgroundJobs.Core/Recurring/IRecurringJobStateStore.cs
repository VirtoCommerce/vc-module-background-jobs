using System;
using System.Threading;
using System.Threading.Tasks;

namespace VirtoCommerce.BackgroundJobs.Core.Recurring;

/// <summary>
/// Shared, durable store of the last fired occurrence per recurring job. Used by the scheduler to de-duplicate
/// firing across a multi-instance fleet (see <see cref="RecurringJobSchedulerHostedService"/>): the distributed
/// lock serializes the check-and-set, and this marker guarantees a given occurrence is enqueued exactly once even
/// when instances tick at slightly different times.
/// <para>The default implementation is DB-backed so it is shared across all instances and survives a cache flush.</para>
/// </summary>
public interface IRecurringJobStateStore
{
    /// <summary>Returns the UTC timestamp of the last occurrence that was enqueued for the job, or null if never.</summary>
    Task<DateTime?> GetLastOccurrence(string recurringJobId, CancellationToken cancellationToken = default);

    /// <summary>Records <paramref name="occurrenceUtc"/> as the last enqueued occurrence for the job.</summary>
    Task SetLastOccurrence(string recurringJobId, DateTime occurrenceUtc, CancellationToken cancellationToken = default);
}
