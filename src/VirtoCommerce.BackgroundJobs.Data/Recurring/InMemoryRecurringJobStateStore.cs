#nullable enable
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using VirtoCommerce.BackgroundJobs.Core.Recurring;

namespace VirtoCommerce.BackgroundJobs.Data.Recurring;

/// <summary>
/// In-process <see cref="IRecurringJobStateStore"/> used when no shared store (Redis) is configured — i.e. a
/// single-instance deployment, where cross-instance de-duplication is unnecessary.
/// </summary>
public sealed class InMemoryRecurringJobStateStore : IRecurringJobStateStore
{
    private readonly ConcurrentDictionary<string, DateTime> _lastOccurrence = new(StringComparer.OrdinalIgnoreCase);

    public Task<DateTime?> GetLastOccurrence(string recurringJobId, CancellationToken cancellationToken = default)
        => Task.FromResult<DateTime?>(_lastOccurrence.TryGetValue(recurringJobId, out var value) ? value : null);

    public Task SetLastOccurrence(string recurringJobId, DateTime occurrenceUtc, CancellationToken cancellationToken = default)
    {
        _lastOccurrence[recurringJobId] = occurrenceUtc;
        return Task.CompletedTask;
    }
}
