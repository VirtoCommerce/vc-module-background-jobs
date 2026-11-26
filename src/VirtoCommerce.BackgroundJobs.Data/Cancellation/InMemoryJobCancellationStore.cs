#nullable enable
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using VirtoCommerce.BackgroundJobs.Core.Cancellation;

namespace VirtoCommerce.BackgroundJobs.Data.Cancellation;

/// <summary>
/// Single-process <see cref="IJobCancellationStore"/> for dev/testing and no-Redis deployments. Not fleet-safe: a
/// flag set on one instance is invisible to others, so cancellation only reaches jobs running in this process. A flag
/// expires after <see cref="_ttl"/>.
/// </summary>
public sealed class InMemoryJobCancellationStore : IJobCancellationStore
{
    // Keep equal to RedisJobCancellationStore's key expiry.
    private static readonly TimeSpan _ttl = TimeSpan.FromHours(6);

    private readonly ConcurrentDictionary<string, DateTimeOffset> _cancelled = new();
    private readonly TimeProvider _timeProvider;

    public InMemoryJobCancellationStore()
        : this(TimeProvider.System)
    {
    }

    public InMemoryJobCancellationStore(TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(timeProvider);

        _timeProvider = timeProvider;
    }

    public Task RequestCancel(string jobId, CancellationToken cancellationToken = default)
    {
        var now = _timeProvider.GetUtcNow();
        _cancelled[jobId] = now + _ttl;
        PruneExpired(now);

        return Task.CompletedTask;
    }

    public Task<bool> IsCancelRequested(string jobId, CancellationToken cancellationToken = default)
    {
        if (!_cancelled.TryGetValue(jobId, out var expiresAt))
        {
            return Task.FromResult(false);
        }

        if (expiresAt > _timeProvider.GetUtcNow())
        {
            return Task.FromResult(true);
        }

        // Removed only if it is still the entry that expired: a request made since then carries a later expiry.
        _cancelled.TryRemove(new(jobId, expiresAt));

        return Task.FromResult(false);
    }

    public Task Clear(string jobId, CancellationToken cancellationToken = default)
    {
        _cancelled.TryRemove(jobId, out _);

        return Task.CompletedTask;
    }

    private void PruneExpired(DateTimeOffset now)
    {
        foreach (var expired in _cancelled.Where(x => x.Value <= now).ToList())
        {
            _cancelled.TryRemove(expired);
        }
    }
}
