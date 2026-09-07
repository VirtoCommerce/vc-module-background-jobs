#nullable enable
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using VirtoCommerce.BackgroundJobs.Core.Cancellation;

namespace VirtoCommerce.BackgroundJobs.Data.Cancellation;

/// <summary>
/// Single-process <see cref="IJobCancellationStore"/> for dev/testing and no-Redis deployments. Not fleet-safe: a
/// flag set on one instance is invisible to others, so cancellation only reaches jobs running in this process.
/// </summary>
public sealed class InMemoryJobCancellationStore : IJobCancellationStore
{
    private readonly ConcurrentDictionary<string, byte> _cancelled = new();

    public Task RequestCancel(string jobId, CancellationToken cancellationToken = default)
    {
        _cancelled[jobId] = 1;
        return Task.CompletedTask;
    }

    public Task<bool> IsCancelRequested(string jobId, CancellationToken cancellationToken = default)
        => Task.FromResult(_cancelled.ContainsKey(jobId));

    public Task Clear(string jobId, CancellationToken cancellationToken = default)
    {
        _cancelled.TryRemove(jobId, out _);
        return Task.CompletedTask;
    }
}
