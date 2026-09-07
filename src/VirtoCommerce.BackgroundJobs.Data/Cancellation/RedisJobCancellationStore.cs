#nullable enable
using System;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis;
using VirtoCommerce.BackgroundJobs.Core.Cancellation;

namespace VirtoCommerce.BackgroundJobs.Data.Cancellation;

/// <summary>
/// Redis-backed <see cref="IJobCancellationStore"/> — fleet-wide. A cancel writes a short-lived key that any worker
/// (the one running the job, or the one that later dequeues it) reads. Keys carry a TTL so a cancel for a job that
/// already finished self-cleans; <see cref="Clear"/> removes it eagerly when the job settles.
/// </summary>
public sealed class RedisJobCancellationStore : IJobCancellationStore
{
    // Outlives a realistic max job duration + queue wait, so a running/queued job still observes the flag; the key
    // self-cleans if Clear is never reached (worker crash, etc.).
    private static readonly TimeSpan _ttl = TimeSpan.FromHours(6);

    private readonly IConnectionMultiplexer _connection;

    public RedisJobCancellationStore(IConnectionMultiplexer connection)
    {
        _connection = connection;
    }

    public Task RequestCancel(string jobId, CancellationToken cancellationToken = default)
        => _connection.GetDatabase().StringSetAsync(GetKey(jobId), "1", _ttl);

    public async Task<bool> IsCancelRequested(string jobId, CancellationToken cancellationToken = default)
        => await _connection.GetDatabase().KeyExistsAsync(GetKey(jobId));

    public Task Clear(string jobId, CancellationToken cancellationToken = default)
        => _connection.GetDatabase().KeyDeleteAsync(GetKey(jobId));

    private static string GetKey(string jobId) => $"vc:jobs:cancel:{jobId}";
}
