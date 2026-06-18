#nullable enable
using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis;
using VirtoCommerce.BackgroundJobs.Core.Recurring;

namespace VirtoCommerce.BackgroundJobs.Data.Recurring;

/// <summary>
/// Redis-backed <see cref="IRecurringJobStateStore"/> — the shared, durable marker used to de-duplicate recurring
/// firing across a multi-instance fleet. Stores the last enqueued occurrence per job id as a string key with a long
/// TTL (well beyond any cron interval, so it never expires within a single occurrence's straggler window).
/// </summary>
public sealed class RedisRecurringJobStateStore : IRecurringJobStateStore
{
    private static readonly TimeSpan _ttl = TimeSpan.FromDays(30);

    private readonly IConnectionMultiplexer _connection;

    public RedisRecurringJobStateStore(IConnectionMultiplexer connection)
    {
        _connection = connection;
    }

    public async Task<DateTime?> GetLastOccurrence(string recurringJobId, CancellationToken cancellationToken = default)
    {
        var value = await _connection.GetDatabase().StringGetAsync(GetKey(recurringJobId));
        if (value.IsNullOrEmpty)
        {
            return null;
        }

        return DateTime.TryParse(value!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var occurrence)
            ? occurrence
            : null;
    }

    public Task SetLastOccurrence(string recurringJobId, DateTime occurrenceUtc, CancellationToken cancellationToken = default)
        => _connection.GetDatabase().StringSetAsync(
            GetKey(recurringJobId),
            occurrenceUtc.ToString("o", CultureInfo.InvariantCulture),
            _ttl);

    private static string GetKey(string recurringJobId) => $"vc:recurring-job:lastfired:{recurringJobId}";
}
