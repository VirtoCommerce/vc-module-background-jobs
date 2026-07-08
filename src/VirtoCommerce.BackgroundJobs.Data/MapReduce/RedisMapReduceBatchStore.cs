#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using StackExchange.Redis;
using VirtoCommerce.BackgroundJobs.Core.MapReduce;

namespace VirtoCommerce.BackgroundJobs.Data.MapReduce;

/// <summary>
/// Redis-backed <see cref="IMapReduceBatchStore"/> — fleet-safe across instances. Metadata is a string key; per-item
/// results are a hash field keyed by index (so a redelivered map task overwrites, keeping the join idempotent and
/// the completed count = <c>HLEN</c>); the reduce trigger is an atomic <c>SET NX</c>. Keys carry a TTL so abandoned
/// batches self-clean.
/// </summary>
public sealed class RedisMapReduceBatchStore : IMapReduceBatchStore
{
    private static readonly TimeSpan _ttl = TimeSpan.FromDays(7);

    private readonly IConnectionMultiplexer _connection;

    public RedisMapReduceBatchStore(IConnectionMultiplexer connection)
    {
        _connection = connection;
    }

    public async Task CreateAsync(MapReduceBatch batch, CancellationToken cancellationToken = default)
    {
        await _connection.GetDatabase().StringSetAsync(MetaKey(batch.BatchId), JsonConvert.SerializeObject(batch), _ttl);
    }

    public async Task<MapReduceBatch?> GetAsync(string batchId, CancellationToken cancellationToken = default)
    {
        var value = await _connection.GetDatabase().StringGetAsync(MetaKey(batchId));
        return value.IsNullOrEmpty ? null : JsonConvert.DeserializeObject<MapReduceBatch>(value!);
    }

    public async Task SaveItemsAsync(string batchId, IReadOnlyList<MapItem> items, CancellationToken cancellationToken = default)
    {
        var db = _connection.GetDatabase();
        var key = ItemsKey(batchId);

        // Called exactly ONCE per batch, inline in the producer (MapReduceJob.Enqueue), with a freshly generated
        // batchId that is never reused — so appending is safe (no prior items to duplicate) and the fan-out job is
        // enqueued only AFTER this returns, so a partially-written list is never observed. A failure here throws back
        // to the caller (no fan-out enqueued); the orphaned :items key self-cleans via TTL.
        // Push in chunks so a huge batch doesn't become one oversized RPUSH command.
        const int chunkSize = 500;
        for (var offset = 0; offset < items.Count; offset += chunkSize)
        {
            var values = items
                .Skip(offset)
                .Take(chunkSize)
                .Select(x => (RedisValue)JsonConvert.SerializeObject(x))
                .ToArray();

            await db.ListRightPushAsync(key, values);
        }

        await db.KeyExpireAsync(key, _ttl);
    }

    public async Task<IReadOnlyList<MapItem>> GetItemsAsync(string batchId, CancellationToken cancellationToken = default)
    {
        var values = await _connection.GetDatabase().ListRangeAsync(ItemsKey(batchId));
        return values
            .Select(x => JsonConvert.DeserializeObject<MapItem>(x!))
            .Where(x => x is not null)
            .Select(x => x!)
            .ToList();
    }

    public async Task<int> SaveResultAndCountAsync(string batchId, MapResultRecord result, CancellationToken cancellationToken = default)
    {
        var db = _connection.GetDatabase();
        var resultsKey = ResultsKey(batchId);

        await db.HashSetAsync(resultsKey, result.Index, JsonConvert.SerializeObject(result));
        await db.KeyExpireAsync(resultsKey, _ttl);

        return (int)await db.HashLengthAsync(resultsKey);
    }

    public Task<bool> TryBeginFanOutAsync(string batchId, CancellationToken cancellationToken = default)
        => _connection.GetDatabase().StringSetAsync(FanOutKey(batchId), "1", _ttl, When.NotExists);

    public Task ReleaseFanOutAsync(string batchId, CancellationToken cancellationToken = default)
        => _connection.GetDatabase().KeyDeleteAsync(FanOutKey(batchId));

    public Task<bool> TryBeginReduceAsync(string batchId, CancellationToken cancellationToken = default)
        => _connection.GetDatabase().StringSetAsync(ReduceKey(batchId), "1", _ttl, When.NotExists);

    public Task ReleaseReduceAsync(string batchId, CancellationToken cancellationToken = default)
        => _connection.GetDatabase().KeyDeleteAsync(ReduceKey(batchId));

    public async Task<IReadOnlyCollection<MapResultRecord>> GetResultsAsync(string batchId, CancellationToken cancellationToken = default)
    {
        var entries = await _connection.GetDatabase().HashGetAllAsync(ResultsKey(batchId));
        return entries
            .Select(x => JsonConvert.DeserializeObject<MapResultRecord>(x.Value!))
            .Where(x => x is not null)
            .Select(x => x!)
            .ToList();
    }

    public async Task CompleteAsync(string batchId, CancellationToken cancellationToken = default)
    {
        var db = _connection.GetDatabase();
        await db.KeyDeleteAsync([MetaKey(batchId), ResultsKey(batchId), ReduceKey(batchId), ItemsKey(batchId), FanOutKey(batchId)]);
    }

    private static string MetaKey(string batchId) => $"vc:mapreduce:{batchId}:meta";
    private static string ResultsKey(string batchId) => $"vc:mapreduce:{batchId}:results";
    private static string ReduceKey(string batchId) => $"vc:mapreduce:{batchId}:reduce";
    private static string ItemsKey(string batchId) => $"vc:mapreduce:{batchId}:items";
    private static string FanOutKey(string batchId) => $"vc:mapreduce:{batchId}:fanout";
}
