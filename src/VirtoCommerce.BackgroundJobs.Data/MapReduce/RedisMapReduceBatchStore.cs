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

        // REPLACE, don't append: delete any existing list first so a re-save for the same batchId cannot double the
        // items. Appending would fan out more map tasks than Total and let reduce fire after only the first Total
        // indices complete. This matches the in-memory store's replace semantics. In the normal flow SaveItemsAsync
        // runs once per fresh batchId, inline in the producer and before the fan-out job is enqueued, so the delete is
        // a no-op — but making it idempotent removes the footgun instead of relying on that invariant.
        await db.KeyDeleteAsync(key);

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

    public async Task<int> GetFanOutProgressAsync(string batchId, CancellationToken cancellationToken = default)
    {
        var value = await _connection.GetDatabase().StringGetAsync(FanOutProgressKey(batchId));
        return value.IsNullOrEmpty ? 0 : (int)value;
    }

    public Task SetFanOutProgressAsync(string batchId, int dispatched, CancellationToken cancellationToken = default)
        => _connection.GetDatabase().StringSetAsync(FanOutProgressKey(batchId), dispatched, _ttl);

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
        await db.KeyDeleteAsync([MetaKey(batchId), ResultsKey(batchId), ReduceKey(batchId), ItemsKey(batchId), FanOutProgressKey(batchId)]);
    }

    // The batch id is wrapped in a Redis hash tag "{...}" so every key for one batch maps to the SAME cluster slot.
    // Redis Cluster requires all keys of a multi-key command to be in a single slot; without the tag the five keys
    // scatter across slots and CompleteAsync's multi-key KeyDelete fails with a CROSSSLOT error. (On a single node the
    // tag is inert.)
    private static string Key(string batchId, string suffix) => $"vc:mapreduce:{{{batchId}}}:{suffix}";
    private static string MetaKey(string batchId) => Key(batchId, "meta");
    private static string ResultsKey(string batchId) => Key(batchId, "results");
    private static string ReduceKey(string batchId) => Key(batchId, "reduce");
    private static string ItemsKey(string batchId) => Key(batchId, "items");
    private static string FanOutProgressKey(string batchId) => Key(batchId, "fanoutprogress");
}
