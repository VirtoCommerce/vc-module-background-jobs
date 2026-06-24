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

    public async Task<int> SaveResultAndCountAsync(string batchId, MapResultRecord result, CancellationToken cancellationToken = default)
    {
        var db = _connection.GetDatabase();
        var resultsKey = ResultsKey(batchId);

        await db.HashSetAsync(resultsKey, result.Index, JsonConvert.SerializeObject(result));
        await db.KeyExpireAsync(resultsKey, _ttl);

        return (int)await db.HashLengthAsync(resultsKey);
    }

    public Task<bool> TryBeginReduceAsync(string batchId, CancellationToken cancellationToken = default)
        => _connection.GetDatabase().StringSetAsync(ReduceKey(batchId), "1", _ttl, When.NotExists);

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
        await db.KeyDeleteAsync([MetaKey(batchId), ResultsKey(batchId), ReduceKey(batchId)]);
    }

    private static string MetaKey(string batchId) => $"vc:mapreduce:{batchId}:meta";
    private static string ResultsKey(string batchId) => $"vc:mapreduce:{batchId}:results";
    private static string ReduceKey(string batchId) => $"vc:mapreduce:{batchId}:reduce";
}
