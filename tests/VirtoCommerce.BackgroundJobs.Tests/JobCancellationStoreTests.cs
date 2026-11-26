#nullable enable
using System;
using System.Threading.Tasks;
using VirtoCommerce.BackgroundJobs.Data.Cancellation;
using Xunit;

namespace VirtoCommerce.BackgroundJobs.Tests;

public class JobCancellationStoreTests
{
    [Fact]
    public async Task InMemory_Request_IsRequested_Clear_RoundTrips()
    {
        var store = new InMemoryJobCancellationStore();
        var ct = TestContext.Current.CancellationToken;

        Assert.False(await store.IsCancelRequested("job-1", ct));

        await store.RequestCancel("job-1", ct);
        Assert.True(await store.IsCancelRequested("job-1", ct));
        Assert.False(await store.IsCancelRequested("job-2", ct));   // isolated per id

        await store.Clear("job-1", ct);
        Assert.False(await store.IsCancelRequested("job-1", ct));
    }

    // The Redis store's key expires after six hours; a flag nothing clears (the consumer leaves it behind for a discarded
    // job's redelivery) must not live in this one for the life of the process: the TTL keeps the store from growing
    // without bound.
    [Fact]
    public async Task InMemory_FlagOlderThanTtl_ReadsAsNotRequested()
    {
        var time = new ManualTimeProvider();
        var store = new InMemoryJobCancellationStore(time);
        var ct = TestContext.Current.CancellationToken;

        await store.RequestCancel("job-1", ct);
        time.Advance(TimeSpan.FromHours(6) - TimeSpan.FromSeconds(1));
        Assert.True(await store.IsCancelRequested("job-1", ct));

        time.Advance(TimeSpan.FromSeconds(2));
        Assert.False(await store.IsCancelRequested("job-1", ct));

        // A request made after the expiry starts a new lifetime.
        await store.RequestCancel("job-1", ct);
        Assert.True(await store.IsCancelRequested("job-1", ct));
    }

    private sealed class ManualTimeProvider : TimeProvider
    {
        private DateTimeOffset _now = new(2026, 11, 25, 0, 0, 0, TimeSpan.Zero);

        public override DateTimeOffset GetUtcNow() => _now;

        public void Advance(TimeSpan by) => _now += by;
    }
}
