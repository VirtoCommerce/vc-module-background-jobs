#nullable enable
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
}
