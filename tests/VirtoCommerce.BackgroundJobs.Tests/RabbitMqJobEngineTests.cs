using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Moq;
using VirtoCommerce.BackgroundJobs.Core.Services;
using VirtoCommerce.BackgroundJobs.RabbitMQ;
using Xunit;

namespace VirtoCommerce.BackgroundJobs.Tests;

/// <summary>
/// Broker-free unit tests for the RabbitMQ engine's contract behavior. Enqueue/consume require a live broker and
/// are covered by integration testing, not here.
/// </summary>
public class RabbitMqJobEngineTests
{
    private static RabbitMqJobEngine CreateEngine() =>
        new(Mock.Of<IRabbitMqConnectionProvider>(), Mock.Of<ILogger<RabbitMqJobEngine>>());

    [Fact]
    public void ProviderName_Is_RabbitMQ()
    {
        var engine = CreateEngine();

        Assert.Equal("RabbitMQ", engine.ProviderName);
    }

    [Fact]
    public async Task GetStatus_Returns_Null_Because_No_Ledger()
    {
        var engine = CreateEngine();

        var status = await engine.GetStatus("job-1", TestContext.Current.CancellationToken);

        // RabbitMQ keeps no job ledger, so every id is unknown — GetStatus returns null (the IJobEngine unknown
        // signal). The monitoring API maps null to a completed job so status pollers stop instead of hanging forever.
        Assert.Null(status);
    }

    [Fact]
    public async Task Delete_Returns_False()
    {
        var engine = CreateEngine();

        var deleted = await engine.Delete("job-1", TestContext.Current.CancellationToken);

        Assert.False(deleted);
    }
}
