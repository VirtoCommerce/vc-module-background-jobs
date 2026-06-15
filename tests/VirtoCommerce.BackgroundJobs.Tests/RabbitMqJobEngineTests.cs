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
    public void Engine_Is_Not_Expression_Capable()
    {
        // Delegates can't be serialized onto a queue, so the RabbitMQ engine must not advertise expression enqueue;
        // JobEngineBackgroundJob relies on this to throw NotSupportedException for expression-based enqueue.
        var engine = CreateEngine();

        Assert.IsNotAssignableFrom<IExpressionJobEngine>(engine);
    }

    [Fact]
    public async Task GetStatus_Returns_Unknown_NotCompleted()
    {
        var engine = CreateEngine();

        var status = await engine.GetStatus("job-1", TestContext.Current.CancellationToken);

        Assert.NotNull(status);
        Assert.Equal("job-1", status.Id);
        Assert.Equal("Unknown", status.State);
        Assert.False(status.Completed);
    }

    [Fact]
    public async Task Delete_Returns_False()
    {
        var engine = CreateEngine();

        var deleted = await engine.Delete("job-1", TestContext.Current.CancellationToken);

        Assert.False(deleted);
    }
}
