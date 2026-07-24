#nullable enable
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Moq;
using VirtoCommerce.BackgroundJobs;
using VirtoCommerce.BackgroundJobs.Web.Infrastructure.HealthChecks;
using Xunit;

namespace VirtoCommerce.BackgroundJobs.Tests;

public class BackgroundJobsHealthCheckTests
{
    private static IConfiguration Config(string? provider) =>
        new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?> { ["VirtoCommerce:BackgroundJobs:Provider"] = provider })
            .Build();

    private static HealthCheckContext Context(BackgroundJobsHealthCheck check) => new()
    {
        Registration = new HealthCheckRegistration("Background jobs", check, HealthStatus.Unhealthy, tags: null),
    };

    [Fact]
    public async Task Unhealthy_When_No_Engine()
    {
        var sut = new BackgroundJobsHealthCheck(Config("RabbitMQ"), engine: null);

        var result = await sut.CheckHealthAsync(Context(sut), TestContext.Current.CancellationToken);

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
    }

    [Fact]
    public async Task Healthy_When_Engine_Matches_Provider()
    {
        var engine = new Mock<IJobEngine>();
        engine.SetupGet(x => x.ProviderName).Returns("RabbitMQ");
        var sut = new BackgroundJobsHealthCheck(Config("RabbitMQ"), engine.Object);

        var result = await sut.CheckHealthAsync(Context(sut), TestContext.Current.CancellationToken);

        Assert.Equal(HealthStatus.Healthy, result.Status);
    }

    [Fact]
    public async Task Degraded_When_Engine_Does_Not_Match_Provider()
    {
        var engine = new Mock<IJobEngine>();
        engine.SetupGet(x => x.ProviderName).Returns("Hangfire");
        var sut = new BackgroundJobsHealthCheck(Config("RabbitMQ"), engine.Object);

        var result = await sut.CheckHealthAsync(Context(sut), TestContext.Current.CancellationToken);

        Assert.Equal(HealthStatus.Degraded, result.Status);
    }
}
