#nullable enable
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using VirtoCommerce.Platform.Core.Jobs;
using Xunit;

namespace VirtoCommerce.BackgroundJobs.Tests;

/// <summary>
/// Certifies the static <see cref="BackgroundJob"/> migration facade: once initialized with a root provider, it opens
/// a scope, resolves the scoped <see cref="IBackgroundJob"/>, and delegates the enqueue verbatim.
/// </summary>
public class StaticBackgroundJobTests
{
    private sealed class SamplePayload { public string? Value { get; set; } }
    private sealed class SampleHandler { }

    [Fact]
    public async Task Enqueue_ResolvesScopedFacade_AndDelegates()
    {
        var facade = new Mock<IBackgroundJob>();
        facade
            .Setup(x => x.Enqueue<SampleHandler>(It.IsAny<object>(), It.IsAny<EnqueueOptions?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync("job-123");

        var services = new ServiceCollection();
        // Registered SCOPED on purpose — matches the real facade lifetime, so this also proves the static helper
        // creates a scope (resolving a scoped service straight from the root provider would throw).
        services.AddScoped(_ => facade.Object);
        using var provider = services.BuildServiceProvider(validateScopes: true);

        BackgroundJob.Initialize(provider);

        var payload = new SamplePayload { Value = "hi" };
        var options = new EnqueueOptions { Title = "static-enqueue" };
        var jobId = await BackgroundJob.Enqueue<SampleHandler>(payload, options, TestContext.Current.CancellationToken);

        Assert.Equal("job-123", jobId);
        facade.Verify(x => x.Enqueue<SampleHandler>(payload, options, It.IsAny<CancellationToken>()), Times.Once);
    }
}
