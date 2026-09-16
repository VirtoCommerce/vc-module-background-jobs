#nullable enable
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Moq;
using VirtoCommerce.BackgroundJobs.Core;
using VirtoCommerce.BackgroundJobs.Data.Services;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.PushNotifications;
using VirtoCommerce.Platform.Core.Security;
using Xunit;

namespace VirtoCommerce.BackgroundJobs.Tests;

public class FacadeCancellationTests
{
    private static JobEngineBackgroundJob Create(IJobEngine? engine)
        => new(Mock.Of<IJobPayloadSerializer>(),
               Options.Create(new BackgroundJobsOptions()),
               Mock.Of<IPushNotificationManager>(),
               Mock.Of<IUserNameResolver>(),
               engine);

    [Fact]
    public async Task Cancel_DelegatesToEngineDelete()
    {
        var engine = new Mock<IJobEngine>();
        engine.Setup(x => x.Delete("job-1", It.IsAny<CancellationToken>())).ReturnsAsync(true);
        engine.SetupGet(x => x.SupportsCancellation).Returns(true);
        var facade = Create(engine.Object);

        Assert.True(facade.SupportsCancellation);
        Assert.True(await facade.Cancel("job-1", TestContext.Current.CancellationToken));
        engine.Verify(x => x.Delete("job-1", It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Cancel_NoEngine_ReturnsFalse_AndNotSupported()
    {
        var facade = Create(engine: null);
        Assert.False(facade.SupportsCancellation);
        Assert.False(await facade.Cancel("job-1", TestContext.Current.CancellationToken));
    }
}
