#nullable enable
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Moq;
using VirtoCommerce.BackgroundJobs;
using VirtoCommerce.BackgroundJobs.Web.Controllers.Api;
using VirtoCommerce.Platform.Core.Jobs;
using Xunit;

namespace VirtoCommerce.BackgroundJobs.Tests;

public class JobsControllerTests
{
    private static JobsController CreateController(IJobEngine engine) => new(engine);

    [Fact]
    public async Task GetStatus_When_Engine_Returns_Null_Returns_Completed_Job()
    {
        // An unknown/expired job id makes a status-capable engine (Hangfire) return null. The REST contract that the
        // admin UI (and the auto-tests) rely on never returns null — an unknown job reads as completed so pollers stop.
        var engine = new Mock<IJobEngine>();
        engine.Setup(x => x.GetStatus(It.IsAny<string>(), It.IsAny<CancellationToken>())).ReturnsAsync((Job?)null);

        var actionResult = await CreateController(engine.Object).GetStatus("missing-job", TestContext.Current.CancellationToken);

        var ok = Assert.IsType<OkObjectResult>(actionResult.Result);
        var job = Assert.IsType<Job>(ok.Value);
        Assert.Equal("missing-job", job.Id);
        Assert.True(job.Completed);
    }

    [Fact]
    public async Task GetStatus_When_Engine_Returns_Status_PassesItThrough()
    {
        var status = new Job { Id = "job-1", State = "Processing", Completed = false };
        var engine = new Mock<IJobEngine>();
        engine.Setup(x => x.GetStatus("job-1", It.IsAny<CancellationToken>())).ReturnsAsync(status);

        var actionResult = await CreateController(engine.Object).GetStatus("job-1", TestContext.Current.CancellationToken);

        var ok = Assert.IsType<OkObjectResult>(actionResult.Result);
        Assert.Same(status, ok.Value);
    }
}
