using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using VirtoCommerce.BackgroundJobs.SampleModule.Jobs;
using VirtoCommerce.Platform.Core.Common;
using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs.SampleModule.Controllers.Api;

/// <summary>Endpoints to enqueue the sample background job for manual testing.</summary>
[Authorize]
[Route("api/background-jobs-sample")]
public class SampleJobsController(IBackgroundJob backgroundJob) : Controller
{
    /// <summary>
    /// Enqueue a sample fire-and-forget job. Returns the engine job id (poll it via
    /// <c>GET api/platform/jobs/{id}</c>).
    /// </summary>
    /// <param name="withProgress">When true, progress is streamed to the admin notification UI over SignalR.</param>
    /// <param name="steps">Number of progress steps the job reports.</param>
    /// <param name="message">A message echoed in the progress/log.</param>
    [HttpPost("enqueue")]
    [ProducesResponseType(typeof(string), StatusCodes.Status200OK)]
    public async Task<ActionResult<string>> Enqueue(
        [FromQuery] bool withProgress = true,
        [FromQuery] int steps = 3,
        [FromQuery] string message = "Hello from SampleJob",
        CancellationToken cancellationToken = default)
    {
        var payload = AbstractTypeFactory<SampleJobPayload>.TryCreateInstance();
        payload.Message = message;
        payload.StepCount = steps;

        var jobId = await backgroundJob.Enqueue(
            payload,
            new EnqueueOptions { ReportProgress = withProgress },
            cancellationToken);

        return Ok(jobId);
    }
}
