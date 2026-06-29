using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using VirtoCommerce.BackgroundJobs.Core.MapReduce;
using VirtoCommerce.BackgroundJobs.SampleModule.Jobs;
using VirtoCommerce.BackgroundJobs.SampleModule.Jobs.Indexing;
using VirtoCommerce.Platform.Core.Common;
using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs.SampleModule.Controllers.Api;

/// <summary>Endpoints to enqueue the sample background jobs for manual testing.</summary>
[Authorize]
[Route("api/background-jobs-sample")]
public class SampleJobsController(IBackgroundJob backgroundJob, IMapReduceJob mapReduce) : Controller
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

        // Handler-explicit enqueue: the call site names the action (SampleJob) that will run the payload.
        var jobId = await backgroundJob.Enqueue<SampleJob>(
            payload,
            new EnqueueOptions { ReportProgress = withProgress },
            cancellationToken);

        return Ok(jobId);
    }

    /// <summary>
    /// Enqueue a map/reduce "product indexing" batch: fan out one map task per page of ids (run in parallel across
    /// workers), then a single reduce aggregates the per-page counts. Returns the batch id. A few ids are seeded as
    /// "bad-*" so the stand-in indexer reports per-page failures, demonstrating <c>ContinueOnError</c>.
    /// </summary>
    /// <param name="productCount">How many fake product ids to index.</param>
    /// <param name="pageSize">Ids per map task (fan out over pages, not individual documents).</param>
    [HttpPost("index")]
    [ProducesResponseType(typeof(string), StatusCodes.Status200OK)]
    public async Task<ActionResult<string>> IndexProducts(
        [FromQuery] int productCount = 1000,
        [FromQuery] int pageSize = 50,
        CancellationToken cancellationToken = default)
    {
        // Simulate a catalog; every 137th id is "bad-*" so the stand-in indexer fails that one (see IndexPageHandler).
        var documentIds = Enumerable.Range(1, productCount)
            .Select(i => i % 137 == 0 ? $"bad-{i}" : $"product-{i:D5}")
            .ToArray();

        // Partition into PAGES — keeps the map-task count and per-result size small.
        var pages = documentIds.Chunk(pageSize).Select(chunk => new IndexPage("Product", chunk));

        // Note: no custom Queue here — the map/reduce tasks run on the default queue, which the engine always drains.
        // Routing to a dedicated queue (e.g. "indexing") additionally requires a worker configured for that queue
        // (VirtoCommerce:Hangfire:Queues or the RabbitMQ consumer queues), otherwise the tasks would sit unprocessed.
        var batchId = await mapReduce.Enqueue<IndexPage, IndexPageResult, IndexSummary>(
            items: pages,
            state: new IndexSummary("Product", DateTime.UtcNow.Ticks),
            options: new MapReduceOptions
            {
                FailurePolicy = FailurePolicy.ContinueOnError,
                ReportProgress = true,
            },
            cancellationToken: cancellationToken);

        return Ok(batchId);
    }
}
