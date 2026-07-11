using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using VirtoCommerce.BackgroundJobs.SampleModule.Jobs.Benchmark;
using VirtoCommerce.Platform.Core.Common;
using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs.SampleModule.Controllers.Api;

/// <summary>
/// Load-control endpoints for the engine benchmark. These are PRODUCERS only — they enqueue a tunable workload and
/// return a run id; throughput/latency/CPU/memory are read from Application Insights (canonical) and the
/// <c>GET benchmark/{runId}</c> endpoint gives a coarse local progress readout. Drive volume with the NBomber
/// scenarios in <c>tests/VirtoCommerce.BackgroundJobs.Benchmarks</c>.
/// </summary>
[Authorize]
[Route("api/background-jobs-sample/benchmark")]
public class BenchmarkController(IBackgroundJob backgroundJob, IMapReduceJob mapReduce, BenchmarkRunRegistry registry) : Controller
{
    /// <summary>Enqueue <paramref name="count"/> fire-and-forget benchmark jobs of the given shape. Returns the run id
    /// and how long enqueuing took (producer-side).</summary>
    [HttpPost("fire")]
    [ProducesResponseType(typeof(object), StatusCodes.Status200OK)]
    public async Task<ActionResult<object>> Fire(
        [FromQuery] int count = 1000,
        [FromQuery] WorkloadKind kind = WorkloadKind.Io,
        [FromQuery] int delayMs = 50,
        [FromQuery] long cpu = 0,
        [FromQuery] int payloadBytes = 0,
        [FromQuery] int failPct = 0,
        CancellationToken cancellationToken = default)
    {
        var runId = Guid.NewGuid().ToString("N");
        var filler = BenchmarkWork.MakeFiller(payloadBytes);
        registry.Start(runId, count, kind.ToString());

        var stopwatch = Stopwatch.StartNew();
        var enqueued = 0;
        // Stop cooperatively if the client disconnects; each enqueue is atomic (CancellationToken.None) so a publish
        // in flight is never torn (see RabbitMqJobEngine.Enqueue). Best-practice bulk-producer pattern.
        for (var i = 0; i < count && !cancellationToken.IsCancellationRequested; i++)
        {
            var payload = AbstractTypeFactory<BenchmarkPayload>.TryCreateInstance();
            payload.RunId = runId;
            payload.Index = i;
            payload.Kind = kind;
            payload.DelayMs = delayMs;
            payload.CpuIterations = cpu;
            payload.FailRatePercent = failPct;
            payload.Filler = filler;

            await backgroundJob.Enqueue<BenchmarkJob>(payload, new EnqueueOptions(), CancellationToken.None);
            enqueued++;
        }

        stopwatch.Stop();
        return Ok(new { runId, requested = count, count = enqueued, enqueueMs = stopwatch.Elapsed.TotalMilliseconds });
    }

    /// <summary>Enqueue one map/reduce batch of <paramref name="width"/> items — the fan-out + Redis + reduce path.</summary>
    [HttpPost("mapreduce")]
    [ProducesResponseType(typeof(object), StatusCodes.Status200OK)]
    public async Task<ActionResult<object>> MapReduce(
        [FromQuery] int width = 500,
        [FromQuery] WorkloadKind kind = WorkloadKind.Io,
        [FromQuery] int delayMs = 50,
        [FromQuery] long cpu = 0,
        [FromQuery] int resultBytes = 0,
        [FromQuery] int failPct = 0,
        CancellationToken cancellationToken = default)
    {
        var runId = Guid.NewGuid().ToString("N");
        registry.Start(runId, width, $"mapreduce:{kind}");

        var items = Enumerable.Range(0, width)
            .Select(i => new BenchmarkMapItem(runId, i, kind, delayMs, cpu, resultBytes, failPct, Seed: 1, Filler: null));

        var stopwatch = Stopwatch.StartNew();
        var batchId = await mapReduce.Enqueue<BenchmarkMapHandler, BenchmarkReducer>(
            items,
            new BenchmarkState(runId, DateTime.UtcNow.Ticks),
            new MapReduceOptions { FailurePolicy = FailurePolicy.ContinueOnError },
            cancellationToken);
        stopwatch.Stop();

        return Ok(new { runId, batchId, width, enqueueMs = stopwatch.Elapsed.TotalMilliseconds });
    }

    /// <summary>Coarse local progress for a run (see <see cref="BenchmarkRunRegistry"/> for the single-instance caveat).</summary>
    [HttpGet("{runId}")]
    [ProducesResponseType(typeof(object), StatusCodes.Status200OK)]
    public ActionResult<object> Status(string runId)
    {
        var state = registry.Get(runId);
        if (state is null)
        {
            return Ok(new
            {
                runId,
                tracked = false,
                note = "Not tracked on this instance (worker-only, or the run was started elsewhere). Use Application Insights for canonical metrics.",
            });
        }

        var done = state.Completed + state.Failed;
        return Ok(new
        {
            runId,
            tracked = true,
            kind = state.Kind,
            state.Total,
            state.Completed,
            state.Failed,
            elapsedMs = (DateTime.UtcNow - state.StartedUtc).TotalMilliseconds,
            done = done >= state.Total,
        });
    }
}
