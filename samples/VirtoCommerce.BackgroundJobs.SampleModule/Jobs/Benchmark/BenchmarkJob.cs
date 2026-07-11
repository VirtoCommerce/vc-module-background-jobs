using System;
using System.Threading;
using System.Threading.Tasks;
using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs.SampleModule.Jobs.Benchmark;

/// <summary>
/// Tunable fire-and-forget benchmark handler. Idempotent (touches no external state beyond an in-memory counter),
/// async (never blocks a worker thread), and does exactly the work the payload asks for — so throughput, latency,
/// CPU and memory are attributable to the engine, not the handler.
/// </summary>
public sealed class BenchmarkJob(BenchmarkRunRegistry registry) : IBackgroundJobHandler<BenchmarkPayload>
{
    public async Task Execute(BenchmarkPayload payload, IJobExecutionContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            await BenchmarkWork.RunAsync(payload.Kind, payload.DelayMs, payload.CpuIterations, cancellationToken);

            if (BenchmarkWork.ShouldFail(payload.Seed, payload.Index, payload.FailRatePercent))
            {
                throw new InvalidOperationException($"Simulated benchmark failure (index {payload.Index}).");
            }

            registry.Complete(payload.RunId, success: true);
        }
        catch (OperationCanceledException)
        {
            throw; // shutdown / token — let the engine requeue; not a failure.
        }
        catch
        {
            registry.Complete(payload.RunId, success: false);
            throw; // exercise the engine's retry / DLQ path.
        }
    }
}
