using System;
using System.Threading;
using System.Threading.Tasks;

namespace VirtoCommerce.BackgroundJobs.SampleModule.Jobs.Benchmark;

/// <summary>
/// Shared, deterministic work simulation used by both the fire-and-forget and map/reduce benchmark handlers, so the
/// two paths exercise the same tunable workload and the engine is the only variable between runs.
/// </summary>
public static class BenchmarkWork
{
    /// <summary>Runs the configured workload. IO uses a real async delay (never a blocking sleep, which would distort
    /// worker-concurrency measurements); CPU burns a bounded loop.</summary>
    public static async Task RunAsync(WorkloadKind kind, int delayMs, long cpuIterations, CancellationToken cancellationToken)
    {
        if (kind is WorkloadKind.Io or WorkloadKind.Mixed && delayMs > 0)
        {
            await Task.Delay(delayMs, cancellationToken);
        }

        if (kind is WorkloadKind.Cpu or WorkloadKind.Mixed && cpuIterations > 0)
        {
            BurnCpu(cpuIterations, cancellationToken);
        }
    }

    /// <summary>Deterministic per-item failure decision (so runs are repeatable): fails <paramref name="failPercent"/>%
    /// of items, chosen by a stable hash of the seed + item index.</summary>
    public static bool ShouldFail(int seed, int index, int failPercent)
    {
        if (failPercent <= 0)
        {
            return false;
        }

        if (failPercent >= 100)
        {
            return true;
        }

        var bucket = (uint)HashCode.Combine(seed, index) % 100u;
        return bucket < (uint)failPercent;
    }

    /// <summary>A filler string of roughly <paramref name="bytes"/> length to inflate payload / result size on demand
    /// (for the large-payload profile). Returns null when not requested, to keep messages small by default.</summary>
    public static string? MakeFiller(int bytes) => bytes > 0 ? new string('x', bytes) : null;

    private static void BurnCpu(long iterations, CancellationToken cancellationToken)
    {
        // FNV-1a style rolling hash — cheap, data-dependent so the loop can't be optimized away.
        var accumulator = 1469598103934665603UL;
        for (long i = 0; i < iterations; i++)
        {
            accumulator = (accumulator ^ (ulong)i) * 1099511628211UL;
            if ((i & 0xFFFF) == 0)
            {
                cancellationToken.ThrowIfCancellationRequested();
            }
        }

        // Consume the result so the JIT keeps the loop (never true in practice).
        if (accumulator == 0)
        {
            throw new InvalidOperationException("unreachable");
        }
    }
}
