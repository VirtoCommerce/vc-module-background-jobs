namespace VirtoCommerce.BackgroundJobs.SampleModule.Jobs.Benchmark;

/// <summary>Shape of the simulated work a benchmark job performs, so a run can isolate one cost dimension.</summary>
public enum WorkloadKind
{
    /// <summary>IO-bound: <c>await Task.Delay</c> — exercises consumer concurrency / worker-count without burning CPU.</summary>
    Io,

    /// <summary>CPU-bound: a bounded hash loop — exercises the thread pool, GC, and CPU per job.</summary>
    Cpu,

    /// <summary>Both a delay and a CPU burn — a realistic mixed handler.</summary>
    Mixed,
}
