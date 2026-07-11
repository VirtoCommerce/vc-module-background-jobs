using VirtoCommerce.Platform.Core.Common;

namespace VirtoCommerce.BackgroundJobs.SampleModule.Jobs.Benchmark;

/// <summary>
/// Payload for the tunable fire-and-forget benchmark job. Every knob a load run varies lives here so the handler
/// itself stays fixed and the engine is the only variable.
/// </summary>
public class BenchmarkPayload : ValueObject
{
    /// <summary>Correlates all jobs from one load run (for the local progress registry and log/telemetry filtering).</summary>
    public string RunId { get; set; } = string.Empty;

    /// <summary>Position of this job within the run — drives deterministic failure selection.</summary>
    public int Index { get; set; }

    public WorkloadKind Kind { get; set; } = WorkloadKind.Io;

    /// <summary>IO delay per job (ms) for <see cref="WorkloadKind.Io"/>/<see cref="WorkloadKind.Mixed"/>.</summary>
    public int DelayMs { get; set; }

    /// <summary>CPU loop iterations for <see cref="WorkloadKind.Cpu"/>/<see cref="WorkloadKind.Mixed"/>.</summary>
    public long CpuIterations { get; set; }

    /// <summary>Percentage of jobs that fail deterministically (to exercise retries / DLQ). 0 = never.</summary>
    public int FailRatePercent { get; set; }

    /// <summary>Seed for the deterministic failure decision, so a run is repeatable.</summary>
    public int Seed { get; set; } = 1;

    /// <summary>Optional filler to inflate the message body (large-payload profile); null keeps messages small.</summary>
    public string? Filler { get; set; }
}
