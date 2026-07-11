namespace VirtoCommerce.BackgroundJobs.SampleModule.Jobs.Benchmark;

/// <summary>One unit of map work for the benchmark map/reduce batch — carries the same tunables as the fire job,
/// plus <see cref="ResultBytes"/> to size the produced result (which is stored in Redis, so it exercises the
/// map/reduce memory amplification described in the sizing docs).</summary>
public sealed record BenchmarkMapItem(
    string RunId,
    int Index,
    WorkloadKind Kind,
    int DelayMs,
    long CpuIterations,
    int ResultBytes,
    int FailRatePercent,
    int Seed,
    string? Filler);

/// <summary>Map result — tiny by default (index + optional filler), so the Redis results hash stays small unless a
/// run deliberately sizes it up via <c>ResultBytes</c>.</summary>
public sealed record BenchmarkMapResult(int Index, string? Payload);

/// <summary>Reduce state carried across the whole batch.</summary>
public sealed record BenchmarkState(string RunId, long StartedTicksUtc);
