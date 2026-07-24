using System;
using System.Collections.Concurrent;
using System.Threading;

namespace VirtoCommerce.BackgroundJobs.SampleModule.Jobs.Benchmark;

/// <summary>
/// In-memory, single-instance progress tracker for a benchmark run, powering the <c>GET benchmark/{runId}</c>
/// convenience endpoint. It only sees completions on the SAME instance that runs the handlers — so it is accurate
/// when Mode=Both (local), but on a separate worker tier the producer won't observe worker completions. Canonical
/// throughput/latency always comes from Application Insights; this is a lightweight local aid, not the source of truth.
/// </summary>
public sealed class BenchmarkRunRegistry
{
    private readonly ConcurrentDictionary<string, RunState> _runs = new();

    public void Start(string runId, int total, string kind) =>
        _runs[runId] = new RunState { Total = total, Kind = kind, StartedUtc = DateTime.UtcNow };

    public void Complete(string runId, bool success)
    {
        if (!_runs.TryGetValue(runId, out var state))
        {
            return; // run started on another instance (worker-only) — nothing to track here.
        }

        if (success)
        {
            Interlocked.Increment(ref state.Completed);
        }
        else
        {
            Interlocked.Increment(ref state.Failed);
        }
    }

    public RunState? Get(string runId) => _runs.TryGetValue(runId, out var state) ? state : null;

    /// <summary>Mutable per-run counters (fields, so they can be updated with <see cref="Interlocked"/>).</summary>
    public sealed class RunState
    {
        public int Total;
        public int Completed;
        public int Failed;
        public string Kind = "";
        public DateTime StartedUtc;
    }
}
