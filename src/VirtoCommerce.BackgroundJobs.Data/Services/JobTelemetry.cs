#nullable enable
using System;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace VirtoCommerce.BackgroundJobs.Data.Services;

/// <summary>
/// Emits engine-agnostic job telemetry via the standard .NET OpenTelemetry primitives from the shared dispatch path,
/// so Hangfire and RabbitMQ are measured identically. 
/// </summary>
/// <remarks>
/// Pre-aggregated metrics use only low-cardinality dimensions (engine, handler, outcome) so they are immune to
/// sampling and never blow the per-metric series cap. The high-cardinality run id is attached to a drill-down
/// JobCompleted activity instead (subject to sampling) and runs are isolated by time window + engine anyway.
/// </remarks>
public sealed class JobTelemetry : IDisposable
{
    /// <summary>ActivitySource/Meter name.</summary>
    public const string MetricNamespace = "VirtoCommerce.BackgroundJobs";

    private readonly ActivitySource _activitySource = new(MetricNamespace);
    private readonly Meter _meter = new(MetricNamespace);
    private readonly Histogram<double> _executionDuration;
    private readonly Histogram<double> _queueLatency;

    public JobTelemetry()
    {
        _executionDuration = _meter.CreateHistogram<double>(
            MetricNamespace + "/jobs/execution.duration.ms", "ms", "Background job handler execution time.");
        _queueLatency = _meter.CreateHistogram<double>(
            MetricNamespace + "/jobs/queue.latency.ms", "ms", "Enqueue-to-dispatch delay for background jobs.");
    }

    /// <summary>True while anything is listening for background-job traces (diagnostic only — recording is always cheap).</summary>
    public bool Enabled => _activitySource.HasListeners();

    /// <summary>
    /// Records one completed job. <paramref name="executionMs"/> is the handler run time; <paramref name="queueLatencyMs"/>
    /// is the enqueue→dispatch delay (null when the enqueue-time header is absent, e.g. a redelivered legacy message).
    /// </summary>
    public void JobCompleted(string engine, string handler, string outcome, string? runId, double executionMs, double? queueLatencyMs)
    {
        // Pre-aggregated, sampling-immune (count is the throughput signal; the value is the latency distribution).
        _executionDuration.Record(executionMs,
            new("engine", engine), new("handler", handler), new("outcome", outcome));
        if (queueLatencyMs is >= 0)
        {
            _queueLatency.Record(queueLatencyMs.Value,
                new("engine", engine), new("handler", handler), new("outcome", outcome));
        }

        // Drill-down span carrying the run id for per-run analysis.
        var end = DateTimeOffset.UtcNow;
        var activity = _activitySource.StartActivity(
            "JobCompleted", ActivityKind.Internal, default(ActivityContext), startTime: end.AddMilliseconds(-executionMs));
        if (activity is null)
        {
            return;
        }

        activity.SetTag("engine", engine);
        activity.SetTag("handler", handler);
        activity.SetTag("outcome", outcome);
        if (!string.IsNullOrEmpty(runId))
        {
            activity.SetTag("runId", runId);
        }
        if (queueLatencyMs is { } queue)
        {
            activity.SetTag("queueLatencyMs", queue);
        }

        activity.SetEndTime(end.UtcDateTime);
        activity.Stop();
    }

    public void Dispose()
    {
        _meter.Dispose();
        _activitySource.Dispose();
    }
}
