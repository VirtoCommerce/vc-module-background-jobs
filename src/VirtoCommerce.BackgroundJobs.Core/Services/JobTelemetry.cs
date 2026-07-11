using System;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;

namespace VirtoCommerce.BackgroundJobs.Core.Services;

/// <summary>
/// Emits engine-agnostic job telemetry to Application Insights from the shared dispatch path, so Hangfire and
/// RabbitMQ are measured identically. The <see cref="TelemetryClient"/> is optional: when the platform's
/// Application Insights module is not installed it resolves to <c>null</c> and every method is a no-op — so this adds
/// no behavior and no cost to installs that don't use AI.
/// </summary>
/// <remarks>
/// Pre-aggregated metrics use only low-cardinality dimensions (engine, handler, outcome) so they are immune to
/// sampling and never blow the per-metric series cap. The high-cardinality run id is attached to a drill-down
/// <c>JobCompleted</c> event instead (subject to sampling), and runs are isolated by time window + engine anyway.
/// </remarks>
public sealed class JobTelemetry
{
    /// <summary>Metric namespace all background-job metrics live under (customMetrics.name prefix in Kusto).</summary>
    public const string MetricNamespace = "VirtoCommerce.BackgroundJobs";

    private readonly TelemetryClient? _client;
    private readonly Metric? _executionDuration;
    private readonly Metric? _queueLatency;

    public JobTelemetry(TelemetryClient? client = null)
    {
        _client = client;
        if (client is null)
        {
            return;
        }

        _executionDuration = client.GetMetric(
            MetricNamespace + "/jobs/execution.duration.ms", "engine", "handler", "outcome");
        _queueLatency = client.GetMetric(
            MetricNamespace + "/jobs/queue.latency.ms", "engine", "handler", "outcome");
    }

    /// <summary>True when telemetry is wired (the AI module is installed).</summary>
    public bool Enabled => _client is not null;

    /// <summary>
    /// Records one completed job. <paramref name="executionMs"/> is the handler run time; <paramref name="queueLatencyMs"/>
    /// is the enqueue→dispatch delay (null when the enqueue-time header is absent, e.g. a redelivered legacy message).
    /// </summary>
    public void JobCompleted(string engine, string handler, string outcome, string? runId, double executionMs, double? queueLatencyMs)
    {
        if (_client is null)
        {
            return;
        }

        // Pre-aggregated, sampling-immune (count is the throughput signal; the value is the latency distribution).
        _executionDuration!.TrackValue(executionMs, engine, handler, outcome);
        if (queueLatencyMs is >= 0)
        {
            _queueLatency!.TrackValue(queueLatencyMs.Value, engine, handler, outcome);
        }

        // Drill-down event carrying the run id for per-run analysis (customEvents in Kusto).
        var completed = new EventTelemetry("JobCompleted");
        completed.Properties["engine"] = engine;
        completed.Properties["handler"] = handler;
        completed.Properties["outcome"] = outcome;
        if (!string.IsNullOrEmpty(runId))
        {
            completed.Properties["runId"] = runId;
        }

        completed.Metrics["executionMs"] = executionMs;
        if (queueLatencyMs is { } queue)
        {
            completed.Metrics["queueLatencyMs"] = queue;
        }

        _client.TrackEvent(completed);
    }
}
