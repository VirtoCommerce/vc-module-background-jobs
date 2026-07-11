# Benchmark KQL (Application Insights / Log Analytics)

Queries for the Hangfire-vs-RabbitMQ comparison. Custom metrics are emitted by `JobTelemetry` from the shared
dispatcher; CPU/memory come from the AI module's performance counters; dependency latency (SQL/Redis) is auto-collected.

> Metric names are prefixed `VirtoCommerce.BackgroundJobs/…`. Dimensions live in `customMetrics.customDimensions`
> (`engine`, `handler`, `outcome`). Scope every query to the run's time window and pick one `engine` at a time.

## Throughput (jobs/s) by engine

```kusto
customMetrics
| where name == "VirtoCommerce.BackgroundJobs/jobs/execution.duration.ms"
| extend engine = tostring(customDimensions.engine), outcome = tostring(customDimensions.outcome)
| summarize jobs = sum(valueCount) by engine, bin(timestamp, 1m)
| extend jobsPerSec = jobs / 60.0
| render timechart
```

## Execution + queue latency percentiles

```kusto
customMetrics
| where name in ("VirtoCommerce.BackgroundJobs/jobs/execution.duration.ms",
                 "VirtoCommerce.BackgroundJobs/jobs/queue.latency.ms")
| extend engine = tostring(customDimensions.engine)
| summarize p50 = percentile(value, 50), p95 = percentile(value, 95), p99 = percentile(value, 99)
          by name, engine
```

## Outcome / error rate

```kusto
customMetrics
| where name == "VirtoCommerce.BackgroundJobs/jobs/execution.duration.ms"
| extend engine = tostring(customDimensions.engine), outcome = tostring(customDimensions.outcome)
| summarize count = sum(valueCount) by engine, outcome
| evaluate pivot(outcome, sum(count))
```

## CPU % and memory (per role/pod)

```kusto
performanceCounters
| where name in ("% Processor Time", "Private Bytes", "Available Bytes")
| summarize avg(value), max(value) by name, cloud_RoleName, bin(timestamp, 1m)
| render timechart
```

CPU **per 1k jobs** = (avg `% Processor Time` × cores × window-seconds) ÷ (jobs completed in the window ÷ 1000);
compute jobs completed from the throughput query above for the same window and role.

## Dependency latency — SQL (Hangfire) vs Redis (map/reduce)

```kusto
dependencies
| where type in ("SQL", "Redis")
| summarize calls = count(), p95 = percentile(duration, 95), avg = avg(duration) by type, bin(timestamp, 1m)
| render timechart
```

## Per-run drill-down (JobCompleted event)

```kusto
customEvents
| where name == "JobCompleted"
| extend engine = tostring(customDimensions.engine), runId = tostring(customDimensions.runId),
         execMs = todouble(customMeasurements.executionMs)
| where runId == "<RUN_ID>"
| summarize jobs = count(), p95 = percentile(execMs, 95) by engine
```

> `JobCompleted` is subject to adaptive sampling; use it for drill-down/correlation, and the pre-aggregated
> `customMetrics` (above) for accurate counts and percentiles.

## Cloud extras

- **Worker replicas over time / per-pod CPU-mem:** Azure Monitor **Container Insights** (`KubePodInventory`,
  `Perf` / `InsightsMetrics`) — the source for KEDA scale 0→N behavior and idle cost.
- **Queue depth / publish-ack rates / broker memory:** RabbitMQ `rabbitmq_prometheus` → Azure Monitor managed
  Prometheus, or the management API.
