# Benchmark Results — Hangfire vs RabbitMQ

Copy this file per benchmark round and fill it from Application Insights (`benchmark-kql.md`) + the NBomber reports.
Roll the headline scorecard into `benchmark-test-plan-deck.html`.

## Run metadata

| | |
|---|---|
| Date / owner | |
| Environment | local / AKS (region, node SKU) |
| Platform version | |
| Managed services | SQL: … · Redis: … · RabbitMQ: … |
| Knobs | Hangfire `WorkerCount`= … · RabbitMQ `PrefetchCount`= … `ConsumerDispatchConcurrency`= … |

## Scorecard (headline)

| Dimension | Hangfire (SQL) | RabbitMQ + Redis |
|---|---|---|
| Throughput (jobs/s) | | |
| Latency p95 end-to-end (ms) | | |
| CPU per 1k jobs (core-s) | | |
| Memory footprint (app, MB) | | |
| Broker/Redis memory (MB) | n/a / — | |
| External dependencies | SQL Server | RabbitMQ + Redis |
| Scale-to-zero | no | yes (KEDA) |
| Cost / million jobs | | |

## Per-profile (fill per engine)

| Profile | Engine | Throughput (jobs/s) | p50 / p95 / p99 exec (ms) | p95 queue latency (ms) | CPU % (avg) | Mem (MB) | Notes |
|---|---|--:|--|--:|--:|--:|---|
| P1 burst 10k | Hangfire | | | | | | |
| P1 burst 10k | RabbitMQ | | | | | | |
| P2 sustained | Hangfire | | | | | | |
| P2 sustained | RabbitMQ | | | | | | |
| P3 IO-bound | Hangfire | | | | | | |
| P3 IO-bound | RabbitMQ | | | | | | |
| P4 CPU-bound | Hangfire | | | | | | |
| P4 CPU-bound | RabbitMQ | | | | | | |
| P5 map/reduce | Hangfire | | | | | | Redis peak MB: |
| P5 map/reduce | RabbitMQ | | | | | | Redis peak MB: |
| P6 large payload | Hangfire | | | | | | broker/Redis MB: |
| P6 large payload | RabbitMQ | | | | | | broker/Redis MB: |

## Pull vs Push (RabbitMQ worker)

| Metric | Pull (fixed N) | Push (KEDA 0→N) |
|---|--:|--:|
| Idle cost (replicas at rest) | | 0 |
| Activation latency p50 / p95 (s) | ~0 (warm) | |
| Burst drain time (s) | | |
| Max replicas reached | N (fixed) | |
| Sustained throughput (jobs/s) | | |
| Scale-down: duplicates / lost | — | / 0 |

## Sizing validation

| | Predicted (formula) | Observed |
|---|--:|--:|
| Redis per batch (Total=…, item/result=…) | | |
| RabbitMQ broker memory at backlog=… | | |

## Findings / recommendation

- …
