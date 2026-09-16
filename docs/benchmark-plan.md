# Hangfire vs RabbitMQ — Benchmark & Sizing Plan (Virto Cloud)

An apples-to-apples comparison of the two background-job engines: **CPU, memory, throughput, latency**, plus a
**memory-sizing basis** for Redis & RabbitMQ and a **KEDA autoscaling** design for the worker tier.

- Slide deck (shareable): [`benchmark-test-plan-deck.html`](./benchmark-test-plan-deck.html)
- Kusto queries: [`benchmark-kql.md`](./benchmark-kql.md) · Scorecard: [`benchmark-results-template.md`](./benchmark-results-template.md)
- Load harness: [`tests/VirtoCommerce.BackgroundJobs.Benchmarks`](../tests/VirtoCommerce.BackgroundJobs.Benchmarks) · Cloud infra: [`deploy/k8s`](../deploy/k8s)

## Topology

```
Frontend → BFF (Producer, Mode=Producer) ──enqueue──▶ RabbitMQ (durable queue, .dlq)
                                          └─batch────▶ Redis (map/reduce items/results/meta/checkpoint)
                                                          ▼
                          Worker tier (Mode=Worker) ◀── KEDA scales 0..N on queue depth
                          consumer → DefaultJobDispatcher → handler; reducer persists a small result
```

Hangfire variant: no broker — the Producer enqueues into **SQL**, the Worker runs the Hangfire server polling the
same DB, scaled by **CPU HPA** (no queue-depth trigger, no scale-to-zero).

## Engines at a glance

| | Hangfire | RabbitMQ + Redis |
|---|---|---|
| Storage | SQL Server (shared platform DB) | Broker queue + Redis (map/reduce) |
| Scaling | workers poll SQL · CPU HPA | KEDA on queue depth · scale-from-zero |
| Scale-to-zero | ✗ | ✓ |
| Throughput knob | `WorkerCount` | `PrefetchCount` · `ConsumerDispatchConcurrency` |
| Delivery | at-least-once | at-least-once + DLQ |

Both run the identical `DefaultJobDispatcher` and the same handler contracts, so a single instrumentation point yields
fair numbers.

## Measurement (Application Insights)

The platform's `VirtoCommerce.ApplicationInsights` module auto-collects requests, dependencies (SQL/Redis/HTTP),
exceptions, **performance counters (CPU %, memory, threads)** and Live Metrics. We add engine-agnostic job telemetry
from the shared dispatcher (`JobTelemetry`) via the standard OpenTelemetry `ActivitySource`/`Meter` primitives:

- `VirtoCommerce.BackgroundJobs/jobs/execution.duration.ms` — handler run time (count = throughput)
- `VirtoCommerce.BackgroundJobs/jobs/queue.latency.ms` — enqueue→dispatch delay (from the `vc-enqueued-at` header)
- both dimensioned by `engine`, `handler`, `outcome` (low-cardinality → sampling-immune)
- a `JobCompleted` span (`ActivityKind.Internal`) carrying `runId` for per-run drill-down; its duration is the actual
  handler run time

Telemetry is inert unless a host registers a listener for the `VirtoCommerce.BackgroundJobs` ActivitySource/Meter
name. The AI v3 module does this via a wildcard subscription (`AddSource("VirtoCommerce.*")` /
`AddMeter("VirtoCommerce.*")`) covering every VirtoCommerce module by naming convention, rather than listing this
module by name. Broker/store depth: RabbitMQ management API + Redis `INFO memory`.

## Pull vs Push (same RabbitMQ worker image)

| Scenario | Pull (always-on) | Push (KEDA 0→N) | Metric |
|---|---|---|---|
| Idle cost | N replicas at rest | 0 replicas | idle cost/hr |
| Cold start | warm (~queue latency) | poll + schedule + warmup + connect | activation p50/p95 |
| Burst of K | fixed N drains | ramps to maxReplicas | drain time, ramp curve |
| Sustained X/s | right-sized N | KEDA holds N | throughput, p95, CPU/1k |
| Scale-down | n/a | drain mid-scale-down | 0 lost, dedup verified |

Pull steady-state + knob tuning ≈ local; Push cold-start/ramp/scale-to-zero = cloud-only. Hangfire is Pull-only.

## Memory sizing

**Redis (map/reduce only)** — per active batch:

```
mem ≈ (Total × itemJSON) + (Total × resultJSON) + meta(~0.3KB) + checkpoint    # ×1.5–2 overhead
```

| Total | item / result JSON | ≈ per batch (with overhead) |
|--:|--|--:|
| 10 000 | 0.5 / 0.3 KB | ~12–16 MB |
| 100 000 | 0.5 / 0.3 KB | ~120–160 MB |
| 1 000 000 | 0.5 / 0.3 KB | ~1.2–1.6 GB |

Size for **peak concurrent + abandoned** batches (7-day TTL). **`maxmemory-policy` MUST be `noeviction`** for the jobs
data — evicting a batch's items/results mid-run corrupts the reduce. Isolate from the platform cache/backplane
(dedicated instance or DB index). Recurring markers are negligible.

**RabbitMQ** — `mem ≈ ~120 MB baseline + (ready+unacked) × (body + ~300 B) + connections`. Persistent classic-queue
messages stay in memory until paged; **`vm_memory_high_watermark` (default 0.4)** blocks publishers past 40% of the
node's RAM limit.

| Backlog | avg body | broker RAM @ 0.4 watermark |
|--:|--:|--:|
| 100 000 | 2 KB | ≥ ~0.5 GB |
| 500 000 | 2 KB | ≥ ~2.5 GB |
| 1 000 000 | 2 KB | ≥ ~5 GB (prefer quorum/lazy queues) |

For large backlogs use **quorum/lazy queues** (page to disk, bounded memory) — the engine declares classic durable
queues today; note this as the scale-up path. Set `disk_free_limit`; budget for `.dlq` depth.

## KEDA

See [`deploy/k8s/keda-scaledobject.yaml`](../deploy/k8s/keda-scaledobject.yaml). Tune `value` (target ready messages
per replica) to worker capacity (a replica drains ≈ `ConsumerDispatchConcurrency / avg-job-seconds` jobs/s). Keep
`PrefetchCount` modest so few messages requeue on scale-down; idempotent handlers + `UniqueKey` cover the requeue
duplicate case; generous `terminationGracePeriodSeconds` for in-flight acks.

## Payload & result rules

- **Payloads = references, not blobs.** Body = payload JSON; map/reduce stores each item's JSON in Redis *and* its
  result → ~2× amplification. Keep items < ~4–32 KB (avoid > 128 KB); externalize big inputs to blob/DB + pass a key.
- **Results stay tiny.** No result path back to the producer; the reducer persists a compact outcome (counts/refs) to
  DB/blob — never inflate the Redis results hash.
- **Correctness:** idempotent handlers, `UniqueKey` dedup, `MaxRetryAttempts` + DLQ. RabbitMQ keeps no status ledger —
  use progress + metrics, not `GET /api/platform/jobs/{id}`.

## Workload profiles

| # | Profile | Isolates | Harness |
|---|---|---|---|
| P1 | Fire-and-forget burst (10k tiny) | enqueue throughput + drain | `burst` |
| P2 | Sustained throughput (X/s, N min) | steady-state CPU/mem/latency | `sustained` |
| P3 | IO-bound (delay, high concurrency) | prefetch/dispatch vs WorkerCount | `io` |
| P4 | CPU-bound (hash loop) | threadpool/GC/CPU per job | `cpu` |
| P5 | Map/reduce fan-out (width 100–5000) | fan-out + Redis growth + reduce | `mapreduce` |
| P6 | Large payload (sweep size) | broker/Redis memory & latency vs size | `largepayload` |
| P7 | Scale test (cloud) | KEDA 0→N latency, drain, cost | manual + `kubectl` |

## Local vs cloud

- **Local (dev env, manually-run RabbitMQ+Redis):** relative throughput/latency at fixed concurrency; per-process
  CPU/mem; Redis & RabbitMQ memory growth vs payload/width (validate the tables above); knob tuning. *Not* reliable:
  absolute numbers, autoscaling, multi-node.
- **Cloud (AKS + KEDA):** horizontal scaling throughput; KEDA scale 0→N + scale-to-zero; producer/worker split;
  managed-service latency; cost per million jobs; DLQ/failover under load.

## Verify

Fill [`benchmark-results-template.md`](./benchmark-results-template.md) from the App Insights KQL + NBomber reports,
run the profiles once per engine (`VC_BENCH_ENGINE` set), and roll the headline numbers into the deck's scorecard.
