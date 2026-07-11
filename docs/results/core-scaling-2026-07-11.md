# Core-scaling comparison — Hangfire vs RabbitMQ (local, 2026-07-11)

Simulating pod CPU limits via `DOTNET_PROCESSOR_COUNT` (drives .NET pool/GC sizing + both engines' auto-scaling) + processor affinity (physical cap). Both engines self-size to the core count: RabbitMQ with `PrefetchCount=0` (the default → auto-scales to cores×`ConcurrencyPerCore`=10); Hangfire native `WorkerCount = cores×5` (**must leave WorkerCount unset**). Clean drain measured via map/reduce (fast server-side load).

## Matrix (map/reduce 2000, IO 50ms; sustained 200/s ×2min)

| Cores | Engine | Effective concurrency | map/reduce drain | CPU peak (core) | WS peak | sustained 200/s |
|---|---|--:|--:|--:|--:|---|
| **1** | RabbitMQ | prefetch 10 | **141 j/s** (14.1s) | 0.70 | 231 MB | held, CPU avg 0.31 |
| **1** | Hangfire | workers 5 | **55 j/s** (36.6s) | 0.69 | 247 MB | held, CPU avg 0.56 (peak 1.06, TP queue 43) |
| **2** | RabbitMQ | prefetch 20 | **282 j/s** (7.1s) | 1.23 | 274 MB | held, CPU avg 0.46 (peak 1.38) |
| **2** | Hangfire | workers 10 | **109 j/s** (18.3s) | 1.73 | 300 MB | held, CPU avg 0.95 (peak 1.89, TP queue 20) |
| **0.5** | both | — | not run | | | |

> The 0.5-core row was not run. It requires a Windows Job Object hard CPU-rate cap (affinity can't express fractions);
> the tooling is prepared (`scratchpad/cap-half-core.ps1`: `DOTNET_PROCESSOR_COUNT=1` to match K8s `ceil(0.5)=1`, plus
> a hard cap at 0.5-of-one-core of total capacity) and can be run later. Expectation from the 1↔2 linear trend:
> ~half the 1-core throughput (RabbitMQ ~70 j/s, Hangfire ~27 j/s map/reduce), and 200/s sustained would exceed a
> 0.5-vCPU pod's capacity for Hangfire (backlog growth) while RabbitMQ would be close to its ceiling.

*(16-core reference from the earlier round: RabbitMQ map/reduce 495 j/s at prefetch 64; Hangfire 220 j/s.)*

## 1-core read (both engines pinned: `DOTNET_PROCESSOR_COUNT=1` + affinity to 1 core)

At the pod-realistic 1-core limit RabbitMQ is decisively ahead:

- **Map/reduce drain: RabbitMQ 141 j/s vs Hangfire 55 j/s (2.6×)** at effectively identical peak CPU (0.70 vs 0.69 core) and memory (231 vs 247 MB). Hangfire's map fan-out round-trips every child job through SQL; RabbitMQ's prefetch-10 consumer drains the broker with far less per-job overhead.
- **Sustained 200/s: both hold zero failures, but the CPU cost diverges.** RabbitMQ holds it at avg **0.31 core** with headroom. Hangfire needs avg **0.56 core** and *peaks at 1.06* — momentarily saturating the whole core — with the threadpool queue climbing to 43 (SQL-poll backpressure). At 1 core, Hangfire is ~1.8× more CPU per job and has little margin before it falls behind; RabbitMQ has roughly half the core still free.

Takeaway for Virto Cloud 1-vCPU pods: RabbitMQ sustains ~2× the safe throughput per pod and leaves headroom for scale decisions; Hangfire at 1 vCPU is already near its ceiling at 200/s.

## 2-core read (both engines: `DOTNET_PROCESSOR_COUNT=2` + affinity to 2 cores)

Both engines scale ~linearly from 1→2 cores, and the RabbitMQ lead holds:

- **Map/reduce: RabbitMQ 141→282 j/s (2.0×, prefetch 10→20); Hangfire 55→109 j/s (2.0×, workers 5→10).** Both double as expected. At 2 cores RabbitMQ still drains **2.6×** faster than Hangfire (282 vs 109) — and does it at *lower* peak CPU (1.23 vs 1.73 core) and **¼ the allocation rate** (14 vs 58 MB/s), so much less GC pressure.
- **Sustained 200/s: RabbitMQ avg 0.46 core vs Hangfire avg 0.95 core** — Hangfire uses ~2× the CPU for the same load, same as the 1-core ratio. RabbitMQ's threadpool queue stays flat (7); Hangfire's climbs to 20.

The per-core efficiency gap is structural (SQL round-trips + more allocation per job on Hangfire), not a tuning artifact — it's consistent at both 1 and 2 cores.
