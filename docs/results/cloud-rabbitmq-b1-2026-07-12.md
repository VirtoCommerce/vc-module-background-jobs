# Cloud RabbitMQ benchmark — Azure App Service B1 (vcst-dev, 2026-07-12)

**Environment:** `https://vcst-dev.govirto.com` · **RabbitMQ engine** (CloudAMQP over `amqps`, prefetch auto-scaled to `ProcessorCount×10` = 10 on B1) · Redis for map/reduce state · **B1 = 1 vCPU / 1.75 GB, single instance**. Same NBomber harness + App Insights (`az`) as the Hangfire run. Engine confirmed via `/health`: *Active background-job engine 'RabbitMQ'*.

## Results

| Profile | Client result | CPU (% proc) | Memory (Private Bytes) | Queue latency | Exec duration |
|---|--|--:|--:|--:|--:|
| validate (10, IO) | 10/10 OK | — | — | — | — |
| `mapreduce` (2000, IO 50 ms) | drained 15.3 s → **~131 j/s** | avg **2.7%**, peak **7.6%** | avg 289 MB, max 300 MB | avg **~5 s**, max ~9.5 s | avg 68 ms |
| `sustained` (200/s×2m) | **0.3% fail** (68/24000), p95 ~700 ms, mean 377 ms | avg **1%**, peak 1.4% | ~321 MB | (drain backlog; AI sampled) | — |
| `cpu` (100/s) | TBD | | | | |
| `largepayload` (2k×64 KB) | TBD | | | | |

## Head-to-head vs Hangfire on the SAME B1 instance

| Metric (mapreduce 2000) | Hangfire | RabbitMQ | RabbitMQ advantage |
|---|--:|--:|--|
| Drain throughput | 24 j/s | **131 j/s** | **~5.5× faster** |
| CPU peak | 34% | 7.6% | far lower |
| Memory | 489 MB | 289 MB | ~40% less |
| Queue latency (avg) | 12 s | 5 s | lower |
| Exec duration (avg) | 106 ms | 68 ms | lower |

**Why:** Hangfire round-trips every job through **remote Azure SQL** (poll + ack), which dominates on cloud; RabbitMQ streams from the broker with prefetch 10 and uses Redis (1 ms) for map/reduce state — no per-job SQL. On cloud B1, RabbitMQ nearly matches its *local* 1-core figure (141 j/s), whereas Hangfire fell from 55 → 24. *(Local baselines: `core-scaling-2026-07-11.md`.)*
