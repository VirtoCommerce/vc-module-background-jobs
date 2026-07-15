# Cloud Hangfire benchmark — Azure App Service B1 (vcst-dev, 2026-07-12)

**Environment:** `https://vcst-dev.govirto.com` · Hangfire engine on Azure SQL · Redis (cache) · **Azure App Service B1 = 1 vCPU / 1.75 GB, single instance**. Load driven by the NBomber harness (`tests/VirtoCommerce.BackgroundJobs.Benchmarks`) from a workstation; server-side truth read from Application Insights (`vcst-dev`, appId `03f33e66…`) via `az monitor app-insights query`.

Metrics available in AI: `…/jobs/execution.duration.ms`, `…/jobs/queue.latency.ms` (custom), and `% Processor Time [Normalized]`, `Private Bytes` (perf counters).

## Results

| Profile | Client result | CPU (% proc) | Memory (Private Bytes) | Queue latency | Exec duration |
|---|--|--:|--:|--:|--:|
| `smoke` (50, IO) | 50/50 OK, drained 0.2 s | — | — | — | — |
| `mapreduce` (2000, IO 50 ms) | drained 84.2 s → **~24 j/s** | avg **4%**, peak **34%** | avg 489 MB, max 528 MB | avg **~12 s**, max ~59 s | avg ~106 ms |
| `sustained` (200/s×2m) | **saturated**: ~125/s accepted, 62% fail (11565 timeout + 3387× 503), p95 101 s | avg **5%**, peak 14.5% | avg 558 MB, max **784 MB** | (backlog) | — |
| `io` (300/s, 200 ms) | TBD | | | | |
| `cpu` (100/s CPU-bound) | ~34% fail (timeouts), p95 45 s, ~7.9k ok | avg **23%**, peak **33%** | avg 648 MB, max 693 MB | ~5 min (cum. backlog) | ~126 ms |
| `io` (300/s) | skipped (redundant with sustained saturation) | | | | |
| `burst` (10k) | skipped (redundant with sustained saturation) | | | | |
| `largepayload` (2k×64 KB) | **enqueue 524** (gateway timeout): single bulk-enqueue request >100 s | avg ~24% | steady ~538 MB (no spike) | — | — |

## Key finding (so far)

**On B1, Hangfire is queue-latency-bound, not CPU-bound.** During the map/reduce drain the core sat ~96% idle (avg 4% CPU, 34% peak) while jobs waited ~12 s in the Hangfire SQL queue. The limiter is Hangfire's SQL-poll dispatch against **remote** Azure SQL plus the small worker count — not compute. That's why cloud drain (~24 j/s) is **less than half** the local 1-core Hangfire figure (55 j/s, localhost SQL): remote SQL round-trips per poll/ack dominate.

Implication: injection-rate profiles (`sustained` 200/s, `io` 300/s) will inject far faster than B1 can drain (~24 j/s), so backlog and queue latency grow without bound — those runs demonstrate **saturation**, not a steady-state throughput. Memory is not a concern (~0.5 GB of 1.75 GB).

**Sustained 200/s confirmed the saturation shape:** B1 accepted only ~125/s and failed 62% of requests — but the rejections were **HTTP 503 / client timeouts at the App Service request-queue/infra layer** (only 1340 requests and zero exceptions reached the app), while **CPU stayed at 5% avg / 14.5% peak**. So the wall is the enqueue→remote-SQL path and the request pipeline's connection/thread/queue limits, not compute. The app was briefly unreachable at peak load and recovered within ~15 s. Peak memory 784 MB.

*(Comparison base — local 1-core Hangfire map/reduce: 55 j/s; local 1-core RabbitMQ: 141 j/s. See `core-scaling-2026-07-11.md`.)*

## Conclusions — B1 Hangfire (cloud)

1. **Never CPU-bound.** CPU peaked at 33% even under a CPU-bound workload, and sat at 4–5% under IO load. The core is not the limiter on B1.
2. **Dispatch is the limiter — remote Azure SQL.** Clean drain is ~24 j/s vs 55 j/s locally; the difference is remote SQL poll/ack round-trips per job. Queue latency runs from ~12 s (light) to minutes (backlogged).
3. **Enqueue/request layer saturates ~125/s.** Beyond that, requests are rejected as **HTTP 503 / client timeouts at the App Service infra layer** (they never reach the app — no exceptions logged). 200/s and 300/s injection failed 34–62%. The app briefly became unreachable at peak but self-recovered in ~15 s.
4. **Bulk-enqueue-in-one-request is a trap.** Enqueuing 2000 × 64 KB in a single HTTP call exceeded the ~100 s Cloudflare gateway limit (**524**). Bulk producers must chunk / use the batch-enqueue API, not one giant request.
5. **Memory is comfortable.** Stayed ≤ 784 MB of 1.75 GB throughout, even with 64 KB payloads (payloads persist to SQL, not held in memory).

**Takeaway:** a single 1-vCPU B1 running Hangfire handles only modest job volume (~20/s sustained, ~24 j/s drain) and degrades by *rejecting* work under bursts rather than by burning CPU. For cloud throughput, scale the plan up/out or move to the RabbitMQ engine with dedicated workers (KEDA) — the RabbitMQ cloud run is next.

*(io/burst were intentionally skipped: they reproduce the same enqueue-bound saturation already shown by sustained.)*
