# Background-Jobs Benchmark Harness (NBomber)

A manual load driver for the Hangfire-vs-RabbitMQ comparison. It hits the sample module's benchmark endpoints on a
**running Producer**; the canonical CPU / memory / throughput / latency numbers come from **Application Insights**
(the platform's AI module) — this harness generates the load and reports client-observed rate/latency and drain time.

> Not part of the module solution or CI. Run it explicitly against a deployment you want to measure.

## Prerequisites

- A running platform with the **Background Jobs** module + the **Sample** module, and the **Application Insights**
  module configured (`APPLICATIONINSIGHTS_CONNECTION_STRING`).
- A worker draining jobs (`VirtoCommerce:BackgroundJobs:Mode = Worker` or `Both`).
- A **bearer token** for the `[Authorize]` endpoints (obtain from the platform's token endpoint / an API key).

## Configure

```
VC_BENCH_BASEURL   e.g. https://localhost:5001      (default: https://localhost:5001)
VC_BENCH_TOKEN     bearer token                     (required unless the API is open)
VC_BENCH_ENGINE    report label, e.g. hangfire | rabbitmq
```

## Run

```bash
dotnet run -c Release -- <profile>
```

| profile | what it does | maps to |
|---|---|---|
| `smoke` | 50 tiny jobs, poll to drain (connectivity check / CI) | — |
| `burst` | enqueue 10k jobs, measure drain throughput | P1 |
| `sustained` | inject 200 jobs/s for 2 min (IO 50 ms) | P2 |
| `io` | inject 300/s, IO 200 ms | P3 |
| `cpu` | inject 100/s, CPU-bound | P4 |
| `mapreduce` | one 2 000-item map/reduce batch, drain | P5 |
| `largepayload` | 2 000 jobs with 64 KB payloads | P6 |

Run the **same profile twice** — once with the platform on `Provider=Hangfire`, once on `RabbitMQ` — and set
`VC_BENCH_ENGINE` accordingly so the NBomber report filenames and the App Insights `engine` dimension line up. Read the
server-side comparison (CPU/1k, memory, queue latency) from Application Insights (`docs/benchmark-kql.md`) and the
client-side rate/latency from the NBomber HTML report under `./reports`.

## Notes

- Handlers are async, idempotent, and tunable (`delayMs`, `cpu`, `payloadBytes`/`resultBytes`, `failPct`) so the
  **engine is the only variable** between runs.
- `smoke` runs against in-memory Hangfire with no external dependency — safe for a CI connectivity gate.
- Absolute numbers are only meaningful from a cloud deployment on representative nodes; locally this measures
  *relative* behavior and validates the sizing formulas.
