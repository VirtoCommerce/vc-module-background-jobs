# Background Jobs Module

[![CI status](https://github.com/VirtoCommerce/vc-module-background-jobs/workflows/Module%20CI/badge.svg?branch=dev)](https://github.com/VirtoCommerce/vc-module-background-jobs/actions?query=workflow%3A%22Module+CI%22)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=VirtoCommerce_vc-module-background-jobs&metric=alert_status&branch=dev)](https://sonarcloud.io/dashboard?id=VirtoCommerce_vc-module-background-jobs)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=VirtoCommerce_vc-module-background-jobs&metric=reliability_rating&branch=dev)](https://sonarcloud.io/dashboard?id=VirtoCommerce_vc-module-background-jobs)

## Overview

The Background Jobs module is the Virto Commerce **background-processing engine**. The platform itself no longer
depends on Hangfire directly — it depends only on a small, engine-agnostic abstraction in
`VirtoCommerce.Platform.Core`, and this module provides the actual engine that runs the work.

A module (or the platform) defines a serializable **payload** and an `IBackgroundJobHandler<TPayload>` **handler**, then
enqueues it through the `IBackgroundJob` facade. Exactly **one engine per platform instance** — chosen by
configuration — executes it: **Hangfire** (the default) or **RabbitMQ**. The same enqueue
code runs unchanged on any engine, with or without live progress reported to the admin UI.

This module replaces the platform's previously built-in Hangfire integration **without breaking existing
modules**: it ships a type-forwarding `VirtoCommerce.Platform.Hangfire.dll`, so modules that referenced the old
platform Hangfire assembly keep working once this module is installed.

## Key Features

* **Engine-agnostic job API** — define a payload + `IBackgroundJobHandler<TPayload>` handler and enqueue via the
  `IBackgroundJob` facade. Consumer modules reference only `VirtoCommerce.Platform.Core`.
* **Pluggable engines behind one port** (`IJobEngine`) — Hangfire and RabbitMQ ship in the box; selected by a
  single configuration key, like the search providers.
* **Fire-and-forget with or without progress** — opt into live progress streamed to the admin notification UI over
  SignalR.
* **Map/reduce (fan-out → aggregate)** — split big work into N map tasks that run in parallel across all workers, then
  a single reduce; engine-agnostic and fleet-safe. Contracts ship in `VirtoCommerce.Platform.Core`.
* **Instance modes** — the same container image runs as `Producer` (enqueue only), `Worker` (process only) or
  `Both`, controlled by configuration.
* **No breaking changes** — a type-forwarding shim keeps existing `VirtoCommerce.Platform.Hangfire.*` consumers
  working; the Hangfire storage, dashboard, queues, retry and recurring jobs are unchanged.
* **Extensible by partners** — override a handler via DI (last registration wins) and extend a payload via
  `AbstractTypeFactory`, using the same tools partners already use across Virto.
* **Static enqueue for migration** — a Hangfire-style `BackgroundJob.Enqueue<THandler>(payload)` static entry point
  (in `VirtoCommerce.Platform.Core`) so code moving off `Hangfire.BackgroundJob.Enqueue` doesn't have to inject
  `IBackgroundJob`; new code should still prefer the injected facade.
* **Admin & integration REST API** — list registered handlers and recurring schedules for troubleshooting, and
  trigger a registered job by name, each under the module's own permission.
* **Application Insights telemetry** — every job on every engine emits pre-aggregated duration/queue-latency metrics
  and a `JobCompleted` drill-down event from the shared dispatch path; inert unless the platform's AI module is
  installed (see [Telemetry](#telemetry-application-insights)).
* **Graceful when absent** — if no engine module is installed, the platform boots and surfaces an actionable
  "install via the Virto Commerce CLI" message instead of silently dropping work.

## Configuration

Background processing is configured under `VirtoCommerce:BackgroundJobs`. Provider-specific options keep their own
sections (`VirtoCommerce:Hangfire`, `VirtoCommerce:RabbitMQ`), so the existing Hangfire configuration is untouched.

```jsonc
"VirtoCommerce": {
  "BackgroundJobs": {
    "Provider": "Hangfire",     // Hangfire | RabbitMQ | InMemory  (the ACTIVE engine behind IBackgroundJob / map-reduce / platform recurring)
    "Mode": "Both",              // Producer | Worker | Both
    "EnableLegacyHangfire": true, // keep Hangfire initialized for legacy direct-Hangfire modules even under another engine (see below)
    "DefaultQueue": "default",
    "MaxRetryAttempts": 3
  },
  "Hangfire": {
    "WorkerCount": 20,         // concurrent jobs this instance processes (default ~ProcessorCount * 5)
    /* plus existing Hangfire options — storage, dashboard, queues */
  },
  "RabbitMQ": {
    "HostName": "localhost", "Port": 5672, "UserName": "guest", "Password": "guest", "VirtualHost": "/",
    // "Uri": "amqp://guest:guest@localhost:5672/",  // alternative to the host/port/credential fields above
    "PrefetchCount": 0,        // unacked prefetch (QoS) + throughput gate; 0 (default) auto-scales to ProcessorCount x ConcurrencyPerCore, a positive value pins it
    // "ConsumerDispatchConcurrency": 0,   // handlers run in parallel per instance; 0 (default) follows the effective PrefetchCount
    // "ConcurrencyPerCore": 10,           // target concurrent handlers per CPU when auto-scaling; ~1-2 for CPU-bound work
    "Queues": [],              // extra queues the consumer drains besides BackgroundJobs.DefaultQueue
    "UseDeadLetterQueue": true,    // route retry-exhausted jobs to "{queue}.dlq" instead of dropping them
    "DeadLetterQueueSuffix": ".dlq"
  }
}
```

When `Provider` is `RabbitMQ`, the engine publishes each job as a persistent message to a durable queue; an
in-process consumer (running when `Mode` is `Worker`/`Both`) drains it and dispatches the handler, retrying a
failed job by re-publishing with an incremented attempt up to `MaxRetryAttempts`. Once retries are exhausted the
job is routed to a dead-letter queue (`{queue}.dlq`, carrying `x-original-queue`/`x-attempts`/`x-death-reason`
headers) for inspection or replay — or dropped if `UseDeadLetterQueue` is `false`. RabbitMQ keeps no job ledger, so
`GET api/platform/jobs/{id}` treats every id as unknown (reported as a completed job, so status pollers stop rather
than hang) and job deletion is unsupported — observe jobs via progress notifications instead.

When `Provider` is `InMemory`, jobs run **in-process** with no SQL, broker or Redis — the enqueue dispatches the job
on a background task (retrying up to `MaxRetryAttempts`) and keeps per-job state for status/delete. It is meant for
**local development and testing only**: it is non-durable (jobs are lost on restart) and single-process (`Mode` is not
honored and it cannot scale across instances). Pair it with `BackgroundJobs:EnableLegacyHangfire = false` for a
truly infra-free run. It is certified by the same engine conformance suite as Hangfire and RabbitMQ.

**Recurring jobs** work on **any** engine. A recurring job is an ordinary `IBackgroundJobHandler<TPayload>` plus a
schedule declared with `AddRecurringJob` (see Usage). On Hangfire they use Hangfire's native recurring scheduler
(persisted, shown in the dashboard); on RabbitMQ (or any non-Hangfire engine) an in-process cron scheduler fires
each occurrence and enqueues the payload, with fleet-safe exactly-once firing via a distributed lock + a shared
occurrence marker (Redis when configured, in-memory for a single instance). The legacy expression-based
`IRecurringJobService` remains Hangfire-only for backward compatibility. If no engine module is installed, the
platform still boots and logs a warning that recurring jobs are not scheduled.

#### RabbitMQ primary with legacy Hangfire (`EnableLegacyHangfire`)

`Provider` selects the **active engine** — the one behind `IBackgroundJob`, map/reduce, and the platform's own
recurring jobs. Separately, **`EnableLegacyHangfire` (default `true`) keeps Hangfire fully initialized** (storage,
`IBackgroundJobClient`/`IRecurringJobManager`, the processing server on `Worker`/`Both`, the `/hangfire` dashboard and
schema) **even when `Provider` is not Hangfire**. This lets you make RabbitMQ primary for new work while modules that
still call the Hangfire API directly (`BackgroundJob.Enqueue`, `RecurringJob.AddOrUpdate`, `IBackgroundJobClient`, or
the legacy `VirtoCommerce.Platform.Hangfire.IRecurringJobService`) keep working — the two run side by side over disjoint
stores (RabbitMQ queue vs Hangfire SQL). New modules should use the engine-agnostic `IBackgroundJob` /
`IBackgroundJobHandler<T>`, which ride the active engine.

- **RabbitMQ-primary, legacy supported:** `Provider = RabbitMQ` (leave `EnableLegacyHangfire = true`). Requires a
  Hangfire store — `VirtoCommerce:Hangfire:JobStorageType` defaults to `Memory`; set `Database`/`SqlServer` for durable
  legacy jobs.
- **Pure RabbitMQ (no Hangfire):** `Provider = RabbitMQ`, `EnableLegacyHangfire = false` — no Hangfire server,
  dashboard, or schema.
- **Upgrade note:** because it defaults to `true`, an existing RabbitMQ-only instance will begin bootstrapping Hangfire
  after upgrade (server on `Worker`/`Both`, `/hangfire`, and SQL schema if `JobStorageType = Database`). Set
  `EnableLegacyHangfire = false` to keep it Hangfire-free.

### Application Settings

This module exposes **no platform settings**. All background-job configuration — `Provider`, `Mode`, `DefaultQueue`,
`MaxRetryAttempts`, and the provider-specific sections — is a deployment-time concern configured **only** in
`appsettings.json` (`VirtoCommerce:BackgroundJobs`, `VirtoCommerce:Hangfire`, `VirtoCommerce:RabbitMQ`) and read at
startup. These are ops decisions, not admin-UI toggles.

### Permissions

| Permission | Description |
|---|---|
| `platform:background:jobs:manage` | View the Hangfire dashboard (`/hangfire`) and the jobs status API. |
| `background-jobs:read` | List registered handlers and recurring schedules via the admin API. |
| `background-jobs:execute` | Trigger a registered background job by name via the admin API. |

## Architecture

Contracts live in the platform; engine internals live in this module.

```
Module / Platform code
   │  IBackgroundJob.Enqueue(payload, options)        ← VirtoCommerce.Platform.Core.Jobs
   ▼
JobEngineBackgroundJob  ── builds JobEnvelope (serializes payload) ──►  IJobEngine (one active)
                                                                          │
                                  ┌───────────────────────────────────────┴───────────────┐
                                  ▼                                                         ▼
                          HangfireJobEngine                                        RabbitMqJobEngine
                                  │  enqueues a job whose body calls…                       │  publishes the envelope;
                                  ▼                                                         ▼  an in-process consumer…
                          IJobDispatcher.Dispatch(envelope) ──► resolves IBackgroundJobHandler<TPayload> from DI ──► Execute(...)
                                                                                            │
                                                                 progress ──► IJobProgress ──► SignalR ──► admin UI
```

* **Producer/Worker/Both** decides whether this instance runs the processing host (Hangfire server / RabbitMQ
  consumer) — so you can scale producers and workers independently from one image.
* **Progress** is engine-independent: handlers call `context.Progress.Report(...)`, surfaced to the admin
  notification UI over SignalR.

## Telemetry (Application Insights)

Every job — on **every** engine — emits telemetry from the shared dispatch path (`DefaultJobDispatcher`), so Hangfire,
RabbitMQ and In-Memory are measured identically. It is **opt-in by presence**: `JobTelemetry` takes an optional
`TelemetryClient`, so when the platform's **Application Insights** module isn't installed it resolves to `null` and
every call is a no-op — no behavior, no cost. (The AI package is a compile-time type reference on
`VirtoCommerce.BackgroundJobs.Data`; nothing is sent unless AI is configured.)

**Pre-aggregated metrics** (`customMetrics`), dimensioned by the low-cardinality `engine` / `handler` / `outcome`
(`success` \| `canceled` \| `failure`) — sampling-immune and safe for the per-series cap:

| Metric | Meaning |
|---|---|
| `VirtoCommerce.BackgroundJobs/jobs/execution.duration.ms` | Handler run time. |
| `VirtoCommerce.BackgroundJobs/jobs/queue.latency.ms` | Enqueue → dispatch delay (omitted when the enqueue-time header is absent, e.g. a redelivered legacy message). |

**Drill-down event** (`customEvents`): `JobCompleted` — properties `engine`, `handler`, `outcome`, `runId`; metrics
`executionMs`, `queueLatencyMs`. The high-cardinality `runId` is carried on the event (subject to sampling) rather
than on the metric dimensions, so per-run analysis stays possible without blowing the metric series cap.

Ready-made Kusto queries (throughput, p95 duration, queue latency, failure rate — sliced by engine/handler) live in
[`docs/benchmark-kql.md`](docs/benchmark-kql.md).

## Components

### Projects

| Project | Layer | Purpose |
|---|---|---|
| `VirtoCommerce.BackgroundJobs.Core` | Core | **Contracts, models, options and reusable helpers only** — `IJobEngine`, `IJobDispatcher`, `IJobPayloadSerializer`, `IBackgroundJobsAdminQuery`, `IJobEnvelopeRunner`, `JobEnvelope`/models, progress, `JobExecutionContextFactory`, `JobJsonSettings`, `RecurringScheduleResolver`, plus the map/reduce + recurring orchestration. Published as a NuGet so client and custom-engine projects reference the port without the service implementations. |
| `VirtoCommerce.BackgroundJobs.Hangfire` | Engine | Hangfire implementation of `IJobEngine`; reuses the platform's former Hangfire storage/dashboard. Published as a NuGet. |
| `VirtoCommerce.Platform.Hangfire.Shim` | Compat | Produces a type-forwarding `VirtoCommerce.Platform.Hangfire.dll` for binary compatibility with existing modules. |
| `VirtoCommerce.BackgroundJobs.RabbitMQ` | Engine | RabbitMQ implementation of `IJobEngine` + the in-process consumer (`RabbitMqJobConsumer`). Published as a NuGet. |
| `VirtoCommerce.BackgroundJobs.Web` | Web | Module host: `PlatformStartup` (engine/mode selection), `JobsController`, settings & permissions. |
| `VirtoCommerce.BackgroundJobs.Data` | Data | The concrete engine-agnostic **service implementations** (`JobEngineBackgroundJob` facade, `DefaultJobDispatcher`, `JsonJobPayloadSerializer`, `JobTelemetry`, `JobEnvelopeRunner`, `BackgroundJobsAdminQuery`) + the reusable in-process recurring scheduler (`AddInProcessRecurringScheduler`) and the Redis/in-memory occurrence-marker & map/reduce batch stores. Published as a NuGet. |

### Key Services

| Service | Interface | Responsibility |
|---|---|---|
| Enqueue facade | `IBackgroundJob` | Engine-agnostic, handler-explicit enqueue (`Enqueue<THandler>(payload)`; also a non-generic `Enqueue(Type, payload)` for name/type-addressed triggering). |
| Static enqueue | `BackgroundJob` (static) | Hangfire-style `BackgroundJob.Enqueue<THandler>(payload)` migration helper; opens a scope and delegates to the scoped `IBackgroundJob`. |
| Job handler | `IBackgroundJobHandler<TPayload>` | Your job logic; resolved from DI, overridable. |
| Admin read model | `IBackgroundJobsAdminQuery` | Lists registered handlers + recurring schedules (effective cron, last/next run) for the admin API. |
| Envelope runner | `IJobEnvelopeRunner` | Runs a pushed `JobEnvelope` in-process (build context → dispatch); reused by push engines (Google Cloud Tasks). |
| Map/reduce facade | `IMapReduceJob` | Fan-out → aggregate jobs (`Enqueue<TMap, TReduce>(items, state)`); contracts ship in `VirtoCommerce.Platform.Core`. |
| Engine port | `IJobEngine` | The active engine (Hangfire/RabbitMQ). One per instance. |
| Dispatcher | `IJobDispatcher` | Shared execution path: deserialize → resolve handler → run. |
| Progress | `IJobProgress` | Reports progress to the admin UI (SignalR). |
| Recurring registration | `AddRecurringJob<THandler,TPayload>` | Declare a handler + cron/setting-driven schedule (engine-agnostic). |
| Recurring scheduler port | `IRecurringJobScheduler` | Engine impl that schedules recurring jobs (Hangfire-native / in-process cron); NoEngine fallback warns. |
| Legacy recurring | `IRecurringJobService` | Expression-based cron registration (Hangfire only; back-compat). |

### REST API

| Method | Endpoint | Description |
|---|---|---|
| GET | `api/platform/jobs/{id}` | Get the status of a background job (engine-agnostic). Requires `background_jobs:manage`. |
| GET | `api/background-jobs/registered` | List registered handlers (name, handler type, payload type). Requires `background-jobs:read`. |
| GET | `api/background-jobs/recurring` | List recurring jobs with effective cron, enabled, last/next run. Requires `background-jobs:read`. |
| POST | `api/background-jobs/enqueue` | Trigger a registered job by name — body `{ name, payload, options? }` → engine job id. Requires `background-jobs:execute`. |

## Usage

```csharp
// 1) Payload — created via AbstractTypeFactory so partners can extend it.
public class SendOrderEmailPayload : ValueObject
{
    public string OrderId { get; set; }
    public string CustomerEmail { get; set; }
}

// 2) Handler.
public class SendOrderEmailJob(IEmailSender sender) : IBackgroundJobHandler<SendOrderEmailPayload>
{
    public async Task Execute(SendOrderEmailPayload payload, IJobExecutionContext context, CancellationToken cancellationToken = default)
    {
        await context.Progress.Report(new() { Message = "Sending…", TotalCount = 1 }, cancellationToken);
        await sender.Send(payload.CustomerEmail, payload.OrderId, cancellationToken);
    }
}

// 3) Register in the module's Initialize(IServiceCollection). The payload type is inferred from the handler;
//    use AddBackgroundJob<THandler, TPayload>() if you prefer to state it explicitly.
services.AddBackgroundJob<SendOrderEmailJob>();

// 4) Enqueue (engine-agnostic). Prefer the HANDLER-EXPLICIT form: the call site names the action that will run,
//    so an enqueue is self-documenting in review.
var payload = AbstractTypeFactory<SendOrderEmailPayload>.TryCreateInstance();
payload.OrderId = order.Id;
payload.CustomerEmail = order.Email;

await jobs.Enqueue<SendOrderEmailJob>(payload);                                            // fire-and-forget
await jobs.Enqueue<SendOrderEmailJob>(payload, new EnqueueOptions { ReportProgress = true }); // with progress
```

**One payload, several handlers.** Because the handler is chosen at enqueue time, the same payload type can drive
different actions — register each handler and enqueue to the one you want:

```csharp
services.AddBackgroundJob<SendOrderEmailJob>();      // both handle SendOrderEmailPayload
services.AddBackgroundJob<ArchiveOrderJob>();

await jobs.Enqueue<SendOrderEmailJob>(payload);      // emails the customer
await jobs.Enqueue<ArchiveOrderJob>(payload);        // archives the order — same payload, different action
```

The enqueue validates at enqueue time that the handler actually handles the payload (throwing otherwise), so a
mismatched handler/payload fails fast at the call site rather than on a worker.

### Enqueue options — what to set and why

`Enqueue<THandler>(payload)` with no options is the common case: the job runs as soon as a worker is free, on the
default queue. Pass `EnqueueOptions` only when you need one of these behaviors:

| Option | Why you'd set it | Notes / engine support |
|---|---|---|
| `Queue` | **Isolate workloads.** Route heavy or slow jobs (catalog indexing, bulk export) to a dedicated queue/worker pool so they don't starve short interactive jobs — e.g. `Queue = "indexing"`. | Honored by **Hangfire** and **RabbitMQ** (the worker drains the configured queues). Google Cloud Tasks is single-queue and ignores it. Falls back to `VirtoCommerce:BackgroundJobs:DefaultQueue` when unset. |
| `ReportProgress` | **The user is watching.** For long jobs, stream `context.Progress.Report(...)` to the admin UI over SignalR (progress bar + log). Skip it for fire-and-forget work to avoid needless notifications. | The engine creates a progress push-notification and returns its id via the notification stream. Works on every engine. |
| `ProgressNotificationId` | **Report into an existing notification** instead of a new one — e.g. a parent operation, or one shared bar across several jobs (this is how map/reduce shows an aggregate bar). | When set you don't also need `ReportProgress`; updates flow to that id. |
| `UniqueKey` | **De-duplicate re-enqueues.** Collapse repeated triggers of the "same" work (a debounce/idempotency key) into a single job. | Honored where the engine supports dedup — **Google Cloud Tasks** (task-name dedup). Hangfire and RabbitMQ currently ignore it. |

**Retries** are configured globally, not per enqueue: `VirtoCommerce:BackgroundJobs:MaxRetryAttempts` governs how many
times a failed job is retried before being dead-lettered/dropped (RabbitMQ); Hangfire uses its own retry filter.
(`EnqueueOptions.MaxRetryAttempts` is reserved for a future per-job override and is not yet honored.)

### Static enqueue (Hangfire migration)

Code migrating off the static `Hangfire.BackgroundJob.Enqueue(...)` API can enqueue **without injecting**
`IBackgroundJob`, via the static `BackgroundJob` facade in `VirtoCommerce.Platform.Core`:

```csharp
using VirtoCommerce.Platform.Core.Jobs;

await BackgroundJob.Enqueue<SendOrderEmailJob>(payload);
await BackgroundJob.Enqueue<SendOrderEmailJob>(payload, new EnqueueOptions { ReportProgress = true });
```

Each call opens a short-lived DI scope and delegates to the scoped `IBackgroundJob` (the current user still flows in
via `IHttpContextAccessor`), so behavior matches the injected facade. It is a **migration aid** — prefer injecting
`IBackgroundJob` in new code (explicit dependency, easily testable). If the BackgroundJobs module isn't installed the
call throws an actionable error.

### Admin & integration API

Engine-agnostic REST endpoints for troubleshooting and integration, each under the module's own permission:

| Method | Endpoint | Permission | Description |
|---|---|---|---|
| GET | `api/background-jobs/registered` | `background-jobs:read` | Registered handlers: name, handler type, payload type. |
| GET | `api/background-jobs/recurring` | `background-jobs:read` | Recurring jobs: effective cron, enabled, time zone, handler/payload, last & next run. |
| POST | `api/background-jobs/enqueue` | `background-jobs:execute` | Trigger a registered job **by name**. |

`POST api/background-jobs/enqueue` lets integration middleware start a job without a compile-time reference to it.
Only **registered** handlers are triggerable — addressed by the friendly `name` from the `registered` list, never a
raw type — and the payload JSON is bound to the handler's payload type:

```jsonc
POST /api/background-jobs/enqueue
{
  "name": "SendOrderEmailJob",        // the registered handler name (defaults to the handler's type name)
  "payload": { "orderId": "123", "customerEmail": "a@b.com" },
  "options": { "reportProgress": true } // optional EnqueueOptions
}
// → "a1b2c3d4"   (engine job id; poll via GET api/platform/jobs/{id})
```

An unknown name returns **404**; a payload that doesn't match the handler returns **400**. The addressable name
defaults to the handler's type name; pass `AddBackgroundJob<THandler, TPayload>(name: "my-name")` to set a custom one.

### Extending jobs (partner modules)

A partner module can **extend the payload** via `AbstractTypeFactory`. The message carries the *concrete* payload
type, so the extended instance survives serialization across the engine and arrives at the handler intact.

```csharp
// Partner module — Initialize(IServiceCollection):

// Extend the payload: derive and override the type so the factory builds the extended one.
public class CustomSendOrderEmailPayload : SendOrderEmailPayload
{
    public string Locale { get; set; }
}
AbstractTypeFactory<SendOrderEmailPayload>.OverrideType<SendOrderEmailPayload, CustomSendOrderEmailPayload>();

// The registered handler receives the extended instance.
public class SendOrderEmailJob(IEmailSender sender) : IBackgroundJobHandler<SendOrderEmailPayload>
{
    public async Task Execute(SendOrderEmailPayload payload, IJobExecutionContext context, CancellationToken ct = default)
    {
        var custom = (CustomSendOrderEmailPayload)payload;   // the extended instance is delivered here
        await sender.Send(custom.CustomerEmail, custom.OrderId, custom.Locale, ct);
    }
}

// Create via the factory (returns the extended type) and enqueue to the handler.
SendOrderEmailPayload payload = AbstractTypeFactory<SendOrderEmailPayload>.TryCreateInstance();
payload.OrderId = order.Id;
((CustomSendOrderEmailPayload)payload).Locale = "fr-FR";
await jobs.Enqueue<SendOrderEmailJob>(payload);   // the extended payload reaches the handler intact
```

> Because enqueue names the concrete handler (`Enqueue<THandler>`), a partner varies behavior by **registering its
> own handler and enqueuing to it** — not by overriding the platform handler via DI. (DI last-registration-wins
> override applied to the older payload-typed enqueue, which this facade no longer exposes.)

How it works: the enqueue facade records `PayloadType = payload.GetType()` (the concrete extended type) while the
handler is keyed on the base contract — so the dispatcher reconstructs the derived instance and hands it to the
named handler. See [`ExtensibilityTests`](tests/VirtoCommerce.BackgroundJobs.Tests/ExtensibilityTests.cs) for the
end-to-end proof.

### Recurring jobs

A recurring job is the same handler declared with a schedule — no recurring-specific contract. Works identically on
Hangfire and RabbitMQ.

```csharp
// Explicit cron (payload type inferred from the handler; use AddRecurringJob<THandler, TPayload>(...) to state it)
services.AddRecurringJob<SendDigestJob>(schedule => schedule
    .WithId("SendDigest")
    .WithCron("0 7 * * *")        // 5- or 6-field cron
    .WithQueue("maintenance"));   // optional

// With a parameterized payload — the simplest form: pass the configured payload directly (no factory).
services.AddRecurringJob<SendDigestJob, SendDigestPayload>(
    new SendDigestPayload { Top = 10, Period = "daily" },
    schedule => schedule.WithId("SendDigest").WithCron("0 7 * * *"));

// Need a fresh/dynamic value each run (e.g. a timestamp)? Use the factory overload — it runs once per occurrence
// (build via AbstractTypeFactory inside the factory to keep the payload partner-overridable).
services.AddRecurringJob<SendDigestJob, SendDigestPayload>(
    () => new SendDigestPayload { Top = 10, RunAtTicks = DateTime.UtcNow.Ticks },
    schedule => schedule.WithId("SendDigest").WithCron("0 7 * * *"));

// Setting-driven (enabler on/off + cron setting; re-applied live when either setting changes)
services.AddRecurringJob<PruneHandler, PrunePayload>(schedule => schedule
    .WithId("Prune")
    .FromSettings(EnablePruneSetting, CronPruneSetting));
```
On each occurrence the active engine runs the handler on a worker. The platform applies these declarations to the
active `IRecurringJobScheduler`; with no engine installed it logs a warning instead of failing.

A complete, runnable example lives in [`samples/VirtoCommerce.BackgroundJobs.SampleModule`](samples/VirtoCommerce.BackgroundJobs.SampleModule/README.md).

### Map/reduce (fan-out → aggregate)

For work that splits into many independent pieces — reindexing a catalog, bulk export/import, image processing — use
map/reduce instead of one long-running job. It fans out **N map tasks that run in parallel across all workers**, then
runs a single **reduce** task once every item finishes. It's engine-agnostic (works on Hangfire/RabbitMQ/any engine)
because the map, reduce, and coordination are ordinary message jobs; the join is fleet-safe via the same atomic
marker pattern as the recurring scheduler (Redis when configured, in-memory for a single instance).

The map/reduce contracts (`IMapJobHandler` / `IReduceJobHandler` / `IMapReduceJob` / `MapResult` / `AddMapReduceJob`)
live in **`VirtoCommerce.Platform.Core`** — the same package as `IBackgroundJob`. A consuming module defines and
enqueues map/reduce jobs with **no compile-time dependency on the Background Jobs module**; the runtime dependency on
the engine is declared in `module.manifest`.

```csharp
// 1) Map handler — runs once per item, in parallel, on any worker. Keep the result small.
public class IndexPageHandler(IIndexingManager indexer) : IMapJobHandler<IndexPage, IndexPageResult>
{
    public async Task<IndexPageResult> Map(IndexPage page, IJobExecutionContext ctx, CancellationToken ct)
    {
        var failed = await indexer.IndexDocuments(page.DocumentType, page.DocumentIds, ct);
        return new IndexPageResult(page.DocumentIds.Length - failed.Length, failed);
    }
}

// 2) Reduce handler — runs once, after every page reaches a terminal state.
public class IndexSummaryReducer : IReduceJobHandler<IndexSummary, IndexPageResult>
{
    public Task Reduce(IndexSummary s, IReadOnlyCollection<MapResult<IndexPageResult>> results,
                       IJobExecutionContext ctx, CancellationToken ct)
    {
        var indexed = results.Where(r => r.Succeeded).Sum(r => r.Value!.Indexed);
        // swap index alias / warm caches / enqueue a targeted re-index of the failed ids …
        return Task.CompletedTask;
    }
}

// 3) Register the handlers (module Initialize). The item/result/state types are inferred from the handlers
//    (or use the explicit AddMapReduceJob<TItem, TResult, TState, TMap, TReduce>() overload).
services.AddMapReduceJob<IndexPageHandler, IndexSummaryReducer>();

// 4) Enqueue a batch — NAME the map and reduce handlers (like Enqueue<THandler>): the item/result/state types are
//    derived from their interfaces and validated against items/state at enqueue. Partition into PAGES, not documents.
var pages = allProductIds.Chunk(50).Select(ids => new IndexPage("Product", ids));
await _mapReduce.Enqueue<IndexPageHandler, IndexSummaryReducer>(
    items: pages,
    state: new IndexSummary("Product", DateTime.UtcNow.Ticks),
    options: new MapReduceOptions { Queue = "indexing", FailurePolicy = FailurePolicy.ContinueOnError, ReportProgress = true });
```

> **Naming the handlers** makes the enqueue self-documenting and lets the *same* item/result/state set drive
> **several** map/reduce handler pairs — the pair you name at enqueue is the one that runs. `items`/`state` are typed
> as `object`/`IEnumerable<object>` and validated against the handlers' contracts at enqueue, so a mismatch fails fast.

**What the parameters mean:**

- **`items`** — the work split into independent units. The engine creates **one map task per item** and runs them in
  parallel across all workers. Partition *coarsely* — a page of ids, not a single document — so the task count and
  each `TResult` stay small (`items` is your fan-out width). `Enqueue` returns quickly even for very large batches: it
  stores the items and queues a single fan-out task that creates the map tasks on a worker (not inline in your call).
- **`state`** — shared, read-only context handed to the **reduce** step once (it is *not* passed to the map handler).
  Put batch-level information the reducer needs here: what's being processed, a started-at timestamp, an index alias
  to swap, a correlation id, etc. Each map item, by contrast, gets only its own `TItem`.
- **`options`** (`MapReduceOptions`) — optional knobs:

| Option | What it means | When to set it |
|---|---|---|
| `Queue` | The queue **both** the map tasks and the reduce task run on. | Route a large batch to a dedicated worker pool so it doesn't starve short interactive jobs. Falls back to `VirtoCommerce:BackgroundJobs:DefaultQueue`. |
| `FailurePolicy` | `FailFast` (default): if any item fails, the batch is marked faulted and **reduce is skipped**. `ContinueOnError`: failures are recorded and **reduce runs with the full result set** (each `MapResult<T>` carries `Succeeded`/`Error`). | Use `ContinueOnError` when partial results are useful (indexing, import) and you want the failed items surfaced to the reducer for a targeted re-run. |
| `ReportProgress` | Streams aggregate progress (completed/total items) to the admin UI on one shared notification. | Long batches the operator is watching. Leave off for fire-and-forget. |

**Why not one long-running job?**

| | One long-running job | Map/reduce |
|---|---|---|
| Throughput | 1 worker, sequential | All workers, in parallel |
| Scale-out | Adding workers does nothing | Linear speedup |
| A failure at item 180k | Whole job fails / restarts from 0 | That page is recorded; the rest still complete |
| Partial success | Hand-rolled checkpointing | `ContinueOnError` → reduce gets every page's outcome; failed ids surfaced |
| Finalize step | Manual | First-class reduce (alias swap, notify, re-index failures) |

**Failure policy:** `FailFast` (default) marks the batch faulted and skips reduce if any item fails;
`ContinueOnError` records failures and runs reduce with the full result set (each `MapResult<T>` carries
`Succeeded`/`Error`). Map items are recorded as failed rather than engine-retried, so the join is deterministic on
every engine; results are idempotent under at-least-once delivery (keyed by item index).

#### How many map tasks run in parallel?

A map task is an **ordinary background job** — it shares the worker pool with every other job, so the per-instance
concurrency is the active engine's worker concurrency, not a map/reduce-specific setting. Scale **out** (more
`Worker`/`Both` instances) to multiply it; the fleet-safe store below keeps the join correct across instances.

| Provider | Concurrent map tasks **per instance** | Knob | Default |
|---|---|---|---|
| **Hangfire** | `WorkerCount` (server worker threads) | `VirtoCommerce:Hangfire:WorkerCount` | ~`ProcessorCount * 5` |
| **RabbitMQ** | `ConsumerDispatchConcurrency` (parallel handler dispatch), bounded by `PrefetchCount` | `VirtoCommerce:RabbitMQ:ConsumerDispatchConcurrency` (follows `PrefetchCount` when `<= 0`) | `0` → auto (`ProcessorCount × ConcurrencyPerCore`) |

> **RabbitMQ gotcha:** `PrefetchCount` alone does *not* parallelize work — it only controls how many unacked
> messages the broker delivers. The client still invokes the consumer handler **one at a time** unless
> `ConsumerDispatchConcurrency` is &gt; 1. Both knobs **default to `0`, which auto-scales** to the CPU count the pod
> sees (`ProcessorCount × ConcurrencyPerCore`, `ConcurrencyPerCore` = 10), so a worker self-sizes with no
> per-environment config; `ConsumerDispatchConcurrency` follows the effective `PrefetchCount`. Set a **positive**
> value on either to pin it — e.g. `"PrefetchCount": 10` gives 10 concurrent handlers; set
> `ConsumerDispatchConcurrency` explicitly to decouple the two (high prefetch for throughput, bounded handler
> parallelism). For CPU-bound handlers drop `ConcurrencyPerCore` to ~1–2.
>
> So a 20 000-page batch where each page takes 5 s finishes in ≈ `20000 / WorkerCount * 5 s` per Hangfire instance,
> and ≈ `20000 / ConsumerDispatchConcurrency * 5 s` per RabbitMQ instance. The sample's `IndexPageHandler` logs
> `MAP START … N running in parallel on this instance` on every item so you can watch the live concurrency.

#### Where are the map results stored?

In the **`IMapReduceBatchStore`** — never in the message itself. Two implementations ship in
`VirtoCommerce.BackgroundJobs.Data` and are selected automatically:

- **Redis** (`RedisMapReduceBatchStore`) when a Redis connection is configured — **fleet-safe across instances**.
  Per batch it keeps four keys under `vc:mapreduce:{batchId}:*` — `:meta` (batch metadata), `:items` (the input
  items the fan-out worker reads), `:results` (a **hash, one field per item index** → that item's serialized
  `TResult`), and `:reduce` (the atomic reduce-claim flag). All carry a **7-day TTL** so abandoned batches
  self-clean, and the whole set is deleted once reduce succeeds.
- **In-memory** (`InMemoryMapReduceBatchStore`) for a single instance — same shape in a `ConcurrentDictionary`.
  Not shared across instances; use Redis for a multi-instance fleet.

Each map result is stored **keyed by its item index** (Redis hash field / dictionary key). That is what makes the
completed count exact (`= number of distinct indices = HLEN`) and redelivery idempotent — a re-run overwrites the
same slot instead of double-counting. The reducer reads the whole set back at the end, so **keep each `TResult`
small** (a count + a handful of failed ids, not the indexed documents themselves).

#### What happens if an instance fails mid-batch?

Distinguish a **handler failure** (your `Map`/`Reduce` throws) from an **infrastructure failure** (the instance
crashes, OOMs, or loses the broker connection):

- **A map handler throws** → the failure is *recorded* at that item's index (`Succeeded = false`, with the message)
  and counts as completed; the item is **not** retried. `FailurePolicy` then decides at reduce time whether the
  batch faults (`FailFast`) or reduce still runs with failures included (`ContinueOnError`). (Cancellation is the
  exception — it's re-thrown so the engine retries, not recorded as a business failure.)
- **An instance dies while a map task is in flight** → the task never reached the store, so the engine redelivers
  it: Hangfire re-queues the job after its invisibility timeout (and `MaxRetryAttempts` applies); RabbitMQ requeues
  the unacked message to another consumer. It re-runs on a surviving instance and lands its result at the **same
  index** — idempotent, no double-count. Because the reduce trigger is an atomic `SET NX`, only **one** reduce is
  ever enqueued no matter how many map tasks redeliver. Already-finished items are untouched — you lose only the
  in-flight item's work, not the whole batch.
- **An instance dies during reduce** → the reduce task is also an ordinary job and is redelivered the same way; it
  re-reads every stored map result and runs the reducer again. The store is cleaned up **only after reduce
  succeeds**, so nothing is lost — but the reducer can run **more than once**, so **make `Reduce` idempotent**
  (e.g. swap an index alias / upsert, don't blindly append). 

A runnable, self-contained map/reduce example (framed as product indexing) is in the
[sample module](samples/VirtoCommerce.BackgroundJobs.SampleModule/README.md).

## Writing a custom engine

The engine selector is open: any provider name other than `Hangfire`/`RabbitMQ` is left for a custom module to
satisfy. A custom engine ships as a normal Virto Commerce module that depends on this one, references
`VirtoCommerce.BackgroundJobs.Core` (the engine port + agnostic implementations) and, optionally,
`VirtoCommerce.BackgroundJobs.Data` (the reusable in-process recurring scheduler). Both are published as NuGet
packages.

### What you implement vs. reuse

| Concern | What to do |
|---|---|
| Engine port (**required**) | Implement `IJobEngine` — `ProviderName`, `Enqueue(JobEnvelope, EnqueueOptions, ct)`, `GetStatus(jobId, ct)`, `Delete(jobId, ct)`. |
| Processing host | An `IHostedService` (like `RabbitMqJobConsumer`) that consumes/receives and calls `IJobDispatcher.Dispatch(envelope, context, ct)`. Register it only when active **and** `Mode != Producer`. A **push** engine instead exposes an inbound callback controller that runs the received envelope via `IJobEnvelopeRunner.Run(envelope, jobId, ct)` (build context → dispatch). |
| Recurring | Either call `services.AddInProcessRecurringScheduler()` (reuses the Cronos + distributed-lock + occurrence-marker scheduler, enqueues via `IBackgroundJob`), **or** implement `IRecurringJobScheduler` natively. |
| Reuse — do **not** reimplement | `IBackgroundJob`, `IJobDispatcher`, `IJobEnvelopeRunner`, `IJobPayloadSerializer`, `JobEnvelope`, `JobExecutionContext`, progress, `RecurringJobsApplier`, `IRecurringJobStateStore` + its Redis/in-memory impls. The host module registers these once. |

### Recipe

```csharp
// module.manifest — depend on the engine host module
//   <dependency id="VirtoCommerce.BackgroundJobs" version="3.x" />

// PlatformStartup : IPlatformStartup — self-activate on the configured provider name.
// Use the host module's IConfiguration.IsBackgroundJobsProvider(...) helper instead of re-implementing the check.
public void ConfigureServices(IServiceCollection services, IConfiguration config)
{
    if (!config.IsBackgroundJobsProvider("MyEngine")) return;   // VirtoCommerce:BackgroundJobs:Provider == "MyEngine"

    services.Configure<MyEngineOptions>(config.GetSection("VirtoCommerce:MyEngine"));
    services.AddSingleton<IJobEngine, MyJobEngine>();   // REQUIRED
    services.AddInProcessRecurringScheduler();          // reuse cron recurring, OR register your own IRecurringJobScheduler
}

public void ConfigureHostServices(IServiceCollection services, IConfiguration config)
{
    if (config.IsBackgroundJobsProvider("MyEngine") && GetMode(config) != BackgroundJobsMode.Producer)
        services.AddHostedService<MyJobConsumer>();      // drain + IJobDispatcher.Dispatch
}
```

`IsBackgroundJobsProvider` / `GetBackgroundJobsProvider` (in `VirtoCommerce.BackgroundJobs.Core`) read
`VirtoCommerce:BackgroundJobs:Provider` and treat an empty value as `Hangfire`, so every engine module checks the
selector the same way without duplicating string comparisons.

Because the facade (`IBackgroundJob`) takes `IJobEngine` as an **optional** dependency, the platform boots even with
no engine: enqueue then throws `BackgroundJobEngineNotInstalledException` with an actionable message, and declared
recurring jobs are skipped with a warning. On startup the host module logs whether the active engine's `ProviderName`
matches the configured `Provider`, and a **`Background jobs` health check** reports on `/health` (Unhealthy when no
engine is registered, Degraded on a provider/engine mismatch) — so a missing or mis-named custom engine is easy to spot.

A complete, buildable **push-based** engine — Google Cloud Tasks, which POSTs to an HTTP callback instead of running a
consumer — lives in [`samples/VirtoCommerce.BackgroundJobs.GoogleCloudTasks`](samples/VirtoCommerce.BackgroundJobs.GoogleCloudTasks/README.md).
It confirms the contracts don't assume a pull/consumer model: a push engine adds an inbound callback controller and
reuses the shared `IJobEnvelopeRunner` (over `IJobDispatcher`) — no new platform contract needed.

### Certify your engine (conformance kit)

Don't guess whether your engine follows the standard — run the **conformance suite**. The packable
[`VirtoCommerce.BackgroundJobs.Conformance`](tests/VirtoCommerce.BackgroundJobs.Conformance/CONFORMANCE.md) project
ships an abstract xUnit base with **one clear scenario per feature** (enqueue→dispatch, payload fidelity, user
context, status, delete, unique-key, queue routing, retry, progress, map/reduce, recurring). Reference
it from your engine's test project, write one small fixture wiring your **real** engine against **real**
infrastructure (a connection string), derive one test class, and `dotnet test` — green means conformant:

```csharp
public sealed class MyEngineConformanceFixture : JobEngineConformanceFixture
{
    public override EngineCapabilities Capabilities => new() { SupportsStatusQuery = true, SupportsRetry = true, /* … */ };

    protected override bool TryConfigureEngine(IServiceCollection services, out string? unavailableReason)
    {
        unavailableReason = null;
        services.AddSingleton<IJobEngine, MyJobEngine>();
        services.AddHostedService<MyJobConsumer>();
        return true;
    }
}

public class MyEngineConformanceTests(MyEngineConformanceFixture fixture)
    : JobEngineConformanceTests<MyEngineConformanceFixture>(fixture);
```

The kit certifies Hangfire (in-process) and RabbitMQ (against a broker via `VC_CONFORMANCE_RABBITMQ`) in this repo,
and includes an infrastructure-free `ReferenceJobEngine` as the copy-paste template. Capability-gated scenarios
assert the documented fallback (or skip) for features your engine doesn't provide, so it's never penalized for them.

## Documentation

* [Background processing developer guide](https://docs.virtocommerce.org/)
* [REST API](https://virtostart-demo-admin.govirto.com/docs/index.html?urls.primaryName=VirtoCommerce.BackgroundJobs)
* [View on GitHub](https://github.com/VirtoCommerce/vc-module-background-jobs/)

## References

* [Deployment](https://docs.virtocommerce.org/platform/developer-guide/Tutorials-and-How-tos/Tutorials/deploy-module-from-source-code/)
* [Installation](https://docs.virtocommerce.org/platform/user-guide/modules-installation/)
* [Home](https://virtocommerce.com)
* [Community](https://www.virtocommerce.org)
* [Download latest release](https://github.com/VirtoCommerce/vc-module-background-jobs/releases/latest)

## License

Copyright (c) Virto Solutions LTD.  All rights reserved.

This software is licensed under the Virto Commerce Open Software License (the "License"); you
may not use this file except in compliance with the License. You may
obtain a copy of the License at http://virtocommerce.com/opensourcelicense.

Unless required to applicable law or written form, the software
distributed under the License is provided on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied.
