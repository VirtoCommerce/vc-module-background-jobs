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

## Requirements

The module needs a platform release that carries `IJobProgress.Complete` and the `JobOutcome` enum in
`VirtoCommerce.Platform.Core.Jobs`. The module closes a job's progress notification through `Complete`, so a host on an
older platform cannot install this version of the module.

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

## Telemetry (OpenTelemetry)

Every job — on **every** engine — emits telemetry from the shared dispatch path (`DefaultJobDispatcher`), so Hangfire,
RabbitMQ and In-Memory are measured identically. `JobTelemetry` uses only the standard .NET OpenTelemetry primitives
(`ActivitySource` + `Meter`, both named `VirtoCommerce.BackgroundJobs`) — no dependency on any specific telemetry
backend, and it never registers itself with a `TracerProviderBuilder`/`MeterProviderBuilder` (that's the host's call,
per standard OTel library guidance — a module that self-registered would force sampling/tag-building cost even in
installs with no telemetry configured at all). Recording is a near-zero-cost no-op unless something has registered a
listener for that name. Export is opt-in per deployment: add `VirtoCommerce.BackgroundJobs` to the `OpenTelemetry:Sources`
and `OpenTelemetry:Meters` lists, which both the [Application Insights module](https://github.com/VirtoCommerce/vc-module-app-insights)
and the [OpenTelemetry module](https://github.com/VirtoCommerce/vc-module-opentelemetry) read:

```
OpenTelemetry__Sources__0=VirtoCommerce.BackgroundJobs
OpenTelemetry__Meters__0=VirtoCommerce.BackgroundJobs
```

Neither module references `VirtoCommerce.BackgroundJobs`; the deployment names the source, the module only declares it.

**Pre-aggregated metrics** (`Meter` histograms → `customMetrics` when exported to Application Insights), dimensioned
by the low-cardinality `engine` / `handler` / `outcome` (`success` \| `canceled` \| `failure`) — sampling-immune and
safe for the per-series cap:

| Metric | Meaning |
|---|---|
| `VirtoCommerce.BackgroundJobs/jobs/execution.duration.ms` | Handler run time. |
| `VirtoCommerce.BackgroundJobs/jobs/queue.latency.ms` | Enqueue → dispatch delay (omitted when the enqueue-time header is absent, e.g. a redelivered legacy message). |

**Drill-down span** (`Activity` → `dependencies` when exported to Application Insights, `ActivityKind.Internal`):
`JobCompleted` — tags `engine`, `handler`, `outcome`, `runId`, `queueLatencyMs`; its duration is the actual handler
run time (the activity is backdated to span the execution window). The high-cardinality `runId` is carried on the
span (subject to sampling) rather than on the metric dimensions, so per-run analysis stays possible without blowing
the metric series cap.

Ready-made Kusto queries (throughput, p95 duration, queue latency, failure rate — sliced by engine/handler) live in
[`docs/benchmark-kql.md`](docs/benchmark-kql.md).

## Components

### Projects

| Project | Layer | Purpose |
|---|---|---|
| `VirtoCommerce.BackgroundJobs.Core` | Core | **Contracts, models, options and reusable helpers only** (namespace `VirtoCommerce.BackgroundJobs`) — `IJobEngine`, `IJobDispatcher`, `IJobPayloadSerializer`, `IBackgroundJobsAdminQuery`, `IJobEnvelopeRunner`, `IRecurringJobStateStore`, `JobEnvelope`/models, progress, `JobExecutionContextFactory`, `JobJsonSettings`, plus the map/reduce orchestration. **No service implementations and no Cronos dependency.** Published as a NuGet so client and custom-engine projects reference the port without the implementations. |
| `VirtoCommerce.BackgroundJobs.Hangfire` | Engine | Hangfire implementation of `IJobEngine`; reuses the platform's former Hangfire storage/dashboard. Published as a NuGet. |
| `VirtoCommerce.Platform.Hangfire.Shim` | Compat | Produces a type-forwarding `VirtoCommerce.Platform.Hangfire.dll` for binary compatibility with existing modules. |
| `VirtoCommerce.BackgroundJobs.RabbitMQ` | Engine | RabbitMQ implementation of `IJobEngine` + the in-process consumer (`RabbitMqJobConsumer`). Published as a NuGet. |
| `VirtoCommerce.BackgroundJobs.Web` | Web | Module host: `PlatformStartup` (engine/mode selection), `JobsController`, settings & permissions. |
| `VirtoCommerce.BackgroundJobs.Data` | Data | The engine-agnostic **service implementations** (`JobEngineBackgroundJob` facade, `DefaultJobDispatcher`, `JsonJobPayloadSerializer`, `JobTelemetry`, `JobEnvelopeRunner`, `BackgroundJobsAdminQuery`), the in-process recurring scheduler + applier + schedule resolver (`AddInProcessRecurringScheduler`, `GenericRecurringJobScheduler`, `RecurringJobsApplier`, `RecurringScheduleResolver` — this is where the **Cronos** dependency lives), and the Redis/in-memory occurrence-marker & map/reduce batch stores. Published as a NuGet. |

### Key Services

| Service | Interface | Responsibility |
|---|---|---|
| Enqueue facade | `IBackgroundJob` | Engine-agnostic, handler-explicit enqueue (`Enqueue<THandler>(payload)`; also a non-generic `Enqueue(Type, payload)` for name/type-addressed triggering) and `Cancel(jobId)` + `SupportsCancellation`. |
| Cancellation store | `IJobCancellationStore` | Shared "cancellation requested" flags (Redis fleet-wide, in-memory fallback); used by RabbitMQ for cooperative cancel. |
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
| POST | `api/background-jobs/enqueue` | Trigger a registered job by name — body `{ name, payload, options? }` → engine job id. `options.progressNotificationId` is refused with `400`. Requires `background-jobs:execute`. |
| POST | `api/platform/jobs/{id}/cancel` | Request cancellation of a job. `501` when the active engine can't cancel, `404` when the engine reports nothing was cancelled (an unknown job, or one that already finished, on an engine that reports status: Hangfire, InMemory). RabbitMQ only records the request for any id and answers `200`. Requires `background_jobs:manage`. |

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
| `ReportProgress` | **The user is watching.** For long jobs, stream `context.Progress.Report(...)` to the admin UI over SignalR (progress bar + log). Skip it for fire-and-forget work to avoid needless notifications. | The `IBackgroundJob` facade creates the job's progress push-notification at enqueue (before the engine takes the job), and the worker updates and closes it. Works on every engine. |
| `ProgressNotificationId` | **Report into an existing notification** instead of a new one — e.g. a parent operation, or one shared bar across several jobs (this is how map/reduce shows an aggregate bar). | When set you don't also need `ReportProgress`; updates flow to that id. The caller owns that notification: a job reporting into it never closes it. In-process callers only — `POST api/background-jobs/enqueue` refuses it (see [Admin & integration API](#admin--integration-api)). |
| `UniqueKey` | **De-duplicate re-enqueues.** Collapse repeated triggers of the "same" work (a debounce/idempotency key) into a single job. | **No shipped engine de-duplicates by it** — Hangfire, RabbitMQ and InMemory ignore the key, so do not rely on it to prevent duplicate runs; make handlers idempotent. Only the [Google Cloud Tasks sample](samples/VirtoCommerce.BackgroundJobs.GoogleCloudTasks/README.md) honors it (task-name dedup). |

**Retries.** `VirtoCommerce:BackgroundJobs:MaxRetryAttempts` is the default count of retries a failed job gets on top
of its first run (so a job runs at most `MaxRetryAttempts + 1` times). `EnqueueOptions.MaxRetryAttempts` overrides it
for one job, on **every shipped engine** (Hangfire, RabbitMQ, InMemory): `N` means `N + 1` runs when the job fails every
time, `0` disables retries, and a negative value counts as `0`. The override may **raise** the count above the default
as well as lower it, and there is **no upper bound** — a caller who sets a large limit gets that many retries. Once
the retries are spent RabbitMQ dead-letters or drops the job (see [Configuration](#configuration)). On Hangfire the
default is the engine's own retry filter (it follows `VirtoCommerce:BackgroundJobs:MaxRetryAttempts` when that key is
configured), and the per-job value is applied on top of it. A custom engine must honor the per-job value too — the
conformance kit asserts it (see [What a custom engine owes the standard](#what-a-custom-engine-owes-the-standard)).

#### Progress notification: attempts and outcome

With `ReportProgress` the facade creates the job's notification at enqueue and the worker keeps it up to date; it
carries the job id (`JobId`, written by the worker, so it is empty while the job waits in the queue), the attempt
(`Attempt`, `TotalAttempts`) and, once the job ended, the `Outcome` and the `Error`. `Outcome` is a **string** holding
the name of the `JobOutcome` value — `Succeeded`, `Failed` or `Canceled` — not a number, because how an enum is
serialized on the live channel depends on the deployment.

* A handler that returns closes the notification `Succeeded`.
* A failed attempt that has a retry pending leaves it open and says so (`Attempt 1 of 4 failed, retrying: …`); only the
  failure of the final attempt closes it `Failed`, with the error. An engine that does not report the total number of
  attempts treats every attempt as the final one.
* A cancel requested through `Cancel` closes it `Canceled` — also for a job that was canceled before it ran or while it
  waited for a retry. A handler that fails with another exception after the cancel does not run again; on the final
  attempt, where no retry was pending for the cancel to stop, the failure is closed `Failed` with its error.
* A notification passed in by the caller (`ProgressNotificationId`, map/reduce) is never closed by a job.

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

An unknown name returns **404**; a payload that doesn't match the handler returns **400**, and so does a request body
that is not valid JSON or a `payload` that cannot be read (the response names the failing member, and no job is
enqueued).

**`options.progressNotificationId` is refused with 400**, naming the field. A REST caller cannot own a notification
that was created elsewhere, and accepting the id would let any caller with `background-jobs:execute` write into another
person's notification. To follow a job, set `options.reportProgress`: the endpoint then creates the notification itself
and the worker stamps it with the job id the endpoint returns. A caller that passed `progressNotificationId` before this
version gets a 400 after the upgrade; in-process callers (`IBackgroundJob`, map/reduce) keep the option. The addressable name
defaults to the handler's type name; pass `AddBackgroundJob<THandler, TPayload>(name: "my-name")` to set a custom one.
When distinct handlers share a name (e.g. a `LogEntityChangesJobHandler` in several modules), each is listed and
addressed by its full handler type name, such as `VirtoCommerce.CatalogModule.Data.Jobs.LogEntityChangesJobHandler`.

### Payload serialization

The job serializer reads a payload exactly as it was written: date-like strings stay strings and numbers with a
fraction are read as `decimal` (`JobJsonSettings` sets `DateParseHandling.None` and `FloatParseHandling.Decimal`, for
both the default instance and the one registered in DI). Without that a polymorphic member (one read through the
platform's `PolymorphJsonConverter`) lost fidelity on every engine, in-process enqueues included: a nested decimal was
cut to `double` precision and a nested date-like string was parsed and re-formatted in the host's culture. The same
applies to the payload `POST api/background-jobs/enqueue` receives: the controller deserializes the raw JSON with the
same settings, so the handler gets every member the payload type declares unchanged. Members the type does not declare
are dropped.

What changes for members **typed `object` or `JToken`** (and the like — `JObject`, `JValue`): they now receive a
`string` for a date-like value instead of a `DateTime`, and a `decimal` instead of a `double` for a number with a
fraction. A fractional number outside the `decimal` range in such a member fails to read instead of becoming a
`double`. Members with a declared type (`DateTime`, `double`, `decimal`, …) are not affected.

> **Behaviour change — date properties in bulk actions.** A bulk update of a *date* property (vc-module-bulk-actions
> with vc-module-catalog) carries the value in an `object` member, so it now arrives as a `string`. The catalog parses
> it with `Convert.ToDateTime(value, CultureInfo.InvariantCulture)`, which turns a `…Z` string into local time. **On a
> host whose time zone is not UTC — a local Windows stand among them — the stored value is shifted by the host's UTC
> offset.** Hosts on UTC store the same value as before; number properties keep the exact decimal instead of a
> `double`. The parsing is in the catalog module and also affects any other caller that passes it a date string; it is
> tracked as VCST-6984.

### Cancellation

Cancel a job engine-agnostically through the facade. Cancellation is a **capability an engine may or may not have**,
so probe `SupportsCancellation` first to tell "not supported" from "nothing to cancel" (a UI can disable the button;
the REST endpoint answers `501`):

```csharp
public class ThumbnailsController(IBackgroundJob backgroundJob)
{
    public async Task<ActionResult> Cancel(string jobId, CancellationToken ct)
    {
        if (!backgroundJob.SupportsCancellation)
            return StatusCode(501, "Cancellation not supported by the active engine.");

        return await backgroundJob.Cancel(jobId, ct) ? Ok() : NotFound();
    }
}
```

**Handlers must honor their `CancellationToken`** — cancellation asks a running job to stop; a handler that ignores
the token can't be stopped mid-run. Per engine:

| Engine | Not started | Running | Notes |
|---|---|---|---|
| **Hangfire** | removed from the queue | transitioned to *Deleted*; the handler's `CancellationToken` trips | native; no extra infrastructure |
| **RabbitMQ** | discarded when the worker dequeues it (a published message can't be recalled) | the handler's `CancellationToken` trips | **cooperative** via `IJobCancellationStore`; needs **Redis** for fleet-wide cancel (in-memory fallback is single-instance). The worker polls the store on a short interval, so running-cancel has bounded latency |
| **In-Memory** | removed | the running task's `CancellationToken` trips | dev/test only, single process |

A job that has already **finished** has nothing to cancel: an engine that reports status refuses the request
(`Cancel` returns `false`, the REST endpoint answers `404`) and the job keeps the state it ended in, so a late cancel
cannot rewrite a finished job's history. A cancel closes the job's progress notification with the outcome `Canceled`
(see [Progress notification: attempts and outcome](#progress-notification-attempts-and-outcome)).

`Cancel` returns `true` when the active engine accepted the request. On RabbitMQ that means "recorded" (it has no job
ledger to confirm the id existed); on Hangfire/In-Memory it reflects whether a job was actually found. Cancelling a
whole map/reduce fan-out by `batchId` is a separate follow-on — a single `jobId` cancel stops one job, not the batch.

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
  Per batch it keeps these keys under `vc:mapreduce:{batchId}:*` — `:meta` (batch metadata), `:items` (the input
  items the fan-out worker reads), `:results` (a **hash, one field per item index** → that item's serialized
  `TResult`), `:reduce` (the atomic reduce-claim flag), `:fanoutprogress` (the fan-out dispatch checkpoint) and
  `:progressclosed` (holds the close once the batch's notification is closed). All carry a **7-day TTL** so abandoned
  batches self-clean, and the whole set is deleted once reduce succeeds.
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

## Migrating from VirtoCommerce.Platform.Hangfire

The platform **no longer ships `VirtoCommerce.Platform.Hangfire`** — its Hangfire integration moved into this module.

All of its former public types (`HangfireOptions`, `HangfireJobStorageType`, `IRecurringJobService`, `SettingCronJob`,
the DI/app-builder extensions, the user-context middleware, …) are preserved by a **type-forwarding shim**
(`VirtoCommerce.Platform.Hangfire.dll`, shipped by this module), so modules that were **already built** against the old
assembly keep loading unchanged once the `VirtoCommerce.BackgroundJobs` module is installed — you don't recompile them.

Modules you **do recompile** can no longer restore the old platform package. Do Step 1 to fix the build; do Step 2 when
you can, to become engine-agnostic (run on Hangfire, RabbitMQ or In-Memory with no code change).

### Step 1 — Fix the build (drop the dead reference)

**You call the Hangfire API directly** — `Hangfire.BackgroundJob.Enqueue`, `RecurringJob.AddOrUpdate`,
`IBackgroundJobClient`, `IRecurringJobManager`, or any `VirtoCommerce.Platform.Hangfire.*` type.

Swap the dead package for this module's Hangfire engine — **no code changes**:

```xml
<!-- .csproj — remove -->
<PackageReference Include="VirtoCommerce.Platform.Hangfire" Version="3.x.x" />
<!-- .csproj — add (defines the former VirtoCommerce.Platform.Hangfire.* types in their original namespaces) -->
<PackageReference Include="VirtoCommerce.BackgroundJobs.Hangfire" Version="3.1050.0" />
```

```xml
<!-- module.manifest — depend on the engine module (provides the engine + the compat shim + wiring at runtime) -->
<dependencies>
  <dependency id="VirtoCommerce.BackgroundJobs" version="3.1050.0" />
</dependencies>
```

Your existing `using VirtoCommerce.Platform.Hangfire;` code compiles unchanged, and keeping
`VirtoCommerce:BackgroundJobs:EnableLegacyHangfire = true` (the default) keeps Hangfire initialized so your direct
Hangfire calls keep working.

This is a **stepping stone** — prefer Step 2.

### Step 2 — Move off Hangfire to the engine-agnostic API (recommended)

Migrate direct Hangfire calls to the `Platform.Core` contracts, then **remove the `VirtoCommerce.BackgroundJobs.Hangfire`
reference** — your module compiles against only `VirtoCommerce.Platform.Core` and runs on whatever engine the deployment
configures.

Define a payload + handler, register it, and enqueue by handler:

```csharp
// Before (Hangfire):
BackgroundJob.Enqueue(() => _emailSender.SendOrderConfirmation(orderId));         // fire-and-forget
RecurringJob.AddOrUpdate("send-digest", () => _digest.Run(), "0 8 * * *");        // recurring
```

```csharp
// After (engine-agnostic):

// 1) payload — a plain, serializable class
public class SendOrderEmailPayload { public string OrderId { get; set; } }

// 2) handler — no Hangfire, no engine reference (Platform.Core only)
public class SendOrderEmailHandler(IEmailSender sender) : IBackgroundJobHandler<SendOrderEmailPayload>
{
    public Task Execute(SendOrderEmailPayload p, IJobExecutionContext ctx, CancellationToken ct)
        => sender.SendOrderConfirmationAsync(p.OrderId, ct);
}

// 3) register in Module.Initialize(IServiceCollection)
services.AddBackgroundJob<SendOrderEmailHandler, SendOrderEmailPayload>();
services.AddRecurringJob<SendDigestHandler, SendDigestPayload>(s => s.WithCron("0 8 * * *"));

// 4) enqueue — inject IBackgroundJob, or use the static BackgroundJob helper (no constructor injection needed)
await _backgroundJob.Enqueue<SendOrderEmailHandler>(new SendOrderEmailPayload { OrderId = orderId });
await BackgroundJob.Enqueue<SendOrderEmailHandler>(new SendOrderEmailPayload { OrderId = orderId });
```

Common Hangfire APIs and their replacements:

| Hangfire (before) | Engine-agnostic (after) |
|---|---|
| `BackgroundJob.Enqueue(() => svc.Work(x))` | payload + `IBackgroundJobHandler<T>` + `IBackgroundJob.Enqueue<THandler>(payload)` (or the static `BackgroundJob.Enqueue<THandler>`) |
| `RecurringJob.AddOrUpdate(id, () => svc.Work(), cron)` | `services.AddRecurringJob<THandler, TPayload>(s => s.WithCron(cron))` |
| `IRecurringJobService` / setting-driven cron | `services.AddRecurringJob<THandler, TPayload>(s => s.FromSettings(enabled: …, cron: …))` — with a `SettingValueType.Cron` setting |
| `PerformContext`, `IJobCancellationToken` (progress / cancel) | the handler's `IJobExecutionContext ctx` + `CancellationToken ct`; report via `ctx.Progress.Report(new JobProgressInfo { Message = …, ProcessedCount = …, TotalCount = … }, ct)` |
| `BackgroundJob.Delete(jobId)` (cancel) | `IBackgroundJob.Cancel(jobId)` (probe `SupportsCancellation` first) — see [Cancellation](#cancellation) |
| `HangfireOptions`, the `/hangfire` dashboard | engine-specific — not referenced by handlers |

```xml
<!-- .csproj after Step 2 — no Hangfire at all -->
<PackageReference Include="VirtoCommerce.Platform.Core" Version="3.1050.0" />
<!-- module.manifest still depends on VirtoCommerce.BackgroundJobs for the runtime engine -->
```

Once **every** module in a deployment has completed Step 2, set `VirtoCommerce:BackgroundJobs:EnableLegacyHangfire = false`
for a Hangfire-free platform (and, on the cloud, switch `Provider` to `RabbitMQ`) — see [Configuration](#configuration).

## Writing a custom engine

The engine selector is open: any provider name other than `Hangfire`/`RabbitMQ` is left for a custom module to
satisfy. A custom engine ships as a normal Virto Commerce module that depends on this one, references
`VirtoCommerce.BackgroundJobs.Core` (the engine port + contracts/models/helpers) and, typically,
`VirtoCommerce.BackgroundJobs.Data` (the reusable service implementations — dispatcher, serializer, envelope runner,
telemetry, and the in-process recurring scheduler). Both are published as NuGet packages.

### What you implement vs. reuse

| Concern | What to do |
|---|---|
| Engine port (**required**) | Implement `IJobEngine` — `ProviderName`, `Enqueue(JobEnvelope, EnqueueOptions, ct)`, `GetStatus(jobId, ct)`, `Delete(jobId, ct)`. |
| Processing host | An `IHostedService` (like `RabbitMqJobConsumer`) that consumes/receives and calls `IJobDispatcher.Dispatch(envelope, context, ct)`. Register it only when active **and** `Mode != Producer`. A **push** engine instead exposes an inbound callback controller that runs the received envelope via `IJobEnvelopeRunner.Run(envelope, jobId, ct)` (build context → dispatch). |
| Recurring | Either call `services.AddInProcessRecurringScheduler()` (reuses the Cronos + distributed-lock + occurrence-marker scheduler, enqueues via `IBackgroundJob`), **or** implement `IRecurringJobScheduler` natively. |
| Reuse — do **not** reimplement | `IBackgroundJob`, `IJobDispatcher`, `IJobEnvelopeRunner`, `IJobPayloadSerializer`, `JobEnvelope`, `JobExecutionContext`, progress, `RecurringJobsApplier`, `IRecurringJobStateStore` + its Redis/in-memory impls. The host module registers these once. |

### What a custom engine owes the standard

Besides moving the envelope to a worker, the engine carries the facts the shared dispatch path and the admin UI need.
Each duty is asserted by the [conformance kit](#certify-your-engine-conformance-kit); the optional ones are gated by a
capability flag on the fixture, and an engine that declares the flag false gets the documented fallback asserted
instead.

| Duty | What to do | Without it |
|---|---|---|
| **One job id** | Pass the worker the exact id `Enqueue` returned, on **every attempt** (`IJobEnvelopeRunner.Run(envelope, jobId, ct)` for a push engine). The id is how the admin UI finds the job's notification. | The progress page never finds its job, and nothing reports an error. Required of every engine, no flag. |
| **Attempts** | Fill `Attempt` (the first run is 1) and `TotalAttempts` on the `JobEnvelope` **before** `IJobDispatcher.Dispatch`, and build the worker's context from that envelope with `JobExecutionContextFactory.Create`. `TotalAttempts` is `Math.Max(0, envelope.MaxRetryAttempts ?? BackgroundJobsOptions.MaxRetryAttempts) + 1`. | `TotalAttempts` stays null and the dispatcher treats every attempt as final: the first failure closes the notification `Failed`, even when the engine retries. A context built any other way lacks the final-attempt flag, so a map/reduce batch whose last attempt fails stays on "running". The reverse also holds: with `TotalAttempts` null every attempt is final, so the first infrastructure failure in a fan-out or map task closes the batch `Failed` and marks it, which silences map progress while the engine retries; a later successful reduce still closes it `Succeeded`. Flag `ReportsAttemptBudget`. |
| **Per-job retry limit** | Honor `JobEnvelope.MaxRetryAttempts` the way the formula above does: it overrides the global count both ways, and a negative value counts as `0`. | A job runs the global number of times whatever the caller asked. |
| **Close a notification** | The dispatcher closes the job's notification on success and on a final failure. An engine closes it itself, through `context.Progress.Complete(JobOutcome.Canceled)` (`IJobProgress.Complete`, `VirtoCommerce.Platform.Core.Jobs`), when it ended the job for a **requested cancel** — only the engine can tell a requested cancel from a shutdown or a lost lock. That includes a job canceled before it ran or while it waited for a retry: close it with the attempt it was about to start and the job's total. | A canceled job's notification stays open on "running" for good. Flag `ClosesCanceledNotification`. |
| **Honor a cancel** | `Delete` of a running job trips its handler's `CancellationToken`, and a cancel requested during an attempt whose handler then fails with another exception stops the retries: no further attempt runs the handler, and the notification closes `Canceled`. On the **final** attempt that failure is a failure, and stays `Failed`. | Flag `SupportsCancellation` (and `SupportsRetry` for the retry case). |
| **Refuse `Delete` of a finished job** | An engine that reports status returns `false` for a job that already finished (succeeded, failed or already deleted) and leaves its state as it was. | A late cancel rewrites a finished job's history. Flags `SupportsStatusQuery` and `SupportsDelete`. |

`Complete` is a method on the platform's `IJobProgress`; a progress wrapper an engine puts around the context's progress
must forward it, or the wrapped reporter never learns the outcome. Wrap through the `wrapProgress` overload of
`JobExecutionContextFactory.Create`; a context rebuilt around the wrapped progress loses the final-attempt flag.
`JobOutcome` is `Succeeded`, `Failed` or `Canceled`;
the notification carries the **name** as a string (`JobProgressPushNotification.Outcome`), never the enum.

A push engine that rebuilds the job id in its callback must derive it the same way `Enqueue` did. The Google Cloud
Tasks sample returns the full task resource name from `Enqueue`, while Cloud Tasks hands the callback only the task id
in `X-CloudTasks-TaskName`, so the callback builds the full name from its own `ProjectId`, `LocationId` and `QueueId`.
The instance that enqueues and the instance that serves the callback must therefore use the **same** `Project`,
`Location` and `Queue` options, or the worker's job id will not match the one `Enqueue` returned.

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
context, status, delete, unique-key, queue routing, retry, progress, cancellation and the notification outcome,
per-job retry limits, one job id, map/reduce, recurring). Reference
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

The kit certifies Hangfire (in-process), InMemory and RabbitMQ (against a broker via `VC_CONFORMANCE_RABBITMQ`) in this
repo, and includes an infrastructure-free `ReferenceJobEngine` as the copy-paste template. The RabbitMQ fixture runs
only when `VC_CONFORMANCE_RABBITMQ` is set and skips otherwise; CI sets none, so a green CI run says nothing about
RabbitMQ. Capability-gated scenarios assert the documented fallback (or skip) for features your engine doesn't
provide, so it's never penalized for them. The capability flags added for the engine duties above
(`ReportsAttemptBudget`, `ClosesCanceledNotification`, `CanHoldQueuedJob`) default to `false`, so a fixture written
against an earlier version of the kit passes a gated scenario after an upgrade, on the fallback or skipped, until it
declares them. A scenario with no flag runs on every engine and fails an engine that breaks it whatever its fixture
declares. That includes one job id (`Worker_JobId_Equals_Enqueue_Result`), `Final_Failure_Closes_Notification_Failed`
and `Success_Closes_Notification_Succeeded`, which an engine written before them may not satisfy.

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
