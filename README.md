# Virto Commerce Background Jobs Module

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
* **Instance modes** — the same container image runs as `Producer` (enqueue only), `Worker` (process only) or
  `Both`, controlled by configuration.
* **No breaking changes** — a type-forwarding shim keeps existing `VirtoCommerce.Platform.Hangfire.*` consumers
  working; the Hangfire storage, dashboard, queues, retry and recurring jobs are unchanged.
* **Extensible by partners** — override a handler via DI (last registration wins) and extend a payload via
  `AbstractTypeFactory`, using the same tools partners already use across Virto.
* **Graceful when absent** — if no engine module is installed, the platform boots and surfaces an actionable
  "install via the Virto Commerce CLI" message instead of silently dropping work.

## Configuration

Background processing is configured under `VirtoCommerce:BackgroundJobs`. Provider-specific options keep their own
sections (`VirtoCommerce:Hangfire`, `VirtoCommerce:RabbitMQ`), so the existing Hangfire configuration is untouched.

```jsonc
"VirtoCommerce": {
  "BackgroundJobs": {
    "Provider": "Hangfire",     // Hangfire | RabbitMQ  (one engine per instance)
    "Mode": "Both",              // Producer | Worker | Both
    "DefaultQueue": "default",
    "MaxRetryAttempts": 3
  },
  "Hangfire": { /* existing Hangfire options — storage, dashboard, queues, worker count */ },
  "RabbitMQ": {
    "HostName": "localhost", "Port": 5672, "UserName": "guest", "Password": "guest", "VirtualHost": "/",
    // "Uri": "amqp://guest:guest@localhost:5672/",  // alternative to the host/port/credential fields above
    "PrefetchCount": 1,        // unacknowledged messages a consumer prefetches (QoS)
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
headers) for inspection or replay — or dropped if `UseDeadLetterQueue` is `false`. RabbitMQ keeps no job ledger,
so `GET api/platform/jobs/{id}` reports `Unknown` and job deletion is unsupported — observe jobs via progress
notifications instead.

**Recurring jobs** work on **either** engine. A recurring job is an ordinary `IBackgroundJobHandler<TPayload>` plus a
schedule declared with `AddRecurringJob` (see Usage). On Hangfire they use Hangfire's native recurring scheduler
(persisted, shown in the dashboard); on RabbitMQ (or any non-Hangfire engine) an in-process cron scheduler fires
each occurrence and enqueues the payload, with fleet-safe exactly-once firing via a distributed lock + a shared
occurrence marker (Redis when configured, in-memory for a single instance). The legacy expression-based
`IRecurringJobService` remains Hangfire-only for backward compatibility. If no engine module is installed, the
platform still boots and logs a warning that recurring jobs are not scheduled.

### Application Settings

This module exposes **no platform settings**. All background-job configuration — `Provider`, `Mode`, `DefaultQueue`,
`MaxRetryAttempts`, and the provider-specific sections — is a deployment-time concern configured **only** in
`appsettings.json` (`VirtoCommerce:BackgroundJobs`, `VirtoCommerce:Hangfire`, `VirtoCommerce:RabbitMQ`) and read at
startup. These are ops decisions, not admin-UI toggles.

### Permissions

| Permission | Description |
|---|---|
| `platform:background:jobs:manage` | View the Hangfire dashboard (`/hangfire`) and the jobs status API. |

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

## Components

### Projects

| Project | Layer | Purpose |
|---|---|---|
| `VirtoCommerce.BackgroundJobs.Core` | Core | Engine-internal contracts (`IJobEngine`, `IJobDispatcher`, `JobEnvelope`, options) and engine-agnostic implementations (facade, dispatcher, progress, serializer). Published as a NuGet so custom engines can reference the port. |
| `VirtoCommerce.BackgroundJobs.Hangfire` | Engine | Hangfire implementation of `IJobEngine`; reuses the platform's former Hangfire storage/dashboard. Published as a NuGet. |
| `VirtoCommerce.Platform.Hangfire.Shim` | Compat | Produces a type-forwarding `VirtoCommerce.Platform.Hangfire.dll` for binary compatibility with existing modules. |
| `VirtoCommerce.BackgroundJobs.RabbitMQ` | Engine | RabbitMQ implementation of `IJobEngine` + the in-process consumer (`RabbitMqJobConsumer`). Published as a NuGet. |
| `VirtoCommerce.BackgroundJobs.Web` | Web | Module host: `PlatformStartup` (engine/mode selection), `JobsController`, settings & permissions. |
| `VirtoCommerce.BackgroundJobs.Data` | Data | Module persistence (EF Core) + the reusable in-process recurring scheduler (`AddInProcessRecurringScheduler`) and its Redis/in-memory occurrence-marker stores. Published as a NuGet. |

### Key Services

| Service | Interface | Responsibility |
|---|---|---|
| Enqueue facade | `IBackgroundJob` | Engine-agnostic enqueue (message-based + Hangfire expression sugar). |
| Job handler | `IBackgroundJobHandler<TPayload>` | Your job logic; resolved from DI, overridable. |
| Engine port | `IJobEngine` | The active engine (Hangfire/RabbitMQ). One per instance. |
| Dispatcher | `IJobDispatcher` | Shared execution path: deserialize → resolve handler → run. |
| Progress | `IJobProgress` | Reports progress to the admin UI (SignalR). |
| Recurring registration | `AddRecurringJob<TPayload,THandler>` | Declare a handler + cron/setting-driven schedule (engine-agnostic). |
| Recurring scheduler port | `IRecurringJobScheduler` | Engine impl that schedules recurring jobs (Hangfire-native / in-process cron); NoEngine fallback warns. |
| Legacy recurring | `IRecurringJobService` | Expression-based cron registration (Hangfire only; back-compat). |

### REST API

| Method | Endpoint | Description |
|---|---|---|
| GET | `api/platform/jobs/{id}` | Get the status of a background job (engine-agnostic). |

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
//    use AddBackgroundJob<TPayload, THandler>() if you prefer to state it explicitly.
services.AddBackgroundJob<SendOrderEmailJob>();

// 4) Enqueue (engine-agnostic).
var payload = AbstractTypeFactory<SendOrderEmailPayload>.TryCreateInstance();
payload.OrderId = order.Id;
payload.CustomerEmail = order.Email;

await jobs.Enqueue(payload);                                            // fire-and-forget
await jobs.Enqueue(payload, new EnqueueOptions { ReportProgress = true }); // with progress
```

### Enqueue options — what to set and why

`Enqueue(payload)` with no options is the common case: the job runs as soon as a worker is free, on the default
queue. Pass `EnqueueOptions` only when you need one of these behaviors:

| Option | Why you'd set it | Notes / engine support |
|---|---|---|
| `Queue` | **Isolate workloads.** Route heavy or slow jobs (catalog indexing, bulk export) to a dedicated queue/worker pool so they don't starve short interactive jobs — e.g. `Queue = "indexing"`. | Honored by **Hangfire** and **RabbitMQ** (the worker drains the configured queues). Google Cloud Tasks is single-queue and ignores it. Falls back to `VirtoCommerce:BackgroundJobs:DefaultQueue` when unset. |
| `ReportProgress` | **The user is watching.** For long jobs, stream `context.Progress.Report(...)` to the admin UI over SignalR (progress bar + log). Skip it for fire-and-forget work to avoid needless notifications. | The engine creates a progress push-notification and returns its id via the notification stream. Works on every engine. |
| `ProgressNotificationId` | **Report into an existing notification** instead of a new one — e.g. a parent operation, or one shared bar across several jobs (this is how map/reduce shows an aggregate bar). | When set you don't also need `ReportProgress`; updates flow to that id. |
| `UniqueKey` | **De-duplicate re-enqueues.** Collapse repeated triggers of the "same" work (a debounce/idempotency key) into a single job. | Honored where the engine supports dedup — **Google Cloud Tasks** (task-name dedup). Hangfire and RabbitMQ currently ignore it. |

**Retries** are configured globally, not per enqueue: `VirtoCommerce:BackgroundJobs:MaxRetryAttempts` governs how many
times a failed job is retried before being dead-lettered/dropped (RabbitMQ); Hangfire uses its own retry filter.
(`EnqueueOptions.MaxRetryAttempts` is reserved for a future per-job override and is not yet honored.)

### Extending & overriding jobs (partner modules)

Background jobs follow the standard Virto Commerce extension model — a partner module can **extend the payload**
(`AbstractTypeFactory`), **override the handler** (DI, last registration wins), or **both**, and `jobs.Enqueue(payload)`
keeps working. The message carries the *concrete* payload type, so the extended type survives serialization across the
engine and arrives at the handler intact.

```csharp
// Partner module — Initialize(IServiceCollection):

// 1) Extend the payload: derive and override the type so the factory builds the extended one.
public class CustomSendOrderEmailPayload : SendOrderEmailPayload
{
    public string Locale { get; set; }
}
AbstractTypeFactory<SendOrderEmailPayload>.OverrideType<SendOrderEmailPayload, CustomSendOrderEmailPayload>();

// 2) Override the handler: register yours AFTER the base module — last registration wins.
public class CustomSendOrderEmailJob(IEmailSender sender) : IBackgroundJobHandler<SendOrderEmailPayload>
{
    public async Task Execute(SendOrderEmailPayload payload, IJobExecutionContext context, CancellationToken ct = default)
    {
        var custom = (CustomSendOrderEmailPayload)payload;   // the extended instance is delivered here
        await sender.Send(custom.CustomerEmail, custom.OrderId, custom.Locale, ct);
    }
}
services.AddBackgroundJob<SendOrderEmailPayload, CustomSendOrderEmailJob>();

// Enqueue is unchanged — create via the factory (returns the extended type) and enqueue as the base type.
SendOrderEmailPayload payload = AbstractTypeFactory<SendOrderEmailPayload>.TryCreateInstance();
payload.OrderId = order.Id;
((CustomSendOrderEmailPayload)payload).Locale = "fr-FR";
await jobs.Enqueue(payload);   // base handler resolved by contract; overriding handler + extended payload both apply
```

How it works: the enqueue facade records `PayloadType = payload.GetType()` (the concrete extended type) while the
handler is keyed on the base contract — so the dispatcher reconstructs the derived instance and resolves the
overriding `IBackgroundJobHandler<SendOrderEmailPayload>`. Either extension works on its own, or together. See
[`ExtensibilityTests`](tests/VirtoCommerce.BackgroundJobs.Tests/ExtensibilityTests.cs) for the end-to-end proof.

### Recurring jobs

A recurring job is the same handler declared with a schedule — no recurring-specific contract. Works identically on
Hangfire and RabbitMQ.

```csharp
// Explicit cron (payload type inferred from the handler; use AddRecurringJob<TPayload, THandler>(...) to state it)
services.AddRecurringJob<SendDigestJob>(s => s
    .WithId("SendDigest")
    .WithCron("0 7 * * *")        // 5- or 6-field cron
    .WithQueue("maintenance"));   // optional

// With a parameterized payload — the factory runs once per occurrence, so you can pass arguments
// (or compute per-run values). Build via AbstractTypeFactory inside the factory to keep it overridable.
services.AddRecurringJob<SendDigestPayload, SendDigestJob>(
    () => new SendDigestPayload { Top = 10, Period = "daily" },
    s => s.WithId("SendDigest").WithCron("0 7 * * *"));

// Setting-driven (enabler on/off + cron setting; re-applied live when either setting changes)
services.AddRecurringJob<PrunePayload, PruneHandler>(s => s
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

The map/reduce contracts live in `VirtoCommerce.BackgroundJobs.Core` (a packable NuGet), so a consuming module
references that package (in addition to the manifest dependency on the engine module).

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

// 4) Enqueue a batch — partition into PAGES (not individual documents) to keep the task count sane.
//    TItem/TState are inferred from the arguments; TResult is stated explicitly (it appears in no argument).
var pages = allProductIds.Chunk(50).Select(ids => new IndexPage("Product", ids));
await _mapReduce.Enqueue<IndexPage, IndexPageResult, IndexSummary>(
    items: pages,
    state: new IndexSummary("Product", DateTime.UtcNow.Ticks),
    options: new MapReduceOptions { Queue = "indexing", FailurePolicy = FailurePolicy.ContinueOnError, ReportProgress = true });
```

**What the parameters mean:**

- **`items`** — the work split into independent units. The engine creates **one map task per item** and runs them in
  parallel across all workers. Partition *coarsely* — a page of ids, not a single document — so the task count and
  each `TResult` stay small (`items` is your fan-out width).
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
| Expression enqueue (optional) | Implement `IExpressionJobEngine` only if your engine supports it. Most don't — the facade already throws `NotSupportedException` for expression enqueue when it's absent. |
| Processing host | An `IHostedService` (like `RabbitMqJobConsumer`) that consumes/receives and calls `IJobDispatcher.Dispatch(envelope, context, ct)`. Register it only when active **and** `Mode != Producer`. A push engine instead exposes an inbound callback controller. |
| Recurring | Either call `services.AddInProcessRecurringScheduler()` (reuses the Cronos + distributed-lock + occurrence-marker scheduler, enqueues via `IBackgroundJob`), **or** implement `IRecurringJobScheduler` natively. |
| Reuse — do **not** reimplement | `IBackgroundJob`, `IJobDispatcher`, `IJobPayloadSerializer`, `JobEnvelope`, `JobExecutionContext`, progress, `RecurringJobsApplier`, `IRecurringJobStateStore` + its Redis/in-memory impls. The host module registers these once. |

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
reuses `IJobDispatcher` — no new platform contract needed.

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
