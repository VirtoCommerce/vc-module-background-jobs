# Design — Engine-agnostic background-job cancellation

**Date:** 2026-08-06
**Ticket:** VCST-5490 (platform gap: `Delete`/`Cancel` not in the engine-agnostic API)
**Scope:** `vc-platform` (Platform.Core), `vc-module-background-jobs` (facade + engines), verified against `vc-module-image-tools`.
**Versions observed:** platform 3.1052.0, `VirtoCommerce.BackgroundJobs` 3.1050.0 (module dev branch).

## 1. Context & problem

The engine-agnostic facade `IBackgroundJob` (Platform.Core) exposes only `Enqueue`. Cancellation exists on the engine
port `IJobEngine.Delete(jobId)` — but that lives in the **module** package (`VirtoCommerce.BackgroundJobs.Core`), so a
consumer can only reach it by taking a compile-time dependency on the module (abandoning the engine-agnostic,
runtime-only model). There is also no REST surface for it.

Concretely, `vc-module-image-tools` `ThumbnailsTasksController.Cancel` calls Hangfire's static
`BackgroundJob.Delete(jobId)`, which no longer compiles under the new model. It needs an engine-agnostic cancel.

The hard part: **RabbitMQ can't recall a published message by id** — cancellation is a capability it lacks natively.
Simply forwarding `Delete` to the facade would make Cancel a **silent no-op** on RabbitMQ.

## 2. Goals / non-goals

**Goals**
1. Add cancellation to the engine-agnostic API **without breaking changes**.
2. Model cancellation as a **capability an engine may not have**, with a probe so degradation is *visible*, never silent.
3. Real cancellation on **Hangfire**.
4. Real cancellation on **RabbitMQ** for both **not-started** and **started** jobs, defined cooperatively, and with a
   clear account of the impact on existing behavior.
5. Unblock `vc-module-image-tools` (its cancel endpoint) on both engines.

**Non-goals (separate ticket)**
- `search` module: enumerate running jobs by handler type, runtime recurring on/off toggle, `IIndexingJobService`
  cross-module contract. Explicitly out of scope.
- Enumerating running jobs in the agnostic API.
- Batch/map-reduce cancellation by `batchId` (noted as a follow-on; a single `jobId` cancel does not stop a fan-out).

## 3. Design

### 3.1 Public API (Platform.Core, additive)

Add two **default interface members** to `IBackgroundJob` so existing external implementers and callers are unaffected:

```csharp
/// Best-effort, engine-dependent cancellation. A not-started job is prevented from running; a running job is asked
/// to stop via its CancellationToken. Returns true if the active engine accepted the request. Prefer checking
/// SupportsCancellation first so a UI/API can distinguish "not supported" from "nothing to cancel".
Task<bool> Cancel(string jobId, CancellationToken cancellationToken = default) => Task.FromResult(false);

/// Whether the active engine supports on-demand cancellation.
bool SupportsCancellation => false;
```

Design choice: **return-false + capability probe** (not a throwing default like `Enqueue(Type,…)`), because "cannot
cancel" is a normal, queryable state, not an exceptional one. The static `BackgroundJob` facade gets the same two
members (symmetry with `Enqueue`).

### 3.2 Engine port & capability (module)

`IJobEngine` gains `bool SupportsCancellation => false;` (default interface member). Built-in engines override:
Hangfire = `true`, RabbitMQ = `true` (via the cooperative store below), In-Memory = `true`. The facade
(`JobEngineBackgroundJob`) implements `SupportsCancellation => engine?.SupportsCancellation ?? false` and
`Cancel(jobId) => engine is null ? false : engine.Delete(jobId, ct)`. (`IJobEngine.Delete` keeps its name/signature.)

### 3.3 Hangfire — already works

`HangfireJobEngine.Delete` already calls `Hangfire.BackgroundJob.Delete(jobId)`: a not-started job is removed from the
queue; a running job is transitioned to *Deleted* and its injected `CancellationToken` (already flowed through
`HangfireJobExecutor` into the dispatcher/handler) is cancelled. Only change: `SupportsCancellation => true`. No other
behavioral change to existing jobs.

### 3.4 RabbitMQ — cooperative cancellation via a shared store

New contract `IJobCancellationStore` (Core), Redis + in-memory implementations (Data), selected by the same
`IConnectionMultiplexer`-present idiom as the map/reduce and recurring stores:

```csharp
public interface IJobCancellationStore
{
    Task RequestCancel(string jobId, CancellationToken ct = default);      // set flag (+ publish for low latency)
    Task<bool> IsCancelRequested(string jobId, CancellationToken ct = default);
    Task Clear(string jobId, CancellationToken ct = default);             // cleanup once the job settles
}
```

- **Redis impl:** key `vc:jobs:cancel:{jobId}` (SET with a TTL that outlives the max job duration; configurable) +
  `PUBLISH vc:jobs:cancel {jobId}` for low-latency running-job cancel. `IsCancelRequested` = key EXISTS. `Clear` = DEL.
- **In-memory impl:** `ConcurrentDictionary` + in-process notification — single-instance only (dev/test).

`RabbitMqJobEngine`:
- `SupportsCancellation => true`.
- `Delete(jobId)` → `await _cancellationStore.RequestCancel(jobId); return true;` (request accepted).

`RabbitMqJobConsumer.OnReceivedAsync`:
- **Not started:** before dispatch, `if (await _cancellationStore.IsCancelRequested(jobId)) { ack + skip + log; return; }`
  — the message is discarded on pickup and never runs. (It still occupies the queue until dequeued.)
- **Started:** keep a `ConcurrentDictionary<string, CancellationTokenSource>` of in-flight jobs; subscribe once (at
  consumer start) to the `vc:jobs:cancel` pub/sub channel; on a message, if that `jobId` is in-flight locally,
  `cts.Cancel()`. Pass `cts.Token` to `dispatcher.Dispatch(envelope, context, cts.Token)` — **replacing today's
  `CancellationToken.None`**. In `finally`: remove from the dict and `Clear(jobId)`.

### 3.5 REST (optional, generic)

Add `POST api/platform/jobs/{id}/cancel` to the platform's `JobsController`, guarded by `background_jobs:manage`:
`SupportsCancellation == false` → 501; else `Cancel(id)` → 200 (`{canceled:true|false}`). Consumer modules that inject
the facade (like image-tools) don't need this; it's for generic callers.

## 4. Effect on current solutions

- **Hangfire:** no behavioral change to existing jobs; cancel behaves exactly as before.
- **RabbitMQ:** the consumer now passes a *real* `CancellationToken` (was `None`). Handlers that ignore it are
  unaffected; handlers that honor it become cancellable. Added cost per delivered message: one cheap Redis `EXISTS`
  check; plus a per-job CTS and one long-lived pub/sub subscription. Fleet-wide cancel requires Redis (already required
  in a fleet); without Redis the in-memory store is single-instance only.
- **No breaking changes:** every addition is additive (default interface members, a new store, an optional REST route).
  Already-built modules and the six migrated modules are unaffected.
- **Map/reduce:** a single `jobId` cancel does not stop a fan-out (N messages) — follow-on ticket.

## 5. Usage — `vc-module-image-tools`

```csharp
// ThumbnailsTasksController — inject the engine-agnostic facade
public class ThumbnailsTasksController(IBackgroundJob backgroundJob) : Controller
{
    [HttpPost("{jobId}/cancel")]
    public async Task<ActionResult> Cancel([FromRoute] string jobId, CancellationToken ct)
    {
        if (!backgroundJob.SupportsCancellation)
            return StatusCode(StatusCodes.Status501NotImplemented, "Cancellation not supported by the active engine.");

        var canceled = await backgroundJob.Cancel(jobId, ct);
        return canceled ? Ok() : NotFound();
    }
}
```

The thumbnail-generation handler honors the `CancellationToken` it receives in
`Execute(payload, IJobExecutionContext ctx, CancellationToken ct)` — the same role `IJobCancellationToken` played on
Hangfire. (The module's other Hangfire constructs already have engine-agnostic replacements: distributed lock →
`IDistributedLockService`; `PerformContext.BackgroundJob.Id` → `IJobExecutionContext.JobId`;
`[AutomaticRetry(0)]` → `EnqueueOptions.MaxRetryAttempts = 0`.)

## 6. Files to change

**vc-platform / Platform.Core**
- `Jobs/IBackgroundJob.cs` — add `Cancel` + `SupportsCancellation` default members.
- `Jobs/BackgroundJob.cs` — add static `Cancel` (+ `SupportsCancellation`).

**vc-module-background-jobs**
- `Core/IJobEngine.cs` — add `SupportsCancellation` default member.
- `Core/Cancellation/IJobCancellationStore.cs` — new contract.
- `Data/Cancellation/{Redis,InMemory}JobCancellationStore.cs` + DI selector — new.
- `Data/Services/JobEngineBackgroundJob.cs` — implement `Cancel` + `SupportsCancellation`.
- `Hangfire/HangfireJobEngine.cs` — `SupportsCancellation => true`.
- `RabbitMQ/RabbitMqJobEngine.cs` — `SupportsCancellation => true`; `Delete` → `RequestCancel`.
- `RabbitMQ/RabbitMqJobConsumer.cs` — pre-dispatch flag check; per-job CTS + pub/sub; pass the token to `Dispatch`.
- `InMemory/InMemoryJobEngine.cs` — `SupportsCancellation => true` (optional: honor the store).
- `Web/PlatformStartup.cs` — register `IJobCancellationStore`; wire it into the RabbitMQ consumer/engine.
- `Web/Controllers/Api/JobsController.cs` — optional `POST {id}/cancel`.
- README — document cancellation + capability probe + engine matrix.

**vc-module-image-tools** (verification target)
- `Web/Controllers/Api/ThumbnailsTasksController.cs` — use `IBackgroundJob.Cancel`; ensure the handler honors `ct`.

## 7. Testing

- **Unit:** facade `Cancel` delegates to `engine.Delete`; `SupportsCancellation` surfacing (incl. no-engine → false);
  in-memory store set/get/clear; `RabbitMqJobEngine.Delete` sets the flag + returns true; consumer discards a flagged
  not-started message and cancels the CTS for a running one.
- **Conformance:** add a cancellation capability case (engines with `SupportsCancellation` — enqueue a long job, cancel,
  assert it stops / status reflects it).
- **Integration (image-tools):** Hangfire — enqueue a thumbnail task, cancel, verify removed/stopped. RabbitMQ — same,
  verify the handler's `ct` trips and the job stops.

## 8. Phasing

The API layer (§3.1–3.2, Hangfire §3.3) is independent of the RabbitMQ cooperative store (§3.4). Ship them together,
but the phasing is available: Hangfire cancel can land first with zero API churn, then the RabbitMQ store follows.
Once done, image-tools cancel works on both engines.

## 9. Open questions
- Cancel-flag TTL default (proposal: a few hours, ≥ realistic max job duration; configurable).
- Whether to also expose `GetStatus` on the facade in the same pass (the gap doc lists it as a sibling gap). Proposal:
  keep this ticket to cancellation; treat status as its own small additive follow-up.
