# Background-Job Engine Conformance Kit

`VirtoCommerce.BackgroundJobs.Conformance` is a reusable xUnit test suite that certifies a background-job **engine**
(`IJobEngine`) against the Virto Commerce background-job standard. Implement your engine, write one small fixture
that wires it against **real** infrastructure, derive one test class, and run it. **Green = conformant.**

It is the regression net for the standard too: every feature has one clear scenario, so a future change that breaks
an engine surfaces immediately.

## What it certifies

The suite drives the real enqueue → dispatch path (`IBackgroundJob` → `IJobEngine` → the worker → `IJobDispatcher`
→ your handler) and asserts the observable contract. Each scenario uses a unique correlation id and waits on a
completion **signal** from the handler (not a `Task.Delay` guess), so it is deterministic regardless of how your
engine schedules work.

Scenarios are numbered by the comment above each test in `JobEngineConformanceTests.cs`; that comment is the source of
truth for the numbers. The map/reduce and recurring scenarios live in two more files of the same partial class
(`MapReduceConformanceTests.cs`, `RecurringJobSchedulerConformanceTests.cs`) and carry comment numbers of their own,
which overlap the numbers of the first file, so refer to them by test name.

**Core and delivery**

| # | Scenario | Applies to |
|---|---|---|
| 1 | Enqueue dispatches the handler with the payload | every engine |
| 2 | An `AbstractTypeFactory`-derived payload round-trips as the concrete type | every engine |
| 3 | The enqueuing user is restored in the handler scope | every engine |
| 4 | Enqueue returns a non-empty job id | every engine |
| 5 | A failing handler does not stop the worker (a later job still runs) | every engine |
| 6 | `GetStatus` contract: a tracked status, or `null`/not-completed, never a false completion | every engine; `SupportsStatusQuery` picks which is asserted |
| 7 | `Delete` of an unknown id returns `false` (a ledgerless cooperative-cancel engine, `SupportsCancellation` without `SupportsStatusQuery`, accepts any id and returns `true`) | every engine |
| 8 | `UniqueKey` collapses duplicate enqueues to one execution | `SupportsUniqueKeyDedup` (skipped otherwise) |
| 10 | A job on a non-default queue is still drained | `SupportsQueueRouting` (skipped otherwise) |
| 11 | A transient failure is retried until it succeeds | `SupportsRetry` (skipped otherwise) |
| 12 | Progress reported by the handler surfaces on the notification | every engine |
| 24 | **One job id:** the worker's context carries the id `Enqueue` returned | every engine, no flag |
| 25 | The same job id on every attempt of a retry, in the context and in every update the worker sends to the notification (the facade's first "Queued" send carries no job id by design) | `SupportsRetry` (skipped otherwise) |

**Cancellation**

| # | Scenario | Applies to |
|---|---|---|
| 13 | A running job is cancelled (`Delete` accepted) and its handler's `CancellationToken` trips | `SupportsCancellation` (skipped otherwise) |
| 17 | A requested cancel of a **running** job closes its notification `Canceled`, without an error and with a finish time. The notification must be the job's own: enqueue with `ReportProgress` and no `ProgressNotificationId`, so the worker closes it, and cancel only after the handler signalled it started. With `ReportsAttemptBudget` the close keeps the attempt numbers of the update before it | `SupportsCancellation`; `ClosesCanceledNotification` picks the assertion: when false the fallback is asserted instead (after the handler observed the cancel, the notification stays open) |
| 18 | The same for a job **canceled while still queued** — enqueued onto `UnservedQueue`, which no worker drains — so no worker will ever close it; the handler never runs, and with `ReportsAttemptBudget` the close shows attempt 1 of the job's total | `CanHoldQueuedJob` and `SupportsDelete` (skipped otherwise); `ClosesCanceledNotification` picks `Canceled` or the stays-open fallback |
| 19 | **Cancel, then the handler fails with another exception** (`FailOnCancel`) on a non-final attempt: a retry was pending, but the cancel wins — the notification closes `Canceled` with no error and no retry announcement in its description, and the handler does not run again | `SupportsCancellation`, `SupportsRetry` and `ClosesCanceledNotification` (skipped otherwise) |
| 20 | The same on the **final** attempt (`MaxRetryAttempts = 0`) is a failure: the notification closes `Failed` with the handler's error, and nothing replaces that close with `Canceled` afterwards | `SupportsCancellation`, `ClosesCanceledNotification` and `ReportsAttemptBudget` (skipped otherwise) |
| 21 | `Delete` of a job that already **finished** returns `false` and the job keeps its `Succeeded` state | `SupportsStatusQuery` and `SupportsDelete` (skipped otherwise) |

**Notification attempts, outcome and the per-job retry limit**

| # | Scenario | Applies to |
|---|---|---|
| 14 | A failed attempt with a retry pending keeps the notification open and says which attempt failed (`Attempt 1 of N failed, retrying`), with `Attempt` and `TotalAttempts` set and no outcome or finish time | `SupportsRetry` (skipped otherwise); `ReportsAttemptBudget` picks the assertion: when false the first failure closes the notification `Failed` (every attempt counts as final) |
| 15 | The failure that exhausts the attempts closes the notification `Failed`, with the error and the finish time; with `ReportsAttemptBudget`, `Attempt` equals `TotalAttempts` | every engine |
| 16 | A job that returns closes its notification `Succeeded`, with no error and a finish time | every engine |
| 22 | `EnqueueOptions.MaxRetryAttempts` **above** the engine's global count runs the job limit + 1 times, and the notification's `TotalAttempts` is limit + 1 | `SupportsRetry` and `ReportsAttemptBudget` (skipped otherwise) |
| 23 | `MaxRetryAttempts` **below** the global count stops the job early: limit + 1 runs | `SupportsRetry` and `ReportsAttemptBudget` (skipped otherwise); needs a `RetryBudget` above 1 |

**Map/reduce and recurring** (comment numbers in their own files)

| # | Scenario | Applies to |
|---|---|---|
| 13 | `MapReduce_FanOut_Then_Reduce_Aggregates_All_Results`: map/reduce fans out and aggregates once | every engine |
| 14 | `MapReduce_ContinueOnError_Runs_Reduce_With_Failures_Included`: `ContinueOnError` includes the failed item | every engine |
| 15 | `Recurring_Scheduler_AddOrUpdate_Then_Remove_DoNotThrow`: the recurring scheduler accepts add and remove | `SupportsRecurringScheduler` (skipped otherwise) |
| 16 | `Recurring_Trigger_Enqueues_Runnable_Job`: a recurring trigger enqueues a runnable job | every engine |

Capability-gated scenarios assert the documented behavior when supported, and the documented fallback (or skip) when
not — so an engine is never penalized for a feature it legitimately cannot provide. A scenario that asserts a closing
send waits on the notification itself (`WaitForProgressAsync`), never on a `Task.Delay` guess; only a scenario that
asserts an event did **not** happen (a close an engine is documented not to make, a handler that does not run again, a
wrongful second execution in 8) can only wait, and it waits after the event it was waiting for was observed — one
second in the cancel scenarios, two in 8.

## Capability flags

`EngineCapabilities` declares which optional parts of the standard an engine supports. **Every flag defaults to
`false`**, so a gated scenario of a fixture written against an earlier version of the kit passes after a package
upgrade — on the fallback, or skipped — until it declares the new flag. A scenario with no flag runs on every engine,
so an engine that breaks it fails after the upgrade whatever its fixture declares. That includes
`Worker_JobId_Equals_Enqueue_Result` (one job id), `Final_Failure_Closes_Notification_Failed` and
`Success_Closes_Notification_Succeeded`, which an engine written before them may not satisfy.

| Flag | Declare `true` when | Scenarios |
|---|---|---|
| `SupportsStatusQuery` | `GetStatus` returns a meaningful status for an enqueued job (Hangfire, InMemory). False: the engine keeps no job ledger and must never report a false completion (RabbitMQ) | 6, 7, 21 |
| `SupportsDelete` | `Delete` can cancel or remove a known job | 18, 21 |
| `SupportsUniqueKeyDedup` | re-enqueuing with the same `UniqueKey` collapses to one execution. **No shipped engine does**; keep it false unless yours truly does | 8 |
| `SupportsQueueRouting` | a job on a non-default queue is processed on this instance | 10 |
| `SupportsRetry` | a handler that throws is retried up to `MaxRetryAttempts` | 11, 14, 19, 22, 23, 25 |
| `SupportsRecurringScheduler` | the engine registers an `IRecurringJobScheduler` (native, or the in-process one) | `Recurring_Scheduler_AddOrUpdate_Then_Remove_DoNotThrow` |
| `SupportsCancellation` | `IJobEngine.SupportsCancellation` is true and a running job can be cancelled on demand | 7, 13, 17, 19, 20 |
| `ReportsAttemptBudget` | the engine fills `Attempt` and `TotalAttempts` on the envelope before `Dispatch`. False: `TotalAttempts` stays null and every attempt counts as final | 14, 15, 17, 18, 19, 20, 22, 23 |
| `ClosesCanceledNotification` | a requested cancel (`Delete`) closes the job's notification `Canceled`, because the engine can tell a requested cancel from a shutdown or a lost lock. False: the notification stays open after a cancel | 17, 18, 19, 20 |
| `CanHoldQueuedJob` | the engine can keep a job queued without running it, so a scenario can cancel a job that never ran (enqueued onto `UnservedQueue`). Only Hangfire declares it: an unserved queue is never consumed on RabbitMQ, and in-process engines run a job at once | 18 |

The fixture adds two members the scenarios read, both overridable:

- **`RetryBudget`** (default `3`) — the engine's global retry count, i.e. the retries a job gets on top of its first
  run when the enqueue does not override it. A job gets `RetryBudget + 1` attempts. Configure your engine with this
  value (`VirtoCommerce:BackgroundJobs:MaxRetryAttempts`), and a scenario states its expectation as `RetryBudget + 1`,
  never as a literal.
- **`UnservedQueue`** (default `conformance-unserved`) — a queue no worker of the engine under test drains.

## What an engine must do, in kit terms

These are the engine duties the scenarios above assert; the README's
[custom-engine section](../../README.md#what-a-custom-engine-owes-the-standard) carries the explanation.

- **One job id.** The id passed to the worker context equals the id `Enqueue` returned, on every attempt (24, 25). An
  engine that passes another id — a short name for a full one — breaks the progress page with no error anywhere.
- **Attempts.** Fill `Attempt` and `TotalAttempts` on the envelope before `Dispatch` and build the worker context from
  that envelope: `TotalAttempts = Math.Max(0, envelope.MaxRetryAttempts ?? global) + 1` (14, 15, 22, 23).
- **Per-job retry limit.** `EnqueueOptions.MaxRetryAttempts` overrides the global count both ways (22, 23).
- **Close `Canceled`.** On a requested cancel, close the notification through `context.Progress.Complete(JobOutcome.Canceled)`,
  for a running job, a queued one, and a job waiting for a retry, showing the attempt it was canceled at and the job's
  total (17–19). On the final attempt a handler that then fails with another exception stays `Failed` (20).
- **Refuse `Delete` of a finished job** and leave its state alone (21).

`JobProgressPushNotification.Outcome` is a **string** — `Succeeded`, `Failed` or `Canceled`, the name of the
`JobOutcome` value — so the scenarios compare it with `nameof(JobOutcome.…)`.

## The conformance payload and handler

Every scenario enqueues a `ConformancePayload` against the one `RecordingConformanceHandler`. The handler records each
execution on the `ConformanceProbe` (keyed by the payload's `CorrelationId`, unique per scenario) and signals the probe
instead of making the test guess when it finished. A payload mode is a public member of the payload:

| Member | Effect | Used by |
|---|---|---|
| `CorrelationId` | Keys the probe's signals and observations; unique per scenario. | all |
| `Value` | A string the handler records; scenario 1 asserts it arrived intact. | 1 |
| `FailAttempts` | Throw on the first N executions of the correlation id, then succeed. `0` = never throw. | 5 (`int.MaxValue`, a poison job), 11, 14, 25 |
| `FailAlways` | Throw on every execution — drives the exhausted-retries (final failure) path. Checked before `FailAttempts`. | 15, 22, 23 |
| `ReportProgress` | Report one progress update (`ConformanceConstants.ProgressMessage`) from the handler. | 12, 14, 15, 16, 17, 19, 20, 22, 23, 25 |
| `BlockUntilCancelled` | Signal "started", then block until the handler's `CancellationToken` trips; on cancel, signal "cancelled" and rethrow the cancellation. With `ReportProgress` the update is sent before "started" is signalled, so a scenario that saw the start also saw the update. | 13, 17, 19, 20 |
| `FailOnCancel` | With `BlockUntilCancelled`: once the token trips, throw an `InvalidOperationException` instead of rethrowing the cancellation — a handler that fails after it was cancelled. False = rethrow. | 19, 20 |

`ExtendedConformancePayload` derives from it with one extra member, `ExtraValue`, registered with
`AbstractTypeFactory`; scenario 2 asserts it round-trips as the concrete type.

The fixture and probe give a scenario the waits it needs, so a new scenario never has to poll by hand:

- `Probe.WaitAsync`, `WaitStartedAsync`, `WaitCancelledAsync` — completion, "the handler is running" and "the handler
  saw the cancel". They are plain timed waits on the probe's signal: on timeout they throw a `TimeoutException` with the
  framework's generic message, which does not say what was awaited. `Probe.ExecutionCount`, `Probe.ExecutionJobIds`
  and `Probe.Job` read what the handler recorded.
- `Fixture.WaitForProgressAsync(notificationId, predicate, timeout)` — the first send to a notification that satisfies
  the predicate; on timeout the `TimeoutException` names the notification, the predicate and every update that did arrive.
- `Fixture.WaitForStatusAsync(jobId, predicate, timeout)` — polls `GetStatus` until the predicate holds; on timeout the
  `TimeoutException` names the job, the predicate and the last state read.
- `Fixture.ProgressUpdates(notificationId)` — the sends to one notification in arrival order; `LastProgressUpdate(title)`.
- `ConformanceConstants.DefaultTimeout` and `CancellationTimeout` (30 s each; cooperative engines poll the shared store
  on an interval).

## How to certify your engine

1. Reference this package from your engine's test project (it brings `xunit.v3`).
2. Write a fixture deriving `JobEngineConformanceFixture`: declare your capabilities, register your **real** engine
   and its worker, and (optionally) gate on a connection string so the suite skips when infra is absent.
3. Derive one test class from `JobEngineConformanceTests<TFixture>`.
4. `dotnet test`.

```csharp
public sealed class MyEngineConformanceFixture : JobEngineConformanceFixture
{
    public override EngineCapabilities Capabilities => new()
    {
        SupportsStatusQuery = true,
        SupportsDelete = true,
        SupportsQueueRouting = true,
        SupportsRetry = true,
        SupportsRecurringScheduler = true,
        SupportsCancellation = true,           // + a Delete that trips the handler's token
        ReportsAttemptBudget = true,           // you fill Attempt and TotalAttempts on the envelope
        ClosesCanceledNotification = true,     // you close a requested cancel through Progress.Complete(JobOutcome.Canceled)
        // CanHoldQueuedJob: only if a job can sit queued on fixture.UnservedQueue without running.
        // SupportsUniqueKeyDedup: only if your engine truly does it.
    };

    public override int RetryBudget => 3;      // the global retry count you configure your engine with

    protected override bool TryConfigureEngine(IServiceCollection services, out string? unavailableReason)
    {
        var connection = Environment.GetEnvironmentVariable("MY_ENGINE_CONNECTION");
        if (string.IsNullOrWhiteSpace(connection))
        {
            unavailableReason = "set MY_ENGINE_CONNECTION to run the conformance suite.";
            return false; // the suite skips with this message instead of failing
        }
        unavailableReason = null;

        services.AddSingleton<IJobEngine, MyJobEngine>();   // REQUIRED
        services.AddHostedService<MyJobConsumer>();          // your worker — base StartWorkerAsync starts all IHostedServices
        // Recurring: register a native IRecurringJobScheduler, or call services.AddInProcessRecurringScheduler()
        //            (+ an IDistributedLockService) to reuse the generic cron scheduler.
        return true;
    }

    // Override StartWorkerAsync/StopWorkerAsync only if starting your worker needs more than starting IHostedServices.
}

public class MyEngineConformanceTests(MyEngineConformanceFixture fixture)
    : JobEngineConformanceTests<MyEngineConformanceFixture>(fixture);
```

The base builds the engine-agnostic services the standard depends on (serializer, dispatcher, the `IBackgroundJob`
facade, map/reduce orchestration, the conformance handlers, a capturing push-notification manager). You supply only
the engine and its worker.

## Reference examples in this repo

- **`ReferenceEngineConformanceTests`** — an in-process `ReferenceJobEngine` (no infrastructure). Always runs; it is
  the kit's own smoke test and the copy-paste template.
- **`InMemoryEngineConformanceTests`** — the shippable `InMemoryJobEngine` (the `Provider=InMemory` local/testing
  engine) with no infrastructure. Always runs.
- **`HangfireConformanceTests`** — the real Hangfire engine + a real Hangfire server, in-process (in-memory storage
  by default; point `VirtoCommerce:Hangfire:JobStorageType` at a database to certify persistent storage). It adds
  Hangfire-only scenarios: a requeued job whose retries are spent closes `Failed`, a running job moved to `Deleted`
  without the engine (the dashboard's Delete) closes `Canceled`, and a running job that is requeued stays open.
- **`RabbitMqConformanceTests`** — the real RabbitMQ engine + consumer against a real broker. Set
  `VC_CONFORMANCE_RABBITMQ` to an AMQP uri (e.g. `amqp://guest:guest@localhost:5672/`). **It runs only when that variable
  is set and skips every scenario otherwise.** CI sets none, so a green CI run says nothing about RabbitMQ: run it
  against a broker before you trust a change to the RabbitMQ engine.

> Google Cloud Tasks is not certified by the live suite (it needs GCP credentials and a publicly reachable callback);
> its request-builder and callback unit tests remain its coverage, and this kit can certify it once deployed.

## Notes

- `EnqueueOptions` exposes no headers, so header propagation is not a scenario; `IJobExecutionContext.Headers` is
  always the envelope's (empty) headers.
- Map/reduce runs single-instance against the in-memory batch store; the fleet-safe Redis store is exercised by the
  module's own tests.
