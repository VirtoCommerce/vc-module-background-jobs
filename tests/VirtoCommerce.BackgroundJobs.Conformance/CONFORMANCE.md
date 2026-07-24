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

| # | Scenario | Applies to |
|---|---|---|
| 1 | Enqueue dispatches the handler with the payload | every engine |
| 2 | An `AbstractTypeFactory`-derived payload round-trips as the concrete type | every engine |
| 3 | The enqueuing user is restored in the handler scope | every engine |
| 4 | Enqueue returns a non-empty job id | every engine |
| 5 | A failing handler does not stop the worker (a later job still runs) | every engine |
| 6 | `GetStatus` contract (tracked status, or documented `Unknown`/not-completed) | `SupportsStatusQuery` |
| 7 | `Delete` of an unknown id returns `false` | every engine |
| 9 | `UniqueKey` collapses duplicate enqueues to one execution | `SupportsUniqueKeyDedup` |
| 10 | A job on a non-default queue is still drained | `SupportsQueueRouting` |
| 11 | A transient failure is retried until it succeeds | `SupportsRetry` |
| 12 | Progress reported by the handler surfaces on the notification | every engine |
| 13–14 | Map/reduce fans out, aggregates once; `ContinueOnError` includes failures | every engine |
| 15–16 | The recurring scheduler accepts add/remove; a recurring trigger enqueues a runnable job | `SupportsRecurringScheduler` |

Capability-gated scenarios assert the documented behavior when supported, and the documented fallback (or skip) when
not — so an engine is never penalized for a feature it legitimately cannot provide.

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
        // SupportsUniqueKeyDedup: only if your engine truly does it.
    };

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
- **`HangfireConformanceTests`** — the real Hangfire engine + a real Hangfire server, in-process (in-memory storage
  by default; point `VirtoCommerce:Hangfire:JobStorageType` at a database to certify persistent storage).
- **`RabbitMqConformanceTests`** — the real RabbitMQ engine + consumer against a real broker. Set
  `VC_CONFORMANCE_RABBITMQ` to an AMQP uri (e.g. `amqp://guest:guest@localhost:5672/`); skips when unset.

> Google Cloud Tasks is not certified by the live suite (it needs GCP credentials and a publicly reachable callback);
> its request-builder and callback unit tests remain its coverage, and this kit can certify it once deployed.

## Notes

- `EnqueueOptions` exposes no headers, so header propagation is not a scenario; `IJobExecutionContext.Headers` is
  always the envelope's (empty) headers.
- Map/reduce runs single-instance against the in-memory batch store; the fleet-safe Redis store is exercised by the
  module's own tests.
