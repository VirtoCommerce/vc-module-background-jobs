# Background-Job Cancellation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add engine-agnostic, capability-probed job cancellation to `IBackgroundJob` — real on Hangfire, cooperative on RabbitMQ (not-started + started) — without breaking changes, and use it from `vc-module-image-tools`.

**Architecture:** Add `Cancel` + `SupportsCancellation` as **default interface members** on `IBackgroundJob` (Platform.Core) and `SupportsCancellation` on `IJobEngine`. The facade delegates `Cancel` → `IJobEngine.Delete`. Hangfire already deletes/cancels for real. RabbitMQ (which can't recall a message) cancels cooperatively via a shared `IJobCancellationStore` (Redis, with in-memory fallback): the consumer discards a flagged not-started message on dequeue, and cancels a per-job `CancellationTokenSource` (polled against the store) for a running job — the handler's `CancellationToken` trips.

**Tech Stack:** .NET 10, C#, xUnit v3 + Moq, StackExchange.Redis, RabbitMQ.Client 7.x, Hangfire.

**Repos:** `vc-platform` (Platform.Core), `vc-module-background-jobs` (facade + engines + tests), `vc-module-image-tools` (consumer).

**Spec:** `docs/superpowers/specs/2026-08-06-background-job-cancellation-design.md`.

---

## File structure

**vc-platform / Platform.Core**
- `Jobs/IBackgroundJob.cs` (modify) — add `Cancel`, `SupportsCancellation` default members.
- `Jobs/BackgroundJob.cs` (modify) — static `Cancel`, `SupportsCancellation`.

**vc-module-background-jobs**
- `src/…Core/IJobEngine.cs` (modify) — `SupportsCancellation` default member.
- `src/…Core/Cancellation/IJobCancellationStore.cs` (create).
- `src/…Data/Cancellation/InMemoryJobCancellationStore.cs` (create).
- `src/…Data/Cancellation/RedisJobCancellationStore.cs` (create).
- `src/…Data/Cancellation/CancellationStoreServiceCollectionExtensions.cs` (create) — `AddJobCancellationStore()` selector.
- `src/…Data/Services/JobEngineBackgroundJob.cs` (modify) — implement `Cancel` + `SupportsCancellation`.
- `src/…Hangfire/HangfireJobEngine.cs` (modify) — `SupportsCancellation => true`.
- `src/…RabbitMQ/RabbitMqJobEngine.cs` (modify) — `SupportsCancellation => true`; `Delete` → store.
- `src/…RabbitMQ/RabbitMqJobConsumer.cs` (modify) — flag-check + per-job CTS + watch loop + real token.
- `src/…InMemory/InMemoryJobEngine.cs` (modify) — `SupportsCancellation => true`.
- `src/…Web/PlatformStartup.cs` (modify) — register the store; inject into RabbitMQ.
- `src/…Web/Controllers/Api/JobsController.cs` (modify) — optional `POST {id}/cancel`.
- `tests/…Tests/*` (create/modify) — unit + conformance tests.
- `README.md` (modify) — document cancellation.

**vc-module-image-tools**
- `src/…Web/Controllers/Api/ThumbnailsTasksController.cs` (modify) — use `IBackgroundJob.Cancel`.

---

## Task 0: Local build/pack workflow for the Platform.Core change

Platform.Core gains new members; until the platform publishes a release with them, build the module against a locally-packed Platform. (These commands mirror the session's established local-feed workflow.)

- [ ] **Step 1: Re-enable the local platform feed mapping** (the module currently maps `VirtoCommerce.Platform.*` to nuget.org). In `vc-module-background-jobs/nuget.config`, inside `<packageSourceMapping>`, add the local source block back:

```xml
<packageSource key="local-platform">
  <package pattern="VirtoCommerce.Platform.*" />
  <package pattern="VirtoCommerce.Testing" />
</packageSource>
```

- [ ] **Step 2: (after Tasks 1–2 land in Platform.Core) pack Platform to the local feed at a new alpha and re-pin the module.** Choose a new version, e.g. `3.1053.0-alpha.cancel`:

```bash
cd /c/Projects/git/VirtoCommerce/vc-platform
dotnet pack VirtoCommerce.Platform.sln -c Debug -o /c/Projects/git/VirtoCommerce/local-nuget -p:PackageVersion=3.1053.0-alpha.cancel --nologo -v q
rm -rf /c/Users/Admin/.nuget/packages/virtocommerce.platform.core/3.1053.0-alpha.cancel
```
Then bump every `VirtoCommerce.Platform.Core` / `VirtoCommerce.Platform.Data` `<PackageReference Version>` in the module to `3.1053.0-alpha.cancel`.

- [ ] **Step 3:** No commit (workflow/config only). Note: revert `nuget.config` and the version pins to the released platform version once it ships the cancellation members.

---

## Task 1: Platform.Core — `IBackgroundJob.Cancel` + `SupportsCancellation`

**Files:**
- Modify: `vc-platform/src/VirtoCommerce.Platform.Core/Jobs/IBackgroundJob.cs`

- [ ] **Step 1: Add the two default members** after the existing `Enqueue(Type, …)` member, inside the interface:

```csharp
    /// <summary>
    /// Request cancellation of a previously enqueued job by id. Best-effort and engine-dependent: a not-yet-started
    /// job is prevented from running; a running job is asked to stop via its <see cref="System.Threading.CancellationToken"/>.
    /// Returns true if the active engine accepted the request. Check <see cref="SupportsCancellation"/> first to tell
    /// "not supported" apart from "nothing to cancel". A default no-op is provided so this member is non-breaking for
    /// existing implementers; the platform's facade overrides it.
    /// </summary>
    Task<bool> Cancel(string jobId, CancellationToken cancellationToken = default) => Task.FromResult(false);

    /// <summary>Whether the active engine supports on-demand cancellation (see <see cref="Cancel"/>).</summary>
    bool SupportsCancellation => false;
```

- [ ] **Step 2: Build Platform.Core**

Run: `cd /c/Projects/git/VirtoCommerce/vc-platform/src/VirtoCommerce.Platform.Core && dotnet build -c Debug --nologo -v q`
Expected: `Build succeeded. 0 Error(s)`

- [ ] **Step 3: Commit**

```bash
git add src/VirtoCommerce.Platform.Core/Jobs/IBackgroundJob.cs
git commit -m "feat(jobs): add engine-agnostic Cancel + SupportsCancellation to IBackgroundJob"
```

---

## Task 2: Platform.Core — static `BackgroundJob.Cancel` / `SupportsCancellation`

**Files:**
- Modify: `vc-platform/src/VirtoCommerce.Platform.Core/Jobs/BackgroundJob.cs`

- [ ] **Step 1: Add the static members** after the existing `Enqueue<THandler>` method:

```csharp
    /// <summary>Static counterpart of <see cref="IBackgroundJob.Cancel"/>. Resolves the scoped facade and delegates.</summary>
    public static async Task<bool> Cancel(string jobId, CancellationToken cancellationToken = default)
    {
        var provider = _rootServiceProvider
            ?? throw new InvalidOperationException(
                "BackgroundJob static facade is not initialized. Install the VirtoCommerce.BackgroundJobs module, " +
                "or inject IBackgroundJob instead.");

        using var scope = provider.CreateScope();
        var backgroundJob = scope.ServiceProvider.GetRequiredService<IBackgroundJob>();
        return await backgroundJob.Cancel(jobId, cancellationToken);
    }

    /// <summary>Whether the active engine supports cancellation; false when uninitialized.</summary>
    public static bool SupportsCancellation
    {
        get
        {
            var provider = _rootServiceProvider;
            if (provider is null)
            {
                return false;
            }

            using var scope = provider.CreateScope();
            return scope.ServiceProvider.GetRequiredService<IBackgroundJob>().SupportsCancellation;
        }
    }
```

- [ ] **Step 2: Build Platform.Core**

Run: `cd /c/Projects/git/VirtoCommerce/vc-platform/src/VirtoCommerce.Platform.Core && dotnet build -c Debug --nologo -v q`
Expected: `Build succeeded. 0 Error(s)`

- [ ] **Step 3: Commit**

```bash
git add src/VirtoCommerce.Platform.Core/Jobs/BackgroundJob.cs
git commit -m "feat(jobs): add static BackgroundJob.Cancel + SupportsCancellation"
```

- [ ] **Step 4: Pack + re-pin per Task 0 Step 2** so the module can compile against the new API.

---

## Task 3: Module Core — `IJobEngine.SupportsCancellation`

**Files:**
- Modify: `src/VirtoCommerce.BackgroundJobs.Core/IJobEngine.cs`

- [ ] **Step 1: Add the default member** after `Delete`:

```csharp
    /// <summary>Whether this engine can cancel a job on demand (surfaced to callers via the facade).</summary>
    bool SupportsCancellation => false;
```

- [ ] **Step 2: Build**

Run: `cd /c/Projects/git/VirtoCommerce/vc-module-background-jobs && dotnet build src/VirtoCommerce.BackgroundJobs.Core -c Debug --nologo -v q`
Expected: `Build succeeded.`

- [ ] **Step 3: Commit**

```bash
git add src/VirtoCommerce.BackgroundJobs.Core/IJobEngine.cs
git commit -m "feat: add IJobEngine.SupportsCancellation capability flag"
```

---

## Task 4: Module Core — `IJobCancellationStore` contract

**Files:**
- Create: `src/VirtoCommerce.BackgroundJobs.Core/Cancellation/IJobCancellationStore.cs`

- [ ] **Step 1: Create the interface**

```csharp
#nullable enable
using System.Threading;
using System.Threading.Tasks;

namespace VirtoCommerce.BackgroundJobs.Core.Cancellation;

/// <summary>
/// Shared, fleet-wide record of "cancellation requested" per job id, used by queue engines (RabbitMQ) that can't
/// recall a published message. A cancel sets a flag; the worker checks it before starting a job (discard) and while
/// running it (trip the handler's CancellationToken). The default implementation is Redis-backed (durable across the
/// fleet), with an in-memory fallback for a single process.
/// </summary>
public interface IJobCancellationStore
{
    /// <summary>Record that <paramref name="jobId"/> should be cancelled. Idempotent.</summary>
    Task RequestCancel(string jobId, CancellationToken cancellationToken = default);

    /// <summary>Returns true when cancellation has been requested for <paramref name="jobId"/>.</summary>
    Task<bool> IsCancelRequested(string jobId, CancellationToken cancellationToken = default);

    /// <summary>Clears the flag once the job has settled (succeeded/failed/cancelled).</summary>
    Task Clear(string jobId, CancellationToken cancellationToken = default);
}
```

- [ ] **Step 2: Build** — `dotnet build src/VirtoCommerce.BackgroundJobs.Core -c Debug --nologo -v q` → succeeds.

- [ ] **Step 3: Commit**

```bash
git add src/VirtoCommerce.BackgroundJobs.Core/Cancellation/IJobCancellationStore.cs
git commit -m "feat: add IJobCancellationStore contract"
```

---

## Task 5: Module Data — `InMemoryJobCancellationStore` (TDD)

**Files:**
- Create: `src/VirtoCommerce.BackgroundJobs.Data/Cancellation/InMemoryJobCancellationStore.cs`
- Test: `tests/VirtoCommerce.BackgroundJobs.Tests/JobCancellationStoreTests.cs`

- [ ] **Step 1: Write the failing test**

```csharp
#nullable enable
using System.Threading.Tasks;
using VirtoCommerce.BackgroundJobs.Data.Cancellation;
using Xunit;

namespace VirtoCommerce.BackgroundJobs.Tests;

public class JobCancellationStoreTests
{
    [Fact]
    public async Task InMemory_Request_IsRequested_Clear_RoundTrips()
    {
        var store = new InMemoryJobCancellationStore();
        var ct = TestContext.Current.CancellationToken;

        Assert.False(await store.IsCancelRequested("job-1", ct));

        await store.RequestCancel("job-1", ct);
        Assert.True(await store.IsCancelRequested("job-1", ct));
        Assert.False(await store.IsCancelRequested("job-2", ct));   // isolated per id

        await store.Clear("job-1", ct);
        Assert.False(await store.IsCancelRequested("job-1", ct));
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `dotnet test tests/VirtoCommerce.BackgroundJobs.Tests -c Debug --nologo -v q --filter "FullyQualifiedName~JobCancellationStoreTests"`
Expected: FAIL — `InMemoryJobCancellationStore` does not exist.

- [ ] **Step 3: Implement**

```csharp
#nullable enable
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using VirtoCommerce.BackgroundJobs.Core.Cancellation;

namespace VirtoCommerce.BackgroundJobs.Data.Cancellation;

/// <summary>
/// Single-process <see cref="IJobCancellationStore"/> for dev/testing and no-Redis deployments. Not fleet-safe: a
/// flag set on one instance is invisible to others.
/// </summary>
public sealed class InMemoryJobCancellationStore : IJobCancellationStore
{
    private readonly ConcurrentDictionary<string, byte> _cancelled = new();

    public Task RequestCancel(string jobId, CancellationToken cancellationToken = default)
    {
        _cancelled[jobId] = 1;
        return Task.CompletedTask;
    }

    public Task<bool> IsCancelRequested(string jobId, CancellationToken cancellationToken = default)
        => Task.FromResult(_cancelled.ContainsKey(jobId));

    public Task Clear(string jobId, CancellationToken cancellationToken = default)
    {
        _cancelled.TryRemove(jobId, out _);
        return Task.CompletedTask;
    }
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `dotnet test tests/VirtoCommerce.BackgroundJobs.Tests -c Debug --nologo -v q --filter "FullyQualifiedName~JobCancellationStoreTests"`
Expected: PASS (1 test).

- [ ] **Step 5: Commit**

```bash
git add src/VirtoCommerce.BackgroundJobs.Data/Cancellation/InMemoryJobCancellationStore.cs tests/VirtoCommerce.BackgroundJobs.Tests/JobCancellationStoreTests.cs
git commit -m "feat: in-memory job cancellation store"
```

---

## Task 6: Module Data — `RedisJobCancellationStore`

**Files:**
- Create: `src/VirtoCommerce.BackgroundJobs.Data/Cancellation/RedisJobCancellationStore.cs`

No unit test (needs a real Redis; covered by integration). Mirrors `RedisRecurringJobStateStore`.

- [ ] **Step 1: Implement**

```csharp
#nullable enable
using System;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis;
using VirtoCommerce.BackgroundJobs.Core.Cancellation;

namespace VirtoCommerce.BackgroundJobs.Data.Cancellation;

/// <summary>
/// Redis-backed <see cref="IJobCancellationStore"/> — fleet-wide. A cancel writes a short-lived key that any worker
/// (the one running the job, or the one that later dequeues it) reads. Keys carry a TTL so a cancel for a job that
/// already finished self-cleans; <see cref="Clear"/> removes it eagerly when the job settles.
/// </summary>
public sealed class RedisJobCancellationStore : IJobCancellationStore
{
    // Outlives a realistic max job duration + queue wait; keep the key self-cleaning if Clear is never reached.
    private static readonly TimeSpan _ttl = TimeSpan.FromHours(6);

    private readonly IConnectionMultiplexer _connection;

    public RedisJobCancellationStore(IConnectionMultiplexer connection)
    {
        _connection = connection;
    }

    public Task RequestCancel(string jobId, CancellationToken cancellationToken = default)
        => _connection.GetDatabase().StringSetAsync(Key(jobId), "1", _ttl);

    public async Task<bool> IsCancelRequested(string jobId, CancellationToken cancellationToken = default)
        => await _connection.GetDatabase().KeyExistsAsync(Key(jobId));

    public Task Clear(string jobId, CancellationToken cancellationToken = default)
        => _connection.GetDatabase().KeyDeleteAsync(Key(jobId));

    private static string Key(string jobId) => $"vc:jobs:cancel:{jobId}";
}
```

- [ ] **Step 2: Build** — `dotnet build src/VirtoCommerce.BackgroundJobs.Data -c Debug --nologo -v q` → succeeds.

- [ ] **Step 3: Commit**

```bash
git add src/VirtoCommerce.BackgroundJobs.Data/Cancellation/RedisJobCancellationStore.cs
git commit -m "feat: redis job cancellation store"
```

---

## Task 7: Module Data — DI selector `AddJobCancellationStore`

**Files:**
- Create: `src/VirtoCommerce.BackgroundJobs.Data/Cancellation/CancellationStoreServiceCollectionExtensions.cs`

- [ ] **Step 1: Implement** (same Redis-or-in-memory idiom as the recurring/map-reduce stores)

```csharp
#nullable enable
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using StackExchange.Redis;
using VirtoCommerce.BackgroundJobs.Core.Cancellation;

namespace VirtoCommerce.BackgroundJobs.Data.Cancellation;

public static class CancellationStoreServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="IJobCancellationStore"/> — Redis when an <see cref="IConnectionMultiplexer"/> is available
    /// (fleet-wide cancel), otherwise the single-process in-memory store. TryAdd so a custom engine may supply its own.
    /// </summary>
    public static IServiceCollection AddJobCancellationStore(this IServiceCollection services)
    {
        services.TryAddSingleton<IJobCancellationStore>(sp =>
        {
            var connection = sp.GetService<IConnectionMultiplexer>();
            return connection is not null
                ? new RedisJobCancellationStore(connection)
                : new InMemoryJobCancellationStore();
        });
        return services;
    }
}
```

- [ ] **Step 2: Build** — `dotnet build src/VirtoCommerce.BackgroundJobs.Data -c Debug --nologo -v q` → succeeds.

- [ ] **Step 3: Commit**

```bash
git add src/VirtoCommerce.BackgroundJobs.Data/Cancellation/CancellationStoreServiceCollectionExtensions.cs
git commit -m "feat: AddJobCancellationStore DI selector (redis or in-memory)"
```

---

## Task 8: Facade — implement `Cancel` + `SupportsCancellation` (TDD)

**Files:**
- Modify: `src/VirtoCommerce.BackgroundJobs.Data/Services/JobEngineBackgroundJob.cs`
- Test: `tests/VirtoCommerce.BackgroundJobs.Tests/FacadeCancellationTests.cs`

- [ ] **Step 1: Write the failing test**

```csharp
#nullable enable
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Moq;
using VirtoCommerce.BackgroundJobs;                       // IJobEngine
using VirtoCommerce.BackgroundJobs.Core;                  // BackgroundJobsOptions
using VirtoCommerce.BackgroundJobs.Core.Models;           // JobEnvelope
using VirtoCommerce.BackgroundJobs.Data.Services;         // JobEngineBackgroundJob
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.PushNotifications;
using VirtoCommerce.Platform.Core.Security;
using Xunit;

namespace VirtoCommerce.BackgroundJobs.Tests;

public class FacadeCancellationTests
{
    private static JobEngineBackgroundJob Create(IJobEngine? engine)
        => new(Mock.Of<IJobPayloadSerializer>(),
               Options.Create(new BackgroundJobsOptions()),
               Mock.Of<IPushNotificationManager>(),
               Mock.Of<IUserNameResolver>(),
               engine);

    [Fact]
    public async Task Cancel_DelegatesToEngineDelete()
    {
        var engine = new Mock<IJobEngine>();
        engine.Setup(x => x.Delete("job-1", It.IsAny<CancellationToken>())).ReturnsAsync(true);
        engine.SetupGet(x => x.SupportsCancellation).Returns(true);
        var facade = Create(engine.Object);

        Assert.True(facade.SupportsCancellation);
        Assert.True(await facade.Cancel("job-1", TestContext.Current.CancellationToken));
        engine.Verify(x => x.Delete("job-1", It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Cancel_NoEngine_ReturnsFalse_AndNotSupported()
    {
        var facade = Create(engine: null);
        Assert.False(facade.SupportsCancellation);
        Assert.False(await facade.Cancel("job-1", TestContext.Current.CancellationToken));
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `dotnet test tests/VirtoCommerce.BackgroundJobs.Tests -c Debug --nologo -v q --filter "FullyQualifiedName~FacadeCancellationTests"`
Expected: FAIL — `Cancel`/`SupportsCancellation` not overridden (facade uses the default no-op → `SupportsCancellation` false so the first test fails).

- [ ] **Step 3: Implement** — add to `JobEngineBackgroundJob` (it already holds the optional `engine` field):

```csharp
    public bool SupportsCancellation => engine?.SupportsCancellation ?? false;

    public Task<bool> Cancel(string jobId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(jobId);
        return engine is null ? Task.FromResult(false) : engine.Delete(jobId, cancellationToken);
    }
```

- [ ] **Step 4: Run to verify it passes**

Run: `dotnet test tests/VirtoCommerce.BackgroundJobs.Tests -c Debug --nologo -v q --filter "FullyQualifiedName~FacadeCancellationTests"`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add src/VirtoCommerce.BackgroundJobs.Data/Services/JobEngineBackgroundJob.cs tests/VirtoCommerce.BackgroundJobs.Tests/FacadeCancellationTests.cs
git commit -m "feat: facade Cancel delegates to engine + SupportsCancellation"
```

---

## Task 9: Hangfire + InMemory engines — declare `SupportsCancellation`

**Files:**
- Modify: `src/VirtoCommerce.BackgroundJobs.Hangfire/HangfireJobEngine.cs`
- Modify: `src/VirtoCommerce.BackgroundJobs.InMemory/InMemoryJobEngine.cs`

- [ ] **Step 1: Hangfire** — add the property (its `Delete` already calls `Hangfire.BackgroundJob.Delete`):

```csharp
    public bool SupportsCancellation => true;
```

- [ ] **Step 2: InMemory** — add the same property. Also honor cancellation: it runs jobs on the thread pool via the shared dispatcher, so it can respect a cancellation store if registered. Minimal version — declare support and cancel via a `CancellationTokenSource` map. Add to `InMemoryJobEngine`:

Change `_states` usage to also track a CTS per job and implement `Delete`:

```csharp
    private readonly ConcurrentDictionary<string, CancellationTokenSource> _running = new();

    public bool SupportsCancellation => true;
```

In `Enqueue`, create and register a CTS, pass its token to `RunAsync`; in `RunAsync`'s `finally` remove it. Update `Delete`:

```csharp
    public Task<bool> Delete(string jobId, CancellationToken cancellationToken = default)
    {
        var canceled = false;
        if (_running.TryGetValue(jobId, out var cts))
        {
            cts.Cancel();
            canceled = true;
        }
        _states.TryRemove(jobId, out _);
        return Task.FromResult(canceled || true);   // best-effort: removed state and/or signaled cancellation
    }
```

Wire the token into `RunAsync(jobId, envelope, token)` and pass it to `dispatcher.Dispatch(envelope with { Attempt = attempt }, context, token)`.

- [ ] **Step 3: Build the solution** — `dotnet build VirtoCommerce.BackgroundJobs.sln -c Debug --nologo -v q` → succeeds.

- [ ] **Step 4: Commit**

```bash
git add src/VirtoCommerce.BackgroundJobs.Hangfire/HangfireJobEngine.cs src/VirtoCommerce.BackgroundJobs.InMemory/InMemoryJobEngine.cs
git commit -m "feat: Hangfire + InMemory declare SupportsCancellation and honor cancel token"
```

---

## Task 10: RabbitMQ engine — cooperative cancel

**Files:**
- Modify: `src/VirtoCommerce.BackgroundJobs.RabbitMQ/RabbitMqJobEngine.cs`

- [ ] **Step 1: Inject the store and implement cancel.** Add a constructor parameter `IJobCancellationStore cancellationStore` (add `using VirtoCommerce.BackgroundJobs.Core.Cancellation;`), store it in a field, and replace the `Delete` method + add the capability:

```csharp
    public bool SupportsCancellation => true;

    /// <summary>
    /// RabbitMQ can't recall a published message, so cancel is cooperative: record the request in the shared store.
    /// The consumer discards the message if it hasn't started, or trips the running handler's CancellationToken.
    /// </summary>
    public async Task<bool> Delete(string jobId, CancellationToken cancellationToken = default)
    {
        await _cancellationStore.RequestCancel(jobId, cancellationToken);
        return true;
    }
```

Update the class doc comment: cancellation is now supported cooperatively; `GetStatus` still returns null (unchanged).

- [ ] **Step 2: Build** — `dotnet build src/VirtoCommerce.BackgroundJobs.RabbitMQ -c Debug --nologo -v q`. (Will fail to resolve `_cancellationStore` until the ctor field is added — add `private readonly IJobCancellationStore _cancellationStore;` and assign it in the constructor.) Re-run → succeeds.

- [ ] **Step 3: Commit**

```bash
git add src/VirtoCommerce.BackgroundJobs.RabbitMQ/RabbitMqJobEngine.cs
git commit -m "feat(rabbitmq): SupportsCancellation + Delete records a cooperative cancel request"
```

---

## Task 11: RabbitMQ consumer — discard-on-dequeue + running cancel

**Files:**
- Modify: `src/VirtoCommerce.BackgroundJobs.RabbitMQ/RabbitMqJobConsumer.cs`

- [ ] **Step 1: Inject the store.** Add `IJobCancellationStore cancellationStore` to the constructor (`using VirtoCommerce.BackgroundJobs.Core.Cancellation;`), assign to a field `_cancellationStore`.

- [ ] **Step 2: In `OnReceivedAsync`, after `jobId` is known and before dispatch, add the not-started discard:**

```csharp
        // Cancelled before it ran: discard on pickup (RabbitMQ can't remove a specific queued message, so the flag
        // is honored here, at dequeue).
        if (!string.IsNullOrEmpty(jobId) && await _cancellationStore.IsCancelRequested(jobId))
        {
            _logger.LogInformation("Job {JobId} was cancelled before start; discarding.", jobId);
            await TryAckAsync(channel, eventArgs.DeliveryTag);
            await _cancellationStore.Clear(jobId);
            return;
        }
```

- [ ] **Step 3: Replace the dispatch call** (currently `await _dispatcher.Dispatch(envelope, context, CancellationToken.None);`) with a per-job CTS + a watch loop that polls the store and trips the token:

```csharp
        using var cts = new CancellationTokenSource();
        using var watchStop = new CancellationTokenSource();
        var watch = WatchForCancellationAsync(jobId, cts, watchStop.Token);
        try
        {
            await _dispatcher.Dispatch(envelope, context, cts.Token);
            dispatched = true;
            await RunChannelOpAsync(async () => await channel.BasicAckAsync(eventArgs.DeliveryTag, multiple: false));
        }
        // ... existing catch blocks unchanged ...
        finally
        {
            await watchStop.CancelAsync();
            try { await watch; } catch (OperationCanceledException) { /* expected */ }
            if (!string.IsNullOrEmpty(jobId)) { await _cancellationStore.Clear(jobId); }
        }
```

- [ ] **Step 4: Add the watch helper** (polls the store; cancels the job CTS when a cancel is requested):

```csharp
    // Polls the shared cancellation store for a running job and trips its token when a cancel is requested. Stops when
    // the job finishes (watchStopToken) or a cancel is observed. Interval is a balance of latency vs Redis load.
    private static readonly TimeSpan _cancelPollInterval = TimeSpan.FromSeconds(3);

    private async Task WatchForCancellationAsync(string jobId, CancellationTokenSource jobCts, CancellationToken watchStopToken)
    {
        if (string.IsNullOrEmpty(jobId))
        {
            return;
        }

        try
        {
            while (!watchStopToken.IsCancellationRequested)
            {
                await Task.Delay(_cancelPollInterval, watchStopToken);
                if (await _cancellationStore.IsCancelRequested(jobId))
                {
                    _logger.LogInformation("Cancellation requested for running job {JobId}; signaling the handler.", jobId);
                    await jobCts.CancelAsync();
                    return;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // job finished — normal stop
        }
    }
```

- [ ] **Step 5: Build** — `dotnet build src/VirtoCommerce.BackgroundJobs.RabbitMQ -c Debug --nologo -v q` → succeeds.

- [ ] **Step 6: Commit**

```bash
git add src/VirtoCommerce.BackgroundJobs.RabbitMQ/RabbitMqJobConsumer.cs
git commit -m "feat(rabbitmq): discard cancelled-before-start jobs and cancel running handlers cooperatively"
```

---

## Task 12: Web wiring — register the store; inject into RabbitMQ

**Files:**
- Modify: `src/VirtoCommerce.BackgroundJobs.Web/PlatformStartup.cs`

- [ ] **Step 1: Register the store** in `ConfigureServices`, next to the other agnostic services (add `using VirtoCommerce.BackgroundJobs.Data.Cancellation;`):

```csharp
        // Shared cancellation flag store (Redis when configured, else in-memory) — used by queue engines to cancel.
        services.AddJobCancellationStore();
```

- [ ] **Step 2: Build the solution** — `dotnet build VirtoCommerce.BackgroundJobs.sln -c Debug --nologo -v q`. The RabbitMQ engine/consumer now resolve `IJobCancellationStore` from DI. Expected: succeeds.

- [ ] **Step 3: Commit**

```bash
git add src/VirtoCommerce.BackgroundJobs.Web/PlatformStartup.cs
git commit -m "feat: register IJobCancellationStore in module startup"
```

---

## Task 13: Optional REST — `POST api/platform/jobs/{id}/cancel`

**Files:**
- Modify: `src/VirtoCommerce.BackgroundJobs.Web/Controllers/Api/JobsController.cs`

- [ ] **Step 1: Add the action** (inject `IBackgroundJob backgroundJob = null` alongside the existing optional `IJobEngine`; add `using Microsoft.AspNetCore.Http;` and `using VirtoCommerce.Platform.Core.Jobs;`):

```csharp
    /// <summary>Request cancellation of a background job. 501 when the active engine can't cancel.</summary>
    [HttpPost]
    [Route("{id}/cancel")]
    public async Task<ActionResult> Cancel(string id, CancellationToken cancellationToken)
    {
        if (backgroundJob is null || !backgroundJob.SupportsCancellation)
        {
            return StatusCode(StatusCodes.Status501NotImplemented, "Cancellation is not supported by the active engine.");
        }

        var canceled = await backgroundJob.Cancel(id, cancellationToken);
        return canceled ? Ok() : NotFound();
    }
```

- [ ] **Step 2: Build** — `dotnet build src/VirtoCommerce.BackgroundJobs.Web -c Debug --nologo -v q` → succeeds.

- [ ] **Step 3: Commit**

```bash
git add src/VirtoCommerce.BackgroundJobs.Web/Controllers/Api/JobsController.cs
git commit -m "feat: POST api/platform/jobs/{id}/cancel (501 when unsupported)"
```

---

## Task 14: Conformance — cancellation capability case

**Files:**
- Modify: `tests/VirtoCommerce.BackgroundJobs.Conformance/JobEngineConformanceTests.cs`
- Modify: `tests/VirtoCommerce.BackgroundJobs.Conformance/*Capabilities*` (the `EngineCapabilities` record) — add `SupportsCancellation`.

- [ ] **Step 1: Add `SupportsCancellation` to the `EngineCapabilities` record** used by the conformance fixtures (default false), and set it true in the fixtures whose engine supports it (Hangfire, RabbitMQ, InMemory, Reference).

- [ ] **Step 2: Add a `[SkippableFact]`/`[Fact]` gated on the capability:**

```csharp
    [Fact]
    public async Task Cancel_Of_Running_Job_Is_Honored_When_Supported()
    {
        if (!Fixture.Capabilities.SupportsCancellation)
        {
            return; // capability not offered by this engine
        }

        // Enqueue a handler that loops until its CancellationToken trips, capturing whether it observed cancellation.
        // (Use the fixture's existing "controllable handler" pattern; assert the engine reports SupportsCancellation
        // and that Cancel(jobId) causes the handler's token to fire within a bounded wait.)
    }
```

Fill the body using the fixture's existing controllable-handler helper (see how retry/status tests obtain a handler + jobId). Assert: `engine.SupportsCancellation` is true; after `engine.Delete(jobId)`, the handler's token trips within, e.g., 10s (poll interval + margin).

- [ ] **Step 3: Run conformance** — `dotnet test tests/VirtoCommerce.BackgroundJobs.Tests -c Debug --nologo -v q --filter "FullyQualifiedName~Conformance"`
Expected: PASS (broker-gated fixtures skip as usual).

- [ ] **Step 4: Commit**

```bash
git add tests/VirtoCommerce.BackgroundJobs.Conformance/
git commit -m "test: conformance case for engine cancellation capability"
```

---

## Task 15: Full solution build + test

- [ ] **Step 1: Build** — `dotnet build VirtoCommerce.BackgroundJobs.sln -c Debug --nologo -v q` → `0 Error(s)`.
- [ ] **Step 2: Test** — `dotnet test tests/VirtoCommerce.BackgroundJobs.Tests -c Debug --nologo -v q` → all pass (infra-gated skipped).
- [ ] **Step 3:** No commit (verification only).

---

## Task 16: README — document cancellation

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add a "Cancellation" subsection** (near Usage) covering: `IBackgroundJob.Cancel(jobId)` + `SupportsCancellation`; the per-engine matrix (Hangfire = removes/aborts; RabbitMQ = cooperative via Redis flag, not-started discarded on dequeue, started trips the handler's `CancellationToken`, needs Redis in a fleet; In-Memory = cancels the running task); the note that handlers must honor their `CancellationToken`; and the optional `POST api/platform/jobs/{id}/cancel`. Also add a mapping row to the migration guide: `BackgroundJob.Delete(jobId)` → `IBackgroundJob.Cancel(jobId)`.

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: document engine-agnostic cancellation + capability matrix"
```

---

## Task 17: image-tools — use the new cancel (verification target)

**Files:**
- Modify: `vc-module-image-tools/src/VirtoCommerce.ImageToolsModule.Web/Controllers/Api/ThumbnailsTasksController.cs`
- Modify: the thumbnail handler — ensure it honors the `CancellationToken`.

- [ ] **Step 1: Bump image-tools' platform + module package pins** to the versions containing the cancellation API (Platform.Core with Task 1–2; `VirtoCommerce.BackgroundJobs.*` with the rest). Restore.

- [ ] **Step 2: Replace the controller cancel action:**

```csharp
public class ThumbnailsTasksController(IBackgroundJob backgroundJob) : Controller
{
    [HttpPost]
    [Route("{jobId}/cancel")]
    public async Task<ActionResult> Cancel([FromRoute] string jobId, CancellationToken cancellationToken)
    {
        if (!backgroundJob.SupportsCancellation)
        {
            return StatusCode(StatusCodes.Status501NotImplemented, "Cancellation not supported by the active engine.");
        }

        var canceled = await backgroundJob.Cancel(jobId, cancellationToken);
        return canceled ? Ok() : NotFound();
    }
}
```

- [ ] **Step 3: Ensure the thumbnail handler honors `ct`** in `Execute(payload, IJobExecutionContext ctx, CancellationToken ct)` — pass `ct` into the generation loop / `ThrowIfCancellationRequested()` at page boundaries (replacing the old `IJobCancellationToken` checks).

- [ ] **Step 4: Build image-tools** — `dotnet build -c Debug --nologo -v q` in the image-tools repo → succeeds.

- [ ] **Step 5: Integration check** — run the platform with (a) `Provider=Hangfire`: start a thumbnail task, POST `{jobId}/cancel`, verify it stops/removes; (b) `Provider=RabbitMQ` + Redis: same, verify the handler's `ct` trips within the poll interval and the task stops.

- [ ] **Step 6: Commit (image-tools repo)**

```bash
git add src/VirtoCommerce.ImageToolsModule.Web/Controllers/Api/ThumbnailsTasksController.cs
git commit -m "feat: cancel thumbnail job via engine-agnostic IBackgroundJob.Cancel"
```

---

## Self-review

- **Spec coverage:** §3.1 API → Tasks 1–2, 8; §3.2 engine capability → Tasks 3, 9, 10; §3.3 Hangfire → Task 9; §3.4 store + RabbitMQ not-started/started → Tasks 4–7, 10, 11; §3.5 REST → Task 13; §5 usage/image-tools → Task 17; §6 files → all; §7 testing → Tasks 5, 8, 14, 15; docs → Task 16. All covered.
- **Placeholders:** Task 14 leaves the conformance test body to be filled from the fixture's existing controllable-handler helper — this is intentional (the helper's exact shape lives in the conformance kit; the assertions are specified). Everything else has concrete code.
- **Type consistency:** `IJobCancellationStore.{RequestCancel,IsCancelRequested,Clear}` used identically across Tasks 4/5/6/7/10/11; `SupportsCancellation` bool on `IBackgroundJob`/`IJobEngine`/facade/engines; `Cancel(string, CancellationToken)` returns `Task<bool>` everywhere; facade `Cancel` → `engine.Delete` (engine keeps `Delete` naming, per spec §3.2).

## Cross-repo / packaging note
Platform.Core changes (Tasks 1–2) must be available to the module and image-tools. For local execution, pack Platform to the local feed and re-pin (Task 0). For the real release: land Tasks 1–2 in a platform release, then the module/image-tools consume it from nuget.org (revert the Task 0 `nuget.config`/pin changes).
