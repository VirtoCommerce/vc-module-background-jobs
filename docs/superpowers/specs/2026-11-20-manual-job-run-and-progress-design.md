# Design — Run a job from the Admin Shell and follow its progress on every engine

**Date:** 2026-11-20
**Ticket:** VCST-6934 (also takes the Hangfire part of VCST-6850 item 3, see Change log)
**Scope:** `vc-module-background-jobs` (this repo). Companion PRs: `vc-platform` (completion contract, §3.4) and
`vc-module-admin-shell` (broadcast filter, §3.10).
One Platform.Core change (2026-11-24, see Change log): `IJobProgress.Complete` with a default implementation and the
`JobOutcome` enum. The module's platform floor rises from **3.1097.0** to the platform version that carries them.
**Code read at:** `vc-module-background-jobs` `5ba4054` (dev), `vc-platform` `7b08741c6` (dev, 3.1102.0),
`vc-module-admin-shell` `b8fa323c5` (dev), `@vc-shell/framework` tags `v2.7.0` and `v2.12.0`.

> **Status (2026-11-24): accepted.** Two independent reviews returned "needs revisions"; their findings are applied
> here, together with a walk-through of the acceptance criteria, the test list and the risks. The external contract
> (§5) was measured on platform 3.1097.0.

## 1. Context and problem

A job registered with `AddBackgroundJob<THandler, TPayload>(triggerable: true)` is meant to be started by a person,
but nothing in the admin lets a person start it, and once it runs there is no reliable place to watch it.

- The only entry point is `POST api/background-jobs/enqueue`. The module ships no admin UI
  (`Web/Scripts/module.js` registers nothing; there is no `vue/` plugin). The Hangfire dashboard is a developer tool
  behind `background_jobs:manage`, exists only on the Hangfire engine, and cannot create a job.
- On platform 3.1097–3.1099, the module's floor before this delivery, the enqueue payload does not bind: `EnqueueJobRequest.Payload`
  is a `System.Text.Json.JsonElement` while the platform's MVC formatter is Newtonsoft, so an object payload arrives
  `Undefined` and the controller enqueues `{}`. The job runs with a default payload and ends Succeeded. Observed on
  platform 3.1097.0 with this module's `dev` (§5): a `DeleteDocumentsJobHandler` payload with both fields set was
  stored as `{"documentType":null,"documentIds":null}` and the job ended Succeeded. Platform 3.1100
  added a global `JsonElementJsonConverter` (`Startup.cs:204`, commit `48e8f0c82`, for OpenIddict) that makes it bind,
  but it still rounds large decimals.
- The progress notification loses data on every update: each `Report`/`Complete` builds a new
  `JobProgressPushNotification` and storage replaces it by id, so `Started` is dropped, `Created` and `IsNew` reset,
  and `Complete` overwrites the job's last message (e.g. "Imported 900 of 1000, 100 errors") with "Completed".
  Counts survive within one attempt (the reporter keeps the last values) but restart at 0 on every retry, because each
  attempt gets a new reporter (`JobExecutionContextFactory.Create`).
- Outcome and retries are wrong on every engine: each failed attempt closes the notification with the error and the
  retry reopens it; a user cancel leaves it open forever; a failure before the handler runs (payload that does not
  deserialize, a type or handler that does not resolve) leaves it "Queued" forever.
- Hangfire: the envelope's `Attempt` is always 1 (Hangfire replays the same envelope JSON), and a per-job
  `MaxRetryAttempts` can only lower the global retry count (`PerJobRetryFilterAttribute`), while RabbitMQ and InMemory
  let it raise the count.
- Since the move to the engine-agnostic API (VCST-5686) nothing writes to the Hangfire console.
- A caller of the REST endpoint can pass `Options.ProgressNotificationId` of another user's notification and
  overwrite it: storage replaces by id regardless of creator, and every notification id is broadcast to all clients.
- In the Admin Shell every admin sees every other user's job progress: the platform broadcasts each notification as
  `SendSystemEvents` (the scale-out synchronizer depends on it, `PushNotificationSynchronizerTask.cs:47`), and the
  Admin Shell host installs no broadcast filter. AngularJS listens to the per-user `Send` only.

## 2. Goals, non-goals

**Goals**
1. A person with the right permission starts any triggerable job from the Admin Shell, with an optional JSON payload,
   and lands on a page that follows that job to its end.
2. The progress notification tells the truth on every engine: stable start time, counts, the job's current message, the
   attempt number out of the total, and a final outcome (succeeded, failed with the error, canceled).
3. A failed attempt that will be retried does not report the job as failed.
4. The enqueue payload reaches the handler unchanged on every supported platform version, for the members the payload
   type declares.
5. A per-job retry limit means the same on every engine, raising included.
6. Hangfire dashboard shows job progress again.
7. A person sees only their own jobs' progress in the Admin Shell.

**Non-goals (this delivery)**
- AngularJS UI of any kind. AngularJS is legacy; new functionality goes to the Admin Shell only. AngularJS keeps the
  catch-all notification templates as they are.
- Engine-agnostic job-state store, a job list endpoint, progress fields on `GET api/platform/jobs/{id}`, and a truthful
  status on RabbitMQ (follows in the next delivery, on top of the completion seam of §3.4).
- Platform.Core changes other than the completion contract (§3.4): recurring-schedule progress/title, a
  handler-visible "skipped" outcome, per-job permissions, the attempt number on `IJobExecutionContext` (stays with
  VCST-6850, see Change log 2026-11-24), the `EnqueueOptions.UniqueKey` xmldoc.
- VCST-6850 items other than the Hangfire raise: backoff delays, retryable exception lists, docs page.
- Real `UniqueKey` de-duplication.
- Changing how the global retry default resolves (`VirtoCommerce:BackgroundJobs:MaxRetryAttempts`, else the legacy
  `VirtoCommerce:Hangfire:AutomaticRetryCount`).
- An upper bound on a per-job retry limit (§3.6).
- The search module's triggerable `DeleteDocumentsJobHandler` (§9, risk).

**Anti-goals**
- A notification that reads "failed" while a retry is pending, or "in progress" after the job has ended.
- A REST caller able to write into a notification it does not own.
- Any write to a job's notification by the enqueuing side after the job has become runnable (§3.4).

## 3. Design

### Glossary

| Term | Meaning | Code |
|---|---|---|
| job id | the id the engine's `Enqueue` returns; the same id reaches the worker on every attempt | `IJobExecutionContext.JobId` |
| attempt | one run of the handler; the first run is attempt 1 | `JobEnvelope.Attempt` |
| total attempts | how many attempts the engine will make at most: retry limit + 1 | new `JobEnvelope.TotalAttempts`, `JobProgressPushNotification.TotalAttempts` |
| retry | an attempt after a failure | — |
| retry limit | how many retries are allowed: the job's `MaxRetryAttempts`, else the engine default; a negative value counts as 0 | `JobEnvelope.MaxRetryAttempts` |
| final attempt | `Attempt >= TotalAttempts`, or `TotalAttempts` unknown | derived in the dispatcher |
| outcome | how the job ended: `Succeeded`, `Failed`, `Canceled` | new enum `JobOutcome` (Platform.Core, `VirtoCommerce.Platform.Core.Jobs`); the notification carries its name as a string (§3.4) |
| close the notification | set `Finished` and `Outcome`, so the UI stops showing it as running | `IJobProgress.Complete`, §3.4 |
| owned notification | the notification the facade created for this job (`CompletesProgressNotification` true) | `JobEnvelope.CompletesProgressNotification` |
| shared notification | a notification several jobs report into (map/reduce batch) | `CompletesProgressNotification` false |

### 3.1 Enqueue payload binding

- `EnqueueJobRequest.Payload` becomes a Newtonsoft `JToken` with a property-level `[JsonConverter]` owned by the module
  that reads with `DateParseHandling.None` and `FloatParseHandling.Decimal`. Analog:
  `vc-platform/src/VirtoCommerce.Platform.Web/Model/SettingValues.cs:13-47` (same problem, same fix). The token goes
  to the registered `IJobPayloadSerializer` as raw text (`ToString(Formatting.None)`), so the worker and the controller
  deserialize with the same settings. It does not depend on the platform's global converter.
- **Fidelity is defined by the payload type.** The controller deserializes the raw text into the registered payload
  type and the facade re-serializes that object. Members the payload type declares (polymorphic ones included, through
  the registered serializer's `PolymorphJsonConverter`) arrive unchanged, decimals at full precision and date-like
  strings as strings. JSON members the type does not declare are dropped, as today.
- **The job serializer reads with `DateParseHandling.None` and `FloatParseHandling.Decimal`** (`JobJsonSettings`, both
  the default instance and the one registered in DI). Without them a polymorphic member loses fidelity on every
  engine, in-process enqueues included: `PolymorphJsonConverter.ReadJson` loads the member with `JObject.Load(reader)`,
  which inherits the reader's defaults, so a nested decimal is cut to `double` precision and a nested date-like string
  is parsed and re-formatted in the host's culture (observed by test 1: `1234567890.123456789` → `1234567890.12346`,
  `"2026-11-20T10:00:00.000Z"` → `"11/20/2026 10:00:00"`). Members with a declared type are unchanged by the setting;
  members typed `object`, `JToken` and the like now receive a `string` for a date-like value and a `decimal` for a
  fractional number (§9). A fractional number outside the `decimal` range in such a member fails to read instead of
  becoming a `double`.
- `BackgroundJobsAdminController.Enqueue` checks `ModelState` **before** the name check. A body that is not valid JSON,
  or a payload the converter cannot read, makes model binding fail; the endpoint answers 400 naming the failing member
  (`payload`, or the body) instead of today's "A registered job 'name' is required.".
- A payload that is valid JSON but does not deserialize into the payload type keeps today's 400
  "Payload does not match job '…'".
- An absent or `null` payload still means "default instance of the payload type", as today.
- Swagger: the generated schema for a `JToken` member is re-checked by the smoke call (§5).

### 3.2 REST enqueue: ownership of the notification

- A request whose `options.progressNotificationId` is set is refused with a 400 that names the field. A REST caller
  cannot own a notification created elsewhere; in-process callers (`IBackgroundJob`, map/reduce) keep the option.
- Compatibility: a REST integrator that passes the field today gets a 400 after the upgrade. The README's REST section
  states it, with the reason.

### 3.3 Run page and progress page (Admin Shell plugin)

New `src/VirtoCommerce.BackgroundJobs.Web/vue/` Module Federation plugin for the Admin Shell. Skeleton mirrors any
module's `vue/` (`vite.config.mts` with `getDynamicModuleConfiguration`, `public/plugin.json`, csproj
`BuildAdminShellPlugin` target guarded by `SkipAdminShellPlugin`, generated API client); the job-following part mirrors
vc-module-search (`pages/index-progress.vue`, `composables/useJob.ts`, `components/JobProgress.vue`) and
vc-module-marketplace-vendor (`pages/sync-details.vue`, which filters the shell's notifications by id).

- **Menu item** "Background jobs", permission `background-jobs:access` (the module convention: menu on `:access`, data
  on `:read`; 7 of 8 module plugins and 6 of 7 modules' permission sets follow it).
- **Jobs page** lists `GET api/background-jobs/registered` (`background-jobs:read`): name, payload type, triggerable.
- **Run** (shown for triggerable jobs, enabled with `background-jobs:execute`) opens a small form with an optional raw
  JSON payload (empty means default payload) and enqueues with `options: { reportProgress: true, title: <job name> }`.
  The server default for a request without options is unchanged.
- **Progress page** opens with the job id from the enqueue response and follows the `JobProgressPushNotification`
  whose `JobId` equals it, from the shell's notification store (`JobProgressPushNotification` is not in
  `EXCLUDED_NOTIFICATION_TYPES`, which in `@vc-shell/framework` v2.12.0 holds only `IndexProgressPushNotification`),
  including a notification that arrived before the page opened. It shows title, bar from processed/total,
  "attempt N of M" ("attempt N" while M is unknown), the job's current message, elapsed time, outcome and error.
  - Until the first worker update arrives the page shows the job as queued, from the enqueue response alone: the
    initial "Queued" notification is sent before the engine assigns the job id, so it carries no `JobId` (§3.4).
  - A Cancel command is shown only with the platform permission `background_jobs:manage` (it guards
    `POST api/platform/jobs/{id}/cancel` today) and only while the job has not finished: cancelling a finished
    Hangfire job answers 200 and turns its state from `Succeeded` into `Deleted` (observed, §5), which rewrites the
    job's history.
- **Notification template** for `JobProgressPushNotification` registered through `defineAppModule({ notifications })`
  with toast mode `progress`; the toast completes with the outcome (`Failed` → error toast).

### 3.4 Completion seam, job id and the notification builder

- **Completion seam.** `IJobProgress` (Platform.Core, `VirtoCommerce.Platform.Core.Jobs`) gains
  `Task Complete(JobOutcome outcome, string error = null, CancellationToken cancellationToken = default)` with a
  default implementation that does nothing, and Platform.Core gains the enum `JobOutcome` (`Succeeded`, `Failed`,
  `Canceled`). This is the shape `IBackgroundJob.Cancel` and `SupportsCancellation` already have (VCST-5686): a
  reporter or engine that does not close a notification keeps today's behaviour, and a module compiled against an
  older Platform.Core keeps loading. `PushNotificationJobProgress` and every progress wrapper implement it;
  `DefaultJobDispatcher` calls `context.Progress.Complete` (today it tests `context.Progress is
  PushNotificationJobProgress`). The existing public `PushNotificationJobProgress.Complete(string? error, …)`
  overload stays for source compatibility. A wrapper that does not override `Complete` still compiles and drops the
  outcome silently: the console wrapper (§3.7) overrides it, and its task review checks the forwarding. Today's
  implementers outside this repo need nothing: the platform's `InlineJobExecutionContext` no-op reporter, and
  vc-module-mcp's `TrackingContext`, which wraps the context it hands to the inner handler only, while the dispatcher
  completes the outer one.
- **The dispatcher always delivers the outcome to the context's progress** through the seam, whether or not the
  envelope owns its notification. Ownership is decided inside the push reporter: it closes the notification only when
  `CompletesProgressNotification` is true. A wrapper (§3.7) therefore always learns the outcome, even around
  `NoOpJobProgress`.
- **Job id comes from the worker.** `JobExecutionContextFactory.Create` already receives the job id; it passes it to
  `PushNotificationJobProgress`, and every update the worker sends carries `JobId`. The facade sends no update after
  `IJobEngine.Enqueue` returns (the draft's re-send raced a fast worker: InMemory starts the job before `Enqueue`
  returns, and storage replaces a notification by id, so a late re-send could turn a finished notification back into
  "Queued"). This is how vc-module-import (`ImportJob.ImportBackgroundAsync` sets `JobId` from `PerformContext`),
  vc-module-marketplace-vendor (`SyncJob`) and others put a job id into their notification: only the worker writes
  after the job is runnable.
- **Engine contract: one job id.** The id an engine passes to the worker context SHALL equal the id its `Enqueue`
  returned, on every attempt. Hangfire (`PerformContext.BackgroundJob.Id` = `client.Create` result), RabbitMQ
  (`MessageId`, kept on retry re-publish), InMemory and Reference meet it today; the GoogleCloudTasks **sample** does
  not (`Enqueue` returns the full task resource name, the callback passes the short name from `X-CloudTasks-TaskName`)
  and is fixed (§3.8). The contract goes into the README custom-engine section and `CONFORMANCE.md`, with a
  conformance test.
- **Initial notification.** `EnqueueCore` stamps `JobHeaders.EnqueuedAt` first and uses that same instant for the
  notification's `Started` and `Created`, then sends it. `Started` therefore means "enqueued", queue wait included.
  The initial description is unique per enqueue (it carries the enqueue time), because platform storage folds a new
  notification with the same title, type and description into the previous one
  (`PushNotificationInMemoryStorage.cs:36-48`): two Runs of the same job would otherwise share one notification.
  `BackgroundJobs.Data` builds it through a public factory on `JobProgressPushNotification` for a queued job, so no
  assembly needs access to Core internals.
- **One builder produces every `JobProgressPushNotification`** (today built in `PushNotificationJobProgress.Report`/
  `Complete`, `MapReduceJob.cs:131-142`, `ReduceCoordinator.cs:120-127`). For an **owned** notification:
  - `Started` and `Created` come from the envelope's `JobHeaders.EnqueuedAt` and stay the same on every update;
    `IsNew` is not raised again by updates.
  - `ProcessedCount`/`TotalCount` keep their last known values when a `Report` omits them.
  - The description is the job's current message; `ProgressLog` keeps today's content (the current message, plus the
    error entry on failure). Messages are not accumulated: the current message and the counts carry the state, as in
    the import module's notification ("Imported 900 of 1000, 100 errors").
  - `Complete` keeps the last progress message as the description and records the outcome in `Outcome` and
    `Finished`; the error goes to `Error`.
  - New fields: `JobId`, `Outcome`, `Attempt`, `TotalAttempts`, `Error`.
  - `Outcome` on the notification is a `string` holding the `JobOutcome` name, not the enum: the live channel's enum
    shape depends on deployment — the default System.Text.Json hub protocol (an enum as a number) without
    `PushNotifications:ScalabilityMode`, Newtonsoft with it (vc-platform `SignalRBuilderExtensions.cs:25-33`) — while
    storage reads go through MVC's `StringEnumConverter`. An enum would reach the Admin Shell in two shapes, and a
    check for `"Failed"` would show a failure as a success. The enum is the C# contract; only the notification's
    field is serialized.
- **State across attempts.** Counts and the current message belong to the attempt: a retry runs the work again, so
  its first report starts from that attempt's own counts. `Started`, `Created`, `JobId` and the title stay the same
  across attempts; `Attempt` and `TotalAttempts` show which attempt the counts belong to.
- **Shared notifications** (map/reduce): a child envelope cannot carry stable `Started`/`JobId` for the batch.
  `Started` = the batch start time recorded on `MapReduceBatch`; `JobId` stays null; the description is the latest map
  message. A map task's update never closes the notification.

### 3.5 Attempts and outcome on every engine

- **The engine fills `Attempt` and `TotalAttempts` on the envelope before `IJobDispatcher.Dispatch`.** `Dispatch`'s
  signature does not change, so custom engines keep compiling.
  - RabbitMQ (`RabbitMqJobConsumer`), InMemory (`InMemoryJobEngine.RunAsync`) and the conformance kit's
    `ReferenceJobEngine`: `TotalAttempts = Math.Max(0, envelope.MaxRetryAttempts ?? global) + 1`, the formula the
    first two already use to decide a retry.
  - Hangfire (`HangfireJobExecutor`): `Attempt = RetryCount + 1` from the job parameter; `TotalAttempts` = the retry
    limit §3.6 applies to this job + 1. Filter and executor read the global limit from one source.
  - An engine that does not set `TotalAttempts` leaves it null: every attempt is treated as final (today's behaviour,
    so a notification never stays open forever).
- **The dispatcher closes or keeps the notification:**
  - handler returned → close `Succeeded`;
  - exception on the final attempt → close `Failed` with the error;
  - exception with a retry pending → keep open, message "attempt N of M failed, retrying" with the error;
  - `OperationCanceledException` while the dispatch token is cancelled → keep open (requested cancel, host shutdown
    or a lost distributed lock — the dispatcher cannot tell them apart; the engine decides, below);
  - `OperationCanceledException` while the dispatch token is **not** cancelled (a timeout inside the handler) → a
    failure like any other exception.
- **The engine closes the notification `Canceled` when the cancel was requested**, through the completion seam,
  because only the engine knows who tripped the token:
  - Hangfire: a cancel is a move to `Deleted` (`HangfireJobEngine.Delete`, or Delete in the Hangfire dashboard). The
    worker trips a running job's token when it reads a state other than `Processing` on its own server and worker
    from storage, and the same token is linked to host shutdown (`ServerJobCancellationToken.IsJobStateChanged`,
    v1.8.23). So on an `OperationCanceledException` with the dispatch token cancelled, `HangfireJobExecutor` reads the
    job's current state from Hangfire storage (`IStorageConnection.GetStateData`): `Deleted` → close `Canceled`;
    anything else keeps the notification open — `Processing` here is a shutdown, `Processing` on another server or
    worker is an ownership change, `Enqueued` is a requeue, and Hangfire runs the job again in each case. The storage
    is shared by every instance, so a cancel accepted by one instance and a job run by another are classified alike,
    with or without Redis. `JobAbortedException` is not usable for this: Hangfire raises it outside the executor, and
    also on an ownership change or requeue (`CoreBackgroundJobPerformer.HandleJobPerformanceException`, v1.8.23).
  - RabbitMQ: the job's token was tripped by the cancellation store (`RabbitMqJobEngine.Delete` is its only writer).
  - InMemory: the job's own `CancellationTokenSource` was canceled by `Delete`.
  - An engine that does nothing keeps today's behaviour (the notification stays open).
- **A job canceled before it runs, or while it waits for a retry**, is closed `Canceled` too:
  - Hangfire: an apply-state filter on `Deleted`, for a job that was `Enqueued`, `Scheduled` (a retry wait included)
    or `Awaiting`, reads the envelope from the job's first argument and closes its notification; no executor will
    run for such a job. A job that was `Processing` is left to the executor, a finished one is left alone. The filter
    builds `PushNotificationJobProgress` from the envelope and the Hangfire job id, so `JobId` stays what earlier
    attempts wrote (storage replaces a notification by id), and waits for `Complete` synchronously, as
    `IPushNotificationManager.Send` itself does, because Hangfire state filters are synchronous. It fills `Attempt`
    and `TotalAttempts` the way `HangfireJobExecutor` does, through one shared helper: `RetryCount` from the job's
    parameters (read through the state context's storage connection) + 1, and the retry limit §3.6 applies + 1. A
    retry-waiting job's last message and counts are not in the envelope and are not kept (AC 12, the one exception);
  - RabbitMQ: the pre-dispatch discard of a cancelled message (`RabbitMqJobConsumer.OnReceivedAsync`) closes it;
  - InMemory: cancellation observed outside the attempt loop (before the first attempt or between attempts) closes it.
  - **On every engine such a close shows the attempt that was canceled before it ran and the job's total**, never
    the envelope's defaults (`Attempt` 1, `TotalAttempts` null), which would overwrite what earlier attempts wrote:
    Hangfire as above; RabbitMQ the received message's `Attempt` (a retry is re-published with `Attempt + 1`) and the
    total, computed **before** the discard check; InMemory and Reference the attempt the loop was about to start
    and their total. The GoogleCloudTasks sample has no such close (its `Delete` removes the task, and the callback
    runs through `IJobEnvelopeRunner` with the envelope as published), so it keeps the documented fallback of an
    engine that does not set `TotalAttempts`.
- **Cancel of a finished job changes nothing.** Today `POST api/platform/jobs/{id}/cancel` on a Succeeded Hangfire job
  answers 200 and moves it to `Deleted`, rewriting its history (observed, §5). `HangfireJobEngine.Delete` moves a job
  to `Deleted` only from `Enqueued`, `Scheduled`, `Awaiting` or `Processing`, and returns `false` for a finished job,
  so `JobsController.Cancel` answers as it does for any job the engine rejects. InMemory (which now keeps the state of
  a finished job) returns `false` the same way.
- Failures before the handler runs (payload deserialization, type or handler resolution) follow the same rules: the
  `try` in `Dispatch` starts before them, and they reach telemetry too.
- InMemory `GetStatus`: `Failed` only after the final attempt; `Cancelled` is terminal and reported completed (InMemory
  `Delete` today removes the state; the fix keeps it). InMemory is a development engine; this is checked by E2E only.

### 3.6 Per-job retry limit on Hangfire

`PerJobRetryFilterAttribute` stops being "tighten only". For an envelope job whose `MaxRetryAttempts` is set (clamped
with `Math.Max(0, …)`), the filter decides from the candidate state the global `AutomaticRetryAttribute` (order 20)
elected and the `RetryCount` that election wrote:

| Global election (candidate state) | `RetryCount` vs job limit L | Filter does |
|---|---|---|
| a retry (`ScheduledState`, or `EnqueuedState` for a zero-delay retry) | `RetryCount <= L` | keep it |
| a retry | `RetryCount > L` | restore `Failed` (the lowering case, today's behaviour, now also for zero-delay retries) |
| `FailedState` (global attempts exhausted) | `RetryCount < L` | apply a copy of the global attribute with `Attempts = L` to the election, from `Failed` only, so Hangfire's own delay formula and `RetryCount` bookkeeping schedule the retry |
| `FailedState` | `RetryCount >= L` | keep `Failed` |

- Jobs without a per-job value and non-envelope jobs keep the global filter's decision.
- The copy is built from the registered global attribute (same delays, same `OnAttemptsExceeded`), not a fresh
  `AutomaticRetryAttribute` with default backoff, and `RetryCount` is counted once per election.
- There is no upper bound on L: a caller who sets a large limit gets that many retries. Stated in the README.
- The `RetryCount` parameter name is already asserted against the shipped Hangfire assembly
  (`PerJobRetryFilterTests`); the new path keeps that guard.

### 3.7 Hangfire console mirror

- `HangfireJobExecutor` wraps the context's progress (the push-notification one and `NoOpJobProgress` alike) in a
  Hangfire-project wrapper that also writes to the job's console: a line when the message changes, a progress bar
  updated when the integer percentage changes (`TotalCount > 0`, clamped to 0..100). The wrapper overrides
  `IJobProgress.Complete`, forwards the outcome to the inner progress (§3.4) and writes a final line with the outcome.
- Console errors are caught and logged; they never fail the job. Without `UseConsole()` (Memory storage) the
  Hangfire.Console calls are silent no-ops (`ConsoleContext.FromPerformContext` returns null, v1.4.3).

### 3.8 Conformance kit and samples

The kit already exists: `tests/VirtoCommerce.BackgroundJobs.Conformance`, published as the NuGet package
`VirtoCommerce.BackgroundJobs.Conformance` (3.1055.0 on nuget.org), with fixtures in this repo for Reference, InMemory,
Hangfire and RabbitMQ. This delivery only adds scenarios and capability flags to it.

- Conformance additions (§8) that only some engines can meet are gated by capability flags on the fixture, like the
  existing `SupportsUniqueKeyDedup`. An engine that declares a capability false gets the fallback asserted instead
  (for cancel: the notification stays open), so a missing capability is visible, not skipped.
- New flags default to `false`, so an external engine's fixture that is not updated keeps passing on the fallback
  after a package upgrade. The one-job-id scenario has no flag: it runs on every engine, because an engine that breaks
  it breaks the progress page.
- `ReferenceJobEngine` implements attempts, total attempts and cancel classification, so the kit has a reference for
  every new assertion.
- `CONFORMANCE.md` and the README custom-engine section list the new engine duties: fill `Attempt` and
  `TotalAttempts`, pass the job id `Enqueue` returned, close `Canceled` on a requested cancel.
- **GoogleCloudTasks sample**: the id the callback passes equals the
  id `Enqueue` returned, so the sample meets the one-job-id contract. Which side changes (the callback rebuilding the
  full task name from its options, or `Enqueue` returning the short name) is the plan's choice; the sample's `Delete`
  and status lookups must keep working with the id it returns.

### 3.9 Documentation in this repo

- README requirements: the minimum platform version rises to the one carrying `IJobProgress.Complete` (§3.4); the
  custom-engine section names `Complete` as the way an engine closes a notification.
- README "Retries": `EnqueueOptions.MaxRetryAttempts` is honored and may raise or lower the count on every engine
  (today's text says "not yet honored"); no upper bound.
- README REST section: `options.progressNotificationId` is refused (§3.2).
- README and `JobEnvelope.UniqueKey` doc: no shipped engine (Hangfire, RabbitMQ, InMemory) de-duplicates by
  `UniqueKey`; correct `docs/benchmark-plan.md:100,109`.
- README "Payload serialization": the job serializer keeps date-like strings and decimals as written; what changes
  for members typed `object` / `JToken` (string instead of `DateTime`, decimal instead of `double`, a fractional number
  outside the decimal range fails); the bulk-actions date-property shift on non-UTC hosts (§9).

### 3.10 Companion PR: Admin Shell broadcast filter (`vc-module-admin-shell`)

- The host `App.vue` installs `setBroadcastFilter(x => x.creator === <current user name>)`, matching the AngularJS
  per-user behaviour and the vendor-portal and import apps. The platform keeps broadcasting (the scale-out synchronizer
  and vendor-portal need it).

## 4. Approaches considered

| Decision | Chosen | Rejected and why |
|---|---|---|
| Admin UI | Admin Shell Vue plugin only | AngularJS blade: legacy, no new functionality goes there |
| After Run | progress page linked by `JobId` on the notification, `JobId` written by the worker | facade re-sends with `JobId` after `Enqueue`: races a fast worker and can turn a finished notification back into "Queued"; return the notification id from `enqueue` (body or header): not needed — the notification type reaches the shell store, so matching by `JobId` works, and the body change would break integrators; re-send only while storage still holds "Queued": a non-atomic read-then-write, narrows the race without closing it, no precedent in any module |
| Payload binding | `JToken` + module-owned converter | raise the floor to 3.1100: depends on an unrelated converter and still loses decimals; read the raw body: lose model binding and the Swagger schema |
| Completion | `IJobProgress.Complete` with a default implementation (Platform.Core), outcome always delivered | a module interface the dispatcher discovers by a type test (`IJobProgressComplete`, chosen first, replaced 2026-11-24): a second seam beside the platform's own, found by a cast, and the Hangfire filter needed Core internals to close a job that had not run; dispatcher unwraps a wrapper: ties it to wrapper types; deliver only to owned notifications: the console wrapper never learns the outcome |
| Final attempt | engine supplies attempt and total, dispatcher derives "final" | engine supplies a final flag: cannot show "attempt N of M"; Hangfire state filter on `Failed`: Hangfire-only and loses counts and the last message |
| Hangfire raise | filter applies a copy of the global `AutomaticRetryAttribute` with the job's limit, by decision table | re-implement scheduling and delays: duplicates Hangfire; a fresh attribute: wrong delays, and it only handles `FailedState` |
| Cancel of a finished job | engine `Delete` refuses it (`false`) | hide Cancel in the UI only: the API still rewrites history for any other caller |
| Hangfire cancel of a running job | the executor reads the job's state from Hangfire storage on `OperationCanceledException` | a `Deleted` filter records the intent in `IJobCancellationStore`: without Redis that store lives in one process, so a cancel accepted by a web instance is invisible to the worker running the job and the notification stays open; the filter is synchronous while the store is asynchronous, and runs before the state is committed; `HangfireJobEngine.Delete` records it: misses a delete from the Hangfire dashboard |
| User cancel vs shutdown | each engine classifies its own cancel and closes `Canceled` | any `OperationCanceledException` in the dispatcher: a lost lock or host shutdown would close a job the engine re-runs; `IJobCancellationStore.IsCancelRequested` in the dispatcher: only RabbitMQ writes that store today; Hangfire `JobAbortedException`: raised outside the executor and also on requeue |
| Broadcast leak | host filter in `vc-module-admin-shell` | filter in this plugin: one filter per app, the plugin would overwrite the host's; stop broadcasting in the platform: breaks scale-out sync |

## 5. External contract

Observed 2026-11-20 on the local stand: platform **3.1097.0** release, `vc-module-background-jobs` `dev` `5ba4054`
(Hangfire engine, SQL storage), `VirtoCommerce.Search` 3.1015.0, `VirtoCommerce.ElasticSearch9` 3.1008.0, admin bearer
token. Rows still marked `read-from-code` are re-observed by the smoke call after the first callable piece.

| Call | Request | Response / message | Status |
|---|---|---|---|
| `GET api/background-jobs/registered` | — (`background-jobs:read`) | 200, array of `{ name, handlerType, payloadType, triggerable }`; 11 entries on this stand, 6 triggerable (platform `AutoAccountLockoutJob`, `PruneExpiredTokensJob`, `SignInLogCleanupJob`; search `DeleteDocumentsJobHandler`, `IndexChangesJobHandler`, `IndexDocumentsJobHandler`) | observed |
| `POST api/background-jobs/enqueue`, object payload | `{ name: "DeleteDocumentsJobHandler", payload: { documentType, documentIds }, options: { reportProgress: true, title } }` (`background-jobs:execute`) | 200, job id as a JSON string (`"696275"`); stored envelope `PayloadJson` = `{"documentType":null,"documentIds":null}`, `ProgressNotificationId` set, `Title` kept, `CompletesProgressNotification: true`, `Attempt: 1`, `MaxRetryAttempts: null`; job ended Succeeded | observed (defect reproduced on 3.1097) |
| same, `payload: 5` | — | 400, body `"A registered job 'name' is required."` | observed |
| same, other outcomes | — | 404 unknown job name; 400 non-triggerable job; 400 payload that does not match the type; 500 (problem) handler type not loadable; 503 no engine installed; 400 handler/payload mismatch (`ArgumentException`) | 404 and 400 non-triggerable **observed** by the smoke call (below); the rest read-from-code (`BackgroundJobsAdminController.Enqueue`) |
| **Smoke call after Task 1** — 2026-11-24, task stand (aspire-host worktree): platform 3.1097.0 release, `qa_*` copy, Hangfire on SQL, local ES 9, module at `feat/VCST-6934` `66b9166` | object payload `{documentType, documentIds}`, `reportProgress: true` | 200 `"696288"`; stored envelope `PayloadJson` = `{"documentType":"Vcst6934Probe","documentIds":["vcst6934-doc-1","vcst6934-doc-2"]}` (both members, unlike the 2026-11-20 row above); the handler ran with them (it then failed on the stand's local-ES TLS trust — environment, not the module); notification carried `jobId: "696288"`, `outcome: "Failed"` (string), `error`, `started == created` | observed |
| same | body cut off inside `payload` | 400 `{"request":["The request field is required."],"payload.documentType":["Unexpected end of content while loading JObject. …"]}`; no job (the next enqueue got the next id, 696289) | observed (AC 2) |
| same | `options.progressNotificationId` set | 400 `"'options.progressNotificationId' is not accepted over REST: …"`; no job | observed (AC 3) |
| same | no `payload`, `PruneExpiredTokensJob` | 200 `"696289"`; stored `PayloadJson` = `"{}"`; job `Succeeded` | observed (default payload) |
| Swagger schema of `payload` after the change to `JToken` (`/docs/VirtoCommerce.BackgroundJobs/swagger.json`) | — | `EnqueueJobRequest.payload` = `{ "description": "The job payload as JSON; …" }` — no `type`, i.e. any JSON value; a generated client types it as `any` | observed |
| Stored notification, `POST api/platform/pushnotifications` `{ take, skip }` | — | `{ totalCount, newCount, notifyEvents[] }`; the probe's item: `notifyType: "JobProgressPushNotification"`, `id`, `creator`, `title`, `description: "Completed"`, `started: null`, `finished`, `created` (= completion time), `isNew: true`, `processedCount: 0`, `totalCount: 0`, `progressLog: []`, `repeatCount: 0`, `serverId: null` | observed (MVC serialization of the stored copy) |
| Platform storage, new notification equal to the previous one in title, type and description | — | folded into the previous notification (`PushNotificationInMemoryStorage.cs:36-48`) | read-from-code |
| SignalR `Send` (creator) / `SendSystemEvents` (all) | — | same members; the hub protocol's serializer settings differ from MVC (camelCase, `$type` only in scale-out mode) | read-from-code |
| Enum members of a notification, by channel | — | MVC (REST history): string (`StringEnumConverter`, platform `Startup.cs:202`); SignalR default protocol (System.Text.Json, `AddSignalR()` at `Startup.cs:137`, aspnetcore `JsonHubProtocol` defaults): number; SignalR scale-out (Newtonsoft, `SignalRBuilderExtensions.cs:30-34`): number. `ProgressMessage.Level`'s Newtonsoft-only attribute does not reach System.Text.Json. Hence `Outcome` is a string (§3.4) | read-from-code; E2E reads a SignalR frame |
| `GET api/platform/jobs/{id}` | (`background_jobs:manage`) | 200 `{ state: "Succeeded", completed: true, id }`; unknown id → 200 `{ state: null, completed: true, id }` (never 404) | observed |
| `POST api/platform/jobs/{id}/cancel` | (`background_jobs:manage`) | on a **finished** Hangfire job: 200, and the job's state becomes `Deleted` (`GET` then answers `state: "Deleted"`); 404 when the engine rejects, 501 when the engine cannot cancel | observed (finished job); read-from-code (404/501, `JobsController.cs:57-68`) |
| Admin Shell notification store | — | `EXCLUDED_NOTIFICATION_TYPES = ["IndexProgressPushNotification"]` in `@vc-shell/framework` v2.12.0 (`framework/core/notifications/types.ts`), so `JobProgressPushNotification` is stored | read-from-code (framework source at the host's tag) |
| Admin Shell plugin discovery | `{module}/plugins/admin-shell/remoteEntry.js` + `plugin.json`; served through the host app's manifest (`GET /api/apps/<admin-shell app id>/manifest`), not `/api/apps/platform/manifest` | menu contribution with `permission` (forwarded opaque by the platform) | read-from-code; Admin Shell not on this stand |
| Admin Shell host framework | — | `@vc-shell/framework` 2.12.0, shared as MF singleton | read-from-code (host `package.json` on GitHub) |

## 6. Acceptance criteria

Each criterion names its check: `test` only where the failure is silent and harmful (§7), otherwise `E2E`.

1. WHEN a job is enqueued through `POST api/background-jobs/enqueue` with an object payload on platform 3.1097–3.1099
   or 3.1100+, THEN the handler SHALL receive every member the payload type declares unchanged, polymorphic members,
   full-precision decimals and date-like strings included. — test
2. WHEN the enqueue request body is not valid JSON or its `payload` cannot be read, THEN the endpoint SHALL NOT
   enqueue a job — in particular not one with the default payload — and SHALL answer 400 naming the failing member.
   — E2E (the "no enqueue" half is supplied by the framework, see §8 item 2)
3. WHEN an enqueue request carries `options.progressNotificationId`, THEN the endpoint SHALL answer 400 naming that
   field and SHALL NOT enqueue a job. — test
4. WHERE the user has `background-jobs:access`, THE Admin Shell SHALL show a "Background jobs" menu item. — E2E
5. WHEN the user opens the Background jobs page, THEN it SHALL list every registered job with its triggerable flag.
   — E2E
6. WHERE a job is triggerable and the user has `background-jobs:execute`, THE page SHALL offer Run with an optional
   JSON payload; Run SHALL enqueue with progress reporting and the job name as title, and open the job's progress
   page. — E2E
7. WHILE a job runs, THE progress page SHALL show its title, processed and total counts, "attempt N of M" (or
   "attempt N" when the total is unknown), the job's current message and elapsed time, updated as the job reports, including a
   job that finished before the page opened. — E2E
8. WHEN a job ends, THEN its owned notification SHALL be closed with outcome `Succeeded`, `Failed` with the error, or
   `Canceled`, and the progress page and toast SHALL show that outcome. — test (dispatcher, all engines); E2E (UI)
9. WHEN an attempt fails and `Attempt < TotalAttempts`, THEN the notification SHALL stay open with "attempt N of M
   failed, retrying" and SHALL NOT be closed as failed, on every engine. — test
10. WHEN the user cancels a job that is running, queued or waiting for a retry, THEN its notification SHALL be closed
    as `Canceled` on every engine that declares cancel support. WHEN an `OperationCanceledException` arises while the
    dispatch token is cancelled and no cancel was requested, THEN the notification SHALL stay open. WHEN it arises
    while the dispatch token is not cancelled, THEN it SHALL be handled as a failure. — test; E2E for a
    retry-waiting job on engines other than Hangfire (§8 item 5)
11. WHEN the dispatcher fails before the handler runs (payload, type or handler resolution), THEN the failure SHALL be
    handled like a handler failure: closed `Failed` on the final attempt, open while a retry is pending. — test
12. FOR an owned notification, every update SHALL keep `Started`, `Created` and `JobId` unchanged, keep the last known
    counts within an attempt when a report omits them, and keep the job's last message as the description on
    completion. The one exception: a Hangfire job canceled while it waits for a retry is closed with `JobId`,
    `Started` and its attempt numbers kept but without its last message and counts (§3.5). — test
13. WHEN a job reports into a shared notification, THEN its updates SHALL NOT close that notification — test; and
    SHALL carry the batch start time — E2E.
14. WHEN a job enqueued with `MaxRetryAttempts = N` (N >= 0; a negative value counts as 0) fails every time, THEN every
    engine SHALL run it N + 1 times, including when N exceeds the global default on Hangfire, and including zero-delay
    retries. — test
15. WHEN a Hangfire job runs attempt K, THEN the envelope the dispatcher receives SHALL carry `Attempt = K` and the
    total the engine will make. — test (through test 4 on the Hangfire fixture: wrong numbers break the final-attempt
    decision it asserts)
16. WHILE a Hangfire job on a storage with `UseConsole()` reports progress, THE dashboard console SHALL show a line per
    changed message and a progress bar per changed integer percentage; WHEN writing to the console fails, THEN the job
    SHALL continue. — E2E
17. WHEN a job's progress is wrapped for the console, THEN the owned notification SHALL still be closed — test
    (through test 4 on the Hangfire fixture, where the executor always wraps); and the console SHALL show the outcome
    line — E2E.
18. WHILE an InMemory job waits for a retry, THE engine SHALL NOT report it completed; WHEN it is canceled, THEN the
    engine SHALL report it completed. — E2E (development engine)
19. WHERE the Admin Shell host has the broadcast filter (companion PR), THE user SHALL NOT see notifications created
    by another user. — test (in `vc-module-admin-shell`); E2E
20. THE README SHALL state that the per-job retry limit is honored on every engine with no upper bound, that no shipped
    engine de-duplicates by `UniqueKey`, that REST refuses `options.progressNotificationId`, and the engine duties of
    §3.8. — E2E (review)
21. EVERY update a worker sends to a job's notification SHALL carry the job id the engine's `Enqueue` returned for that
    job, on every attempt, and the enqueuing side SHALL NOT write the notification after `Enqueue` returns. — test
22. WHEN the same job is enqueued twice in a row with progress reporting, THEN each run SHALL get its own notification.
    — test
23. WHERE the user has `background_jobs:manage`, THE progress page SHALL offer Cancel only while the job has not
    finished. — E2E
24. WHEN a cancel is requested for a job that has already finished, THEN an engine that reports status SHALL refuse it
    and SHALL leave the job's state unchanged. — test

## 7. Testing rule

Test only a branch whose failure is silent and harmful: data (a duplicate, a lost or wrong value, an item left in a
wrong state), security or privacy (another role, another person's data, a token leak), or a lost message the person
needs. Loud or harmless failures get no test; E2E covers them. A surviving mutant alone is not a reason.

## 8. Test list

The silent-and-harmful branches this design creates or fixes. Cross-engine items go into the conformance kit
(`tests/VirtoCommerce.BackgroundJobs.Conformance`), so Hangfire, InMemory, Reference (and RabbitMQ when enabled) run
the same assertion; items an engine may not support are capability-gated with the fallback asserted (§3.8).

1. Payload through the MVC pipeline and on to dispatch (TestHost, Newtonsoft configured as the platform does), with and
   without a stand-in for the 3.1100 global `JsonElementJsonConverter` (absent from Platform.Core 3.1097): a payload
   with a polymorphic member, a high-precision decimal and a date-like string reaches the handler unchanged. Data,
   wrong value. (AC 1)
2. Dropped (2026-11-24): it cannot fail. On any deserialization error the Newtonsoft input formatter returns no
   model (`NewtonsoftJsonInputFormatter`, aspnetcore v10.0.0, lines 176-205) or rethrows (500), the controller has
   no `[ApiController]`, and `Enqueue` already answers 400 to a null request before anything is enqueued — with or
   without the new `ModelState` check. AC 2 is checked by E2E. The number is kept so later items keep theirs.
3. `options.progressNotificationId` on REST → 400, no enqueue. Security, another person's notification. (AC 3)
4. Conformance: failing attempt with a retry pending leaves the notification open with attempt N of M; the final
   failure (`Attempt >= TotalAttempts`) closes it `Failed` with the error; success closes `Succeeded`. Lost message.
   (AC 8, 9; on the Hangfire fixture also AC 15 and the closing half of AC 17)
5. Conformance: user cancel of a running and a queued job closes `Canceled`; on the Hangfire fixture also a running
   job moved to `Deleted` without the engine (as the dashboard does) closes `Canceled`, and a running job requeued
   without a cancel stays open; an `OperationCanceledException` with the dispatch token cancelled and no request
   leaves it open; one with the token not cancelled closes `Failed` on the final attempt. A retry-waiting job: a unit
   test of the Hangfire `Deleted` filter with old state `Scheduled` closes its notification `Canceled`; no fixture can
   hold a job in a retry wait (the Hangfire fixture retries instantly, InMemory has no pause), so other engines are
   checked by E2E. Wrong state. (AC 10)
6. Dispatcher: payload that does not deserialize, on final and non-final attempts. Item left in a wrong state. (AC 11)
7. Notification builder: stable `Started`/`Created`/`JobId` across updates and attempts, counts kept within an
   attempt, last message kept on completion. Lost or wrong value. (AC 12)
8. Shared notification: a map task's update does not close it. Wrong state. (AC 13)
9. Conformance: `MaxRetryAttempts = N` above the global default gives N + 1 runs on every engine; on Hangfire also with
   zero-delay retries, and lowering below the global default. Data, a job given up early or retried too often. (AC 14)
10. Conformance: the job id in the worker's context equals the id `Enqueue` returned, on the first attempt and on a
    retry; the facade sends nothing after `Enqueue` returns. Lost message (the page never finds its job). (AC 21)
11. Two enqueues of the same job in a row produce two notifications. Lost message. (AC 22)
12. `vc-module-admin-shell`: the host filter drops a broadcast from another creator and keeps one from the current
    user. Privacy. (AC 19)
13. Conformance (`SupportsStatusQuery` and `SupportsDelete`): `Delete` of a succeeded job returns `false` and its
    status stays `Succeeded`. Data, a finished job's history rewritten. (AC 24)

## 9. Risks and open points

- **A live surface appears.** Once payload binding works on every platform, `api/background-jobs/enqueue` with
  `background-jobs:execute` reaches every triggerable handler with a caller-built payload. Today the bind defect is
  the only thing keeping vc-module-search's `DeleteDocumentsJobHandler` inert through this route. Out of scope here;
  to be raised with the search module.
- **Every non-opted-out handler is triggerable**, recurring ones included (`AddRecurringJob` registers with
  `triggerable: true`): the platform's token and sign-in-log cleanups and auto lockout. Run will list and start them.
  Recorded, not changed.
- **Hangfire cancel classification** rests on Hangfire tripping a running job's token only after it has read a
  committed state other than `Processing` (`ServerJobCancellationToken.IsJobStateChanged`, v1.8.23, read from source,
  not run), so the executor's own read sees `Deleted`. Proven by test 5 on the Hangfire fixture. A shutdown and a
  cancel at the same moment read `Deleted` and close `Canceled`, which is what the user asked for.
- **The job serializer change reaches other modules' untyped payload members** (§3.1; sweep of 38 local and 59
  GitHub `VirtoCommerce` handlers, 2026-11-24). Two are affected:
  - vc-module-task-management `WorkTask.Parameters` / `Result` (`JObject`): date and fractional tokens become string
    and decimal tokens; the notifications handler does not read them (code search).
  - vc-module-bulk-actions with vc-module-catalog: `PropertyValue.Value` (`object`) inside the polymorphic
    `BulkActionContext` arrives as a `string` instead of a UTC `DateTime` for a date property. Catalog stores it with
    `Convert.ToDateTime(value, CultureInfo.InvariantCulture)` (`PropertyValueEntity.SetValue`), which turns a "…Z"
    string into local time: **on a host whose time zone is not UTC — local Windows stands among them — a bulk update
    of a date property stores the value shifted by the host's UTC offset.** UTC hosts store the same value as today.
    Number properties get the exact decimal instead of a double. Tracked by a separate vc-module-catalog ticket (the
    same parsing affects any caller that passes a date string); stated in the README and the PR as a behaviour change.
- **The `Deleted` filter for a job not yet running** runs inside Hangfire's state transition, before the commit. A
  transition that fails after the filter has run leaves a notification closed `Canceled` for a job still queued.
  Accepted: the filter does no storage work of its own, and a failed transition is loud in Hangfire's log.
- **Hangfire raise** applies `AutomaticRetryAttribute` inside another filter's election; proven by test 9 on the
  Hangfire conformance fixture (MemoryStorage, instant retries) and by E2E on SQL storage.
- **Progress page start state**: before the first worker update the page knows only the job id. A job that dies
  before any worker update and before the dispatcher runs (an envelope that does not deserialize in the executor)
  leaves the page at "queued". E2E (AC 7).
- **The vc-module-search controller has the race this design avoids** (`SearchIndexationModuleController.IndexDocuments`
  sends its notification after `EnqueueAsync`). Outside this repo; recorded for the search module.
- **Scale-out** (read-from-code, not run): with `PushNotifications:ScalabilityMode` set, the hub protocol serializes
  with `TypeNameHandling.Objects` ("store full type information in JSON to deserialize push notifications on other
  instances", `SignalRBuilderExtensions.cs:29-34`), and `PushNotificationSynchronizerTask` reads with the same setting
  (`:35-39`) before saving into the local store (`:74-82`). So the derived `JobProgressPushNotification`, with
  `JobId`, `Outcome` and attempts, is what other instances store and serve. The same path already carries other
  modules' derived notifications. Not reproduced on a multi-instance stand; the single-instance 3.1097 stand cannot
  observe it.
- **The platform floor rises** (§3.4): a host on an older platform cannot install this module version. The companion
  vc-platform PR ships first; the module pins its alpha, then its release.
- **Live enum shape varies by deployment** (found 2026-11-24, outside this delivery): with no
  `PushNotifications:ScalabilityMode` the hub uses System.Text.Json, which ignores the Newtonsoft
  `[JsonConverter(typeof(StringEnumConverter))]` on `ProgressMessage.Level`, so the level arrives as a number live and
  as a string from storage, and admin code comparing `level === 'Error'` (vc-module-admin-angular-js
  `module-wizard-progress-step.js:34`) misses it on a single-instance host. Unifying the hub protocol changes the live
  shape of every module's notification; a separate vc-platform ticket, not this PR.

## 10. Change log

- 2026-11-20 — Scope extended (operator): the Hangfire part of VCST-6850 item 3 — a per-job `MaxRetryAttempts` may
  raise the retry count on Hangfire — and correct attempt numbering on Hangfire. VCST-6850 keeps backoff, retryable
  exceptions, the attempt number on `IJobExecutionContext`, and its docs.
- 2026-11-20 — Revised after spec review (two independent reviews, 20 findings): job id
  written by the worker instead of a facade re-send; Hangfire cancel by recorded intent; Hangfire raise by decision
  table; cancel of queued and retry-waiting jobs; timeout `OperationCanceledException` as failure; state across
  attempts; shared notifications; payload fidelity scoped to the payload type; outcome always delivered to wrappers;
  conformance capability flags and `ReferenceJobEngine`; storage folding; AC wording; §5 rows.
- 2026-11-23 — Operator: no accumulated message log (the current message and counts carry the state, as in the
  import module); completion interface named `IJobProgressComplete`. Following from the first: no carrying of counts
  or log into the next attempt (proposed by the lead, confirmed by the operator 2026-11-23).
- 2026-11-23 — Scope extended (operator): fix the GoogleCloudTasks sample so the callback passes the job id `Enqueue`
  returned (§3.8).
- 2026-11-23 — AC and test list reviewed with the operator: AC 2 states the silent harm (no job with the default
  payload); batch start time (AC 13) and the console outcome line (AC 17) moved to E2E; the separate Hangfire
  attempt-number and console-wrapper tests dropped, covered by test 4 on the Hangfire fixture; AC 23 added (Cancel
  hidden once finished).
- 2026-11-24 — Risks walked through with the operator: cancel of a finished job is fixed here (engine `Delete`
  refuses it; AC 24, test 13) — it was wrongly listed as out of scope, `JobsController` is in this repo; Hangfire
  cancel intent is recorded by the `Deleted` state filter (covers the dashboard); new conformance flags default to
  `false`, the one-job-id scenario runs on every engine; a ticket for the search module is drafted (live
  `DeleteDocumentsJobHandler` surface, send-after-enqueue race). Spec accepted by the operator.
- 2026-11-24 — Revised after plan review (two independent reviews), decisions by the operator: Hangfire classifies a
  cancel of a running job by reading the job's state from Hangfire storage, not by an intent recorded in
  `IJobCancellationStore` (lost across instances without Redis); the `Deleted` filter only closes jobs not yet
  running; `Outcome` is a string from the static class `JobOutcome`, not an enum (SignalR sends enums as numbers);
  test 2 dropped, AC 2 checked by E2E; the retry-waiting cancel is covered by a Hangfire filter unit test, other
  engines by E2E, and test 5 gains the dashboard delete and requeue cases. If test 1 shows `PolymorphJsonConverter`
  altering a decimal or a date-like string, implementation stops for a decision before `JobJsonSettings` changes.
- 2026-11-24 — Scope extended (operator), after test 1 stopped on the case above: `JobJsonSettings` reads with
  `DateParseHandling.None` and `FloatParseHandling.Decimal` (§3.1), after a sweep of untyped payload members in other
  modules; the bulk-actions date shift on non-UTC hosts is a risk (§9) with a separate vc-module-catalog ticket.
- 2026-11-24 — Scope changed (operator), after the Task 5a review: the completion seam moves into Platform.Core as
  `IJobProgress.Complete` with a default implementation, plus the `JobOutcome` enum (§3.4), in a companion vc-platform
  PR; the module's platform floor rises (lifts the earlier "no Platform.Core change"). The module interface
  `IJobProgressComplete` and the module `JobOutcome` constants, never released, are removed, and with them every
  `InternalsVisibleTo` this branch added: the Hangfire filter closes through the public reporter, and
  `BackgroundJobs.Data` builds the queued notification through a public factory. The Task 5a review found that the
  filter's closing notification dropped `JobId` for a job waiting for a retry (AC 12); it now carries the Hangfire job
  id, and the remaining loss (last message, counts) is the AC 12 exception in §3.5. Kept out of the platform PR: the
  attempt number on `IJobExecutionContext` (stays with VCST-6850; no handler reads its attempt today, and a Hangfire
  job requeued after failing would report its final attempt on a fresh run), and unifying the hub's enum shape (§9).
- 2026-11-24 — Operator, after the Task R review: a cancel that closes a job before an attempt starts shows that
  attempt and the job's total on every engine, not the envelope's defaults (§3.5); the AC 12 exception keeps the
  attempt numbers. Checked across Hangfire, RabbitMQ, InMemory, Reference and the GoogleCloudTasks sample.
