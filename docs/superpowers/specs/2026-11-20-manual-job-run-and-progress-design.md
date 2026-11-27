# Design — Run a job from the Admin Shell and follow its progress on every engine

**Date:** 2026-11-20
**Ticket:** VCST-6934 (also takes the Hangfire part of VCST-6850 item 3, see Change log)
**Scope:** `vc-module-background-jobs` (this repo). Companion PRs: `vc-platform` (completion contract, §3.4) and
`vc-module-admin-shell` (platform floor, §3.10); delivery of notifications
changed by vc-platform#3175 (§3.10).
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
  Admin Shell host installs no broadcast filter. AngularJS listens to the per-user `Send` only. vc-platform#3175
  removes this at the server (§3.10).

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
  A "Triggerable only" toggle filter in the table's search bar is on by default (a default search view), so the page
  opens on the jobs a person can run; turning it off shows every registered job. The search text filters by name.
  The analog is the host's Modules list (`search-bar`, `global-filters` with `{ toggle: true }`, `search-views`).
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
  with toast mode `progress`; the toast completes with the outcome by its kind: `Failed` → error, `Canceled` →
  warning, `Succeeded` → success. The toast's text stays the job title. `completedType` is typed `"success" | "error"`
  in `@vc-shell/framework` 2.13.0 while its toast runtime already renders `"warning"` (every 2.x release), so a
  companion vc-shell PR widens the type and the plugin pins that PR's preview build for type-checking only; the host
  Admin Shell needs no change. The bell entry uses the same three colours.

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
- **The four-argument `PushNotificationJobProgress` constructor** owns its notification whatever the envelope says, so
  a reporter built with it closes a notification the envelope shares. It is `[Obsolete]` (diagnostic `VC0016`); a
  reporter is built from the envelope (`PushNotificationJobProgress(IPushNotificationManager, JobEnvelope, string)`) or
  through `JobExecutionContextFactory`.
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
  The platform of §3.10 no longer folds; the unique description stays, and is harmless.
  `BackgroundJobs.Data` builds it through a public factory on `JobProgressPushNotification` for a queued job, so no
  assembly needs access to Core internals.
- **Notification sends never change what a job does.** `BestEffortNotificationSend.TrySendAsync(send, onFailure,
  onCanceled, cancellationToken)` (public, `VirtoCommerce.BackgroundJobs.Core.Notifications`) awaits the send and passes
  any exception to the caller's `onFailure` instead of throwing; an `OperationCanceledException` while the token is
  cancelled goes to `onCanceled` when the caller gives one. Each caller logs with its own constant message template and
  structured fields. It wraps the dispatcher's start report, retry announcement and outcome, the map coordinator's
  progress report, the batch close, `TryCompleteCanceledAsync`, the Queued send of the facade and of `MapReduceJob` (a
  failed send is logged and the enqueue goes ahead), and the enqueue-failed close below.
- **A failed enqueue closes the Queued notification.** When the engine's `Enqueue` throws after the Queued send, or, for
  a batch, the store, the item check or the fan-out enqueue does, the notification is closed `Failed` with the
  description "Enqueue failed: <error>" and the error as `Error`, and the original exception is rethrown.
  `JobProgressPushNotification.CreateEnqueueFailed` builds the close (public), and the extension
  `TryCloseEnqueueFailedAsync` sends it best-effort; the facade and `MapReduceJob` both close through it. Accepted
  trade-off: a failure report does not prove the job was not taken (a broker's publish can time out on its confirm
  after the message was routed); if a worker does run it, the worker's own sends replace the close, so the
  notification can show `Failed` and then change.
- **One builder produces every `JobProgressPushNotification`** (today built in `PushNotificationJobProgress.Report`/
  `Complete`, `JobProgressPushNotification.CreateQueued` / `CreateEnqueueFailed` and
  `MapReduceBatchNotification.SendCloseAsync`). For an
  **owned** notification:
  - `Started` and `Created` come from the envelope's `JobHeaders.EnqueuedAt` and stay the same on every update.
  - Progress updates send `IsNew = false`, so the notification is not raised again while the job runs and one the
    user has read stays read. The closing update (`Complete` of an owned notification, the reduce close of a shared
    one) sends `IsNew = true`: platform storage keeps the flag of the last send, so a finished job is unread until the
    user marks it read, including when the user read it while it ran. The Admin Shell keeps its own flag for a
    notification it already holds, so an open tab is not changed; the stored flag shows after a reload.
  - `ProcessedCount`/`TotalCount` keep their last known values when a `Report` omits them.
  - The description is the job's current message, and a `Report` that carries no message keeps it (a count-only
    update does not blank it); `ProgressLog` keeps today's content (the message of an update that carries one,
    plus the error entry on failure). Messages are not accumulated: the current message and the counts carry the state, as in
    the import module's notification ("Imported 900 of 1000, 100 errors").
  - `Complete` keeps the last progress message as the description and records the outcome in `Outcome` and
    `Finished`; the error goes to `Error`.
  - New fields: `JobId`, `Outcome`, `Attempt`, `TotalAttempts`, `Error`.
  - `Outcome` on the notification is a `string` holding the `JobOutcome` name, not the enum: the live channel and the
    history read serialize an enum differently — the System.Text.Json hub protocol as a number, MVC's
    `StringEnumConverter` as a string. An enum would reach the Admin Shell in two shapes, and a
    check for `"Failed"` would show a failure as a success. The enum is the C# contract; only the notification's
    field is serialized.
- **State across attempts.** Counts and the current message belong to the attempt: a retry runs the work again, so
  its first report starts from that attempt's own counts. `Started`, `Created`, `JobId` and the title stay the same
  across attempts; `Attempt` and `TotalAttempts` show which attempt the counts belong to.
- **Shared notifications** (map/reduce): a child envelope cannot carry stable `Started`/`JobId` for the batch.
  `Started` = the batch start time recorded on `MapReduceBatch`; `JobId` stays null; the description is the latest map
  message. A map task's progress update never closes the notification; a coordinator's final-attempt failure does
  (§3.5).
  - The batch start reaches the map children as the `JobHeaders.BatchStartedAt` header. `FanOutCoordinator` opens an
    ambient scope with `MapReduceBatch.Started` around its child enqueue loop through a public
    `JobEnqueueContext` member beside `BeginRun` (the same `AsyncLocal` shape), and `EnqueueCore` stamps the header only
    on an enqueue that carries a `ProgressNotificationId`. The platform `EnqueueOptions` has no header slot.
  - Every batch close (the reduce's outcome, and the final-attempt failure close of §3.5) is built through the
    notification builder with `MapReduceBatch.Started`, so the closing replacement keeps the batch start time. One
    internal helper, `MapReduceBatchNotification`, sends it for the fan-out, map and reduce coordinators. A batch stored
    before the upgrade has no `Started` and behaves as before.

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
  - attempt starts → before the handler runs, one update to an **owned** notification carrying the job id and the
    attempt, with no message and no counts, so the page shows a running job even when the handler never reports
    (the queued notification has no job id, and the page finds its job by job id);
  - handler returned → close `Succeeded`;
  - exception on the final attempt → close `Failed` with the error;
  - exception with a retry pending → keep open, message "attempt N of M failed, retrying" with the error — only on an
    **owned** notification (a map task's failure writes nothing to the shared bar, which carries the batch, not one
    task's attempt), and only while the dispatch token is **not** cancelled: with the token cancelled, whether a
    retry follows is the engine's to decide (a cancel closes `Canceled`, a shutdown runs the job again), so the
    dispatcher announces nothing;
  - `OperationCanceledException` while the dispatch token is cancelled → keep open (requested cancel, host shutdown
    or a lost distributed lock — the dispatcher cannot tell them apart; the engine decides, below);
  - `OperationCanceledException` while the dispatch token is **not** cancelled (a timeout inside the handler) → a
    failure like any other exception.
- **A map/reduce batch closes `Failed` when a coordinator fails on its final attempt.** The fan-out and reduce jobs are
  enqueued without a `ProgressNotificationId`, so `context.Progress` does nothing for them, and a map task's `Complete`
  leaves the shared bar alone (§3.4). `FanOutCoordinator`, `MapCoordinator` and `ReduceCoordinator` therefore close the
  batch's notification through `IPushNotificationManager` with the batch's own title, start and total, and the
  description "Fan-out failed: <message>", "Map failed: <message>" or "Reduce failed: <message>". `FanOutCoordinator`
  and `MapCoordinator` take `IPushNotificationManager` in their constructors for it.
  - For the fan-out it covers any exception after the batch is loaded; for a map task, any exception after the batch is
    loaded (the batch store's `SaveResultAndCountAsync` and `TryBeginReduceAsync`, and the reduce enqueue, whose failure
    still releases the reduce claim first); for the reduce, the read of the map results, the `FailFast` check, the
    handler and its setup (not the batch read before them, nor the success close and `CompleteAsync` after them). A
    map item handler's own failure is recorded in the batch and does not throw, except an
    `OperationCanceledException`, which `MapCoordinator.RunMapAsync` rethrows.
  - The condition is the dispatcher's final attempt: `JobExecutionContextFactory` sets the context's final-attempt flag
    from the envelope, and a context built any other way never closes a batch. A cancel (an
    `OperationCanceledException` while the token is cancelled) leaves the bar open for the engine.
  - The original exception is rethrown unchanged, so the engine's retry or dead-letter acts on the real cause; a failed
    close is logged, never thrown.
- **A closed batch notification stays closed.** Platform storage keeps only the last send to a notification id. A
  fan-out or map task that fails on its final attempt after some map tasks were already enqueued closes the bar
  `Failed`, and the map tasks that then run send "Mapped k/N" under the same id, which replaces the close and, since no
  later close follows, leaves the bar running for good.
  - `IMapReduceBatchStore` gains `MarkProgressClosedAsync(batchId, close)` and `GetProgressCloseAsync(batchId)`, with
    default implementations (mark does nothing, get returns `null`), so a third-party store compiles and keeps the
    earlier behaviour. The marker holds the close itself, a `ProgressClose` (description, outcome, finish time), so a
    map task can send the same close again. The in-memory store keeps the record on the batch entry; the Redis store
    keeps it as JSON in the `progressclosed` key with the store's TTL, deleted by `CompleteAsync` with the batch's
    other keys.
  - `MapReduceBatchNotification.CloseAsync` builds the record (finish time now), marks the batch, then sends the
    close, so every coordinator close of a batch with a notification marks: the reduce's success, fault and failure,
    and the fan-out and map failures. The enqueue-failed close does not go through it; worker sends may replace that
    close, as accepted above. A failure to mark is logged and the close is still sent. The close notification is built
    in one place, which the map task's re-send uses too.
  - `MapCoordinator` checks the marker before each throttled progress report and skips the report when it is set. When
    it is not set, the task sends the report and reads the marker again; a record found then is sent again as the
    close. The re-read runs even when the report send threw, because the report may already be stored when the send
    fails. The closer marks and then sends, the map task sends and then reads, so a report stored after the close was
    sent is followed by a read that sees the mark and puts the close back. All of it runs inside the best-effort send,
    so a failed read or send is logged like a failed report and never fails the map task, which by then has saved its
    result and may still have to trigger the reduce.
  - The reduce close is not gated. A batch is not deleted on a coordinator failure, so that a replay of the
    dead-lettered job can finish it; a replay that completes the batch closes it `Succeeded`.
  - What remains, all accepted:
    - A map report sent after the reduce's success or `FailFast` close **and** after its `CompleteAsync` deleted the
      record. The handshake finds no record then, and the report replaces the close. This is the delayed-report race
      the baseline already had. A fan-out close is concurrent with running map tasks by construction, which is why the
      handshake exists; the reduce close follows the last map report through a queue hop, so only a report delayed
      past that hop and the `CompleteAsync` can land after it.
    - A `Failed` close that lands after the reduce's `Succeeded` close, on a batch that still completed (a fan-out
      whose final checkpoint failed after every map task was enqueued, or an engine that leaves `TotalAttempts` null):
      either a map task re-read the `Failed` record and the reduce marked and sent its close before the task's re-send,
      or a `Failed` closer read the batch and the reduce sent its close before the closer's send. No `CompleteAsync` is
      needed for either. Both are rare: the reduce's close has to fall inside that short gap in one task.
    - A failed marker write returns the race for that batch: the close is still sent and only an error is logged, so a
      map task finds no record and its report can replace the close.
    - Several instances: with Redis configured, the platform of §3.10 keeps one notification history for all of them,
      so the storage order is global and the handshake holds across instances. With Azure SignalR Service and no Redis
      each instance keeps its own history, which no module can reconcile. Read from the platform code, not run on
      several instances.
    - On the Redis store, a `Failed` closer that read the batch before the reduce's `CompleteAsync` and marks after it
      recreates the `progressclosed` key as an orphan, which expires with the store's TTL (7 days). A map task still in
      its report step can read it and re-send that `Failed` close, which is the case of the `Failed`-after-`Succeeded`
      bullet above. The in-memory store does not recreate the record.
- **A `Failed` close is not sent for a batch that is already gone.** `CloseFailedOnFinalAttemptAsync` reads the batch
  (best effort) before it closes. When the read returns nothing, because the reduce completed the batch or it expired,
  no close is sent and no marker is set: a map task that fails on its final attempt after the reduce closed the batch
  `Succeeded` would otherwise replace that close with `Failed`. A failed read closes as before.
- **The reduce's results read is inside the final-attempt close.** `ReduceCoordinator` reads the map results and applies
  the `FailFast` check inside the scope whose catch closes `Failed` on the final attempt, so a store failure there on
  every attempt does not dead-letter the reduce with the bar left on "Mapped N/N". The batch read stays first and
  outside, since there is nothing to close without a batch. The success close, the `FailFast` close and their
  `CompleteAsync` stay outside: a `CompleteAsync` failure after a close must not send a second close in its place.
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
- **A requested cancel whose handler then fails with another exception** (not `OperationCanceledException`) on a
  non-final attempt is still a cancel. The dispatcher sees a failure with the dispatch token cancelled and keeps the
  notification open without announcing a retry; the engine then honours the cancel: no further attempt runs the handler, and the notification
  closes `Canceled`.
  - InMemory and Reference: the cancellation check before the next attempt closes it, with that attempt's number
    (below).
  - RabbitMQ: the retry is re-published with `Attempt + 1`, and the cancel request is not cleared while a retry of the
    job is in flight, so the retry's pre-dispatch discard closes it (below).
  - Hangfire: the job is already `Deleted`, so no retry follows; `HangfireJobExecutor` classifies by the stored state
    on any exception thrown during a non-final attempt while the dispatch token is cancelled, not only on
    `OperationCanceledException`, and closes `Canceled` with the current attempt's numbers.
- **On the final attempt, a requested cancel whose handler then fails with another exception is a failure.** The
  dispatcher closes `Failed` with the error, on every engine, and no engine replaces that close with `Canceled`: no
  retry was pending for the cancel to stop, and the error is the only record of why the job ended. An
  `OperationCanceledException` on the final attempt with a cancel requested still closes `Canceled`.
- **A job canceled before it runs, or while it waits for a retry**, is closed `Canceled` too:
  - Hangfire: an apply-state filter on `Deleted`, for a job that was `Enqueued`, `Scheduled` (a retry wait included)
    or `Awaiting`, reads the envelope from the job's first argument and closes its notification; no executor will
    run for such a job. A job that was `Processing` is left to the executor, a finished one is left alone. The filter
    builds `PushNotificationJobProgress` from the envelope and the Hangfire job id, so `JobId` stays what earlier
    attempts wrote (storage replaces a notification by id), and waits for `Complete` synchronously, as
    `IPushNotificationManager.Send` itself does, because Hangfire state filters are synchronous. It fills `Attempt`
    and `TotalAttempts` the way `HangfireJobExecutor` does, through one shared helper: `RetryCount` from the job's
    parameters (read through the state context's storage connection) + 1, and the retry limit §3.6 applies + 1. A
    retry-waiting job's last message and counts are not in the envelope and are not kept (AC 12's exception, which
    covers the RabbitMQ and InMemory closes below for the same reason);
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
| `DeletedState` reached from a `FailedState` (a global attribute with `OnAttemptsExceeded = Delete` gave up) | `RetryCount < L` | put the traversed `Failed` back as the candidate and apply the copy as in the `FailedState` row; on the final attempt the global `Delete` stands |
| `DeletedState` reached from a `FailedState` | `RetryCount >= L` | keep `Deleted` |

A `DeletedState` with no traversed `FailedState` (a dashboard delete, a cancel) is never touched: in Hangfire.Core
v1.8.23 only `AutomaticRetryAttribute` turns a `Failed` candidate into `Deleted`. Each extended attempt under `Delete`
leaves "Failed", "Deleted" and "Failed" rows before the retry in the job's history (Hangfire records every candidate
an election passed through) and Hangfire's "automatically deleted" warning in the log; the job itself is not deleted
until the per-job limit is spent.

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

### 3.10 Companion changes: delivery (`vc-platform`) and the Admin Shell (`vc-module-admin-shell`)

- vc-platform#3175 sends `SendSystemEvents` to the creator's hub group only: the creator and the users an
  `IPushNotificationAudienceProvider` admits, administrators included in the restriction. With Redis configured the
  history is shared by all instances, otherwise it stays in memory; both keep a user's newest 20 notifications for 24
  hours by default. A notification no user created (a recurring job's `system:<id>`, a module install's `system`)
  reaches nobody live.
- The Admin Shell host filters nothing. It requires the platform that carries the change, in its manifest and its
  `Platform.Core` reference, as this module does.

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
| Broadcast leak | the platform sends each notification to its creator's group only (vc-platform#3175) | host filter in `vc-module-admin-shell` (built first, removed 2026-11-27): hides other users' notifications while the data still reaches the browser, and would drop the ones an audience provider delivers; filter in this plugin: one filter per app |

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
| Platform storage, new notification equal to the previous one in title, type and description | — | folded into the previous notification (`PushNotificationInMemoryStorage.cs:36-48`); vc-platform#3175 stops folding | read-from-code |
| SignalR `Send` (creator) / `SendSystemEvents` (all) | — | same members; the hub protocol's serializer settings differ from MVC (camelCase, `$type` only in scale-out mode). vc-platform#3175 (`318225a69`): `SendSystemEvents` goes to the creator's group, and every mode uses the default JSON protocol | read-from-code |
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
5. WHEN the user opens the Background jobs page, THEN it SHALL list the triggerable jobs; WHEN the user turns the
   "Triggerable only" filter off, THEN it SHALL list every registered job with its triggerable flag. — E2E
6. WHERE a job is triggerable and the user has `background-jobs:execute`, THE page SHALL offer Run with an optional
   JSON payload; Run SHALL enqueue with progress reporting and the job name as title, and open the job's progress
   page. — E2E
7. WHILE a job runs, THE progress page SHALL show its title, processed and total counts, "attempt N of M" (or
   "attempt N" when the total is unknown), the job's current message and elapsed time, updated as the job reports, including a
   job that finished before the page opened. — E2E
8. WHEN a job ends, THEN its owned notification SHALL be closed with outcome `Succeeded`, `Failed` with the error, or
   `Canceled`, and the progress page SHALL show that outcome and the toast SHALL show it by its kind (icon and
   colour). — test (dispatcher, all engines); E2E (UI)
9. WHEN an attempt fails, `Attempt < TotalAttempts` and the dispatch token is not cancelled, THEN the job's owned
   notification SHALL stay open with "attempt N of M failed, retrying" and SHALL NOT be closed as failed, on every
   engine. — test
10. WHEN the user cancels a job that is running, queued or waiting for a retry, THEN its notification SHALL be closed
    as `Canceled` on every engine that declares cancel support. WHEN an `OperationCanceledException` arises while the
    dispatch token is cancelled and no cancel was requested, THEN the notification SHALL stay open. WHEN it arises
    while the dispatch token is not cancelled, THEN it SHALL be handled as a failure. WHEN a cancel is requested
    during a non-final attempt whose handler then fails with another exception, THEN the handler SHALL NOT run again
    and the notification SHALL close as `Canceled`. WHEN a cancel is requested during the final attempt whose handler
    then fails with another exception, THEN the notification SHALL close as `Failed` with the error, on every engine.
    — test; E2E for a queued RabbitMQ message (§8 item 5)
11. WHEN the dispatcher fails before the handler runs (payload, type or handler resolution), THEN the failure SHALL be
    handled like a handler failure: closed `Failed` on the final attempt, open while a retry is pending. — test
12. FOR an owned notification, every update SHALL keep `Started`, `Created` and `JobId` unchanged, keep the last known
    counts within an attempt when a report omits them, and keep the job's last message as the description on
    completion. The exception: a job canceled before an attempt starts — a Hangfire job waiting for a retry, a
    RabbitMQ retry re-published but not yet consumed, an InMemory or Reference job between attempts — is closed with
    `JobId`, `Started` and its attempt numbers kept but without the previous attempt's last message and counts, which
    no engine carries in the envelope (§3.5). — test
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
19. WHILE the platform carries vc-platform#3175, THE user SHALL NOT receive a notification created by another user,
    live or from the history, unless an audience provider admits them. — E2E (platform behaviour)
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
25. WHEN a worker starts an attempt of a job that owns its notification, THEN before the handler runs the
    notification SHALL carry the job id and the attempt, so the progress page shows the job as running even when the
    handler never reports; a notification the job does not own SHALL NOT be written. — test (dispatcher); E2E (page)

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
   leaves it open; one with the token not cancelled closes `Failed` on the final attempt. A cancel during a
   non-final attempt whose handler then throws another exception: the handler does not run again and the
   notification closes `Canceled`, its description carrying no retry announcement — on every fixture that supports
   cancellation and retries (Hangfire, InMemory and Reference in CI; RabbitMQ with a broker via
   `VC_CONFORMANCE_RABBITMQ`, which CI does not run). The description half can fail only on Hangfire, the one engine
   that closes from the attempt's own reporter; the others close from a fresh one. The same on the final attempt
   closes `Failed` with the error on those fixtures. A retry-waiting job: a unit
   test of the Hangfire `Deleted` filter with old state `Scheduled` closes its notification `Canceled`. A queued
   RabbitMQ message: no fixture can hold one (an unserved queue is never consumed, so a test there could not fail),
   so its discard close is checked by E2E on a stand with a broker, one consumer slot held by a blocking job. Wrong
   state. (AC 10)
6. Dispatcher: payload that does not deserialize, on final and non-final attempts. Item left in a wrong state. (AC 11)
7. Notification builder: stable `Started`/`Created`/`JobId` across updates and attempts, counts kept within an
   attempt, the current message kept by a count-only update and on completion; `IsNew` false on progress updates and true on the close (owned
   `Complete` and the shared reduce close). Lost or wrong value: a finished job stored as read is missed silently.
   (AC 12, §3.4)
8. Shared notification: a map task's update does not close it. Wrong state. (AC 13)
9. Conformance: `MaxRetryAttempts = N` above the global default gives N + 1 runs on every engine; on Hangfire also with
   zero-delay retries, and lowering below the global default. Data, a job given up early or retried too often. (AC 14)
10. Conformance: the job id in the worker's context equals the id `Enqueue` returned, on the first attempt and on a
    retry; the facade sends nothing after `Enqueue` returns. Lost message (the page never finds its job). (AC 21)
11. Two enqueues of the same job in a row produce two notifications. Lost message. (AC 22)
12. Retired 2026-11-27 with the Admin Shell broadcast filter (§3.10).
13. Conformance (`SupportsStatusQuery` and `SupportsDelete`): `Delete` of a succeeded job returns `false` and its
    status stays `Succeeded`. Data, a finished job's history rewritten. (AC 24)
14. Dispatcher: for a job that owns its notification, an update carrying the job id and the attempt reaches the
    reporter before the handler runs; for a map task (shared notification) none does. Wrong state: a running job
    shown as waiting for a worker invites a second Run. (AC 25)
15. Conformance, gated on `SupportsRetry` and `ReportsAttemptBudget`: a map/reduce batch's shared notification closes
    `Failed` when its reduce handler fails, when the reduce handler is not registered, when its fan-out fails, or when
    the map task that completes the batch fails before the reduce is enqueued, each on the final attempt
    (`MapReduce_Reduce_Failure_On_Final_Attempt_Closes_Notification_Failed`,
    `MapReduce_Reduce_Setup_Failure_On_Final_Attempt_Closes_Notification_Failed`,
    `MapReduce_FanOut_Failure_On_Final_Attempt_Closes_Notification_Failed`,
    `MapReduce_Map_Failure_On_Final_Attempt_Closes_Notification_Failed`). The kit's batch store fails the fan-out or the
    join step for a batch whose reduce handler is the matching marker type. In the map case both map tasks may observe
    the batch complete and both fail on the claim, so the bar may close twice, always `Failed`, and ends `Failed`; in
    the other cases it closes once. Wrong state: the batch's bar stays "running" for good.
16. A failed enqueue closes the Queued notification `Failed` and rethrows the original exception, also when the closing
    send throws (`Enqueue_EngineThrows_ClosesQueuedNotificationAsFailed_AndRethrows`,
    `Enqueue_EngineThrowsAndClosingSendThrows_RethrowsTheEngineError`,
    `Enqueue_FanOutEnqueueThrows_ClosesBatchNotificationAsFailed_AndRethrows`,
    `Enqueue_ItemOfWrongType_ClosesBatchNotificationAsFailed_AndThrowsArgumentException`). Wrong state: the admin's bell
    shows a job that never ran as queued for good.
17. A failing notification send does not change the job: the dispatcher's start report, success close and failure
    report (`Dispatch_StartReportFails_HandlerStillRuns`, `Dispatch_SuccessCloseFails_DoesNotFailTheAttempt`,
    `Dispatch_FailureReportFails_StillThrowsTheHandlerException`), the map progress report on the last item
    (`MapTask_ProgressReportOnLastItemThrows_StillEnqueuesReduce`) and the reduce's success close
    (`Reduce_SuccessCloseSendThrows_DoesNotFailTheAttempt`). Data: a finished handler run again, or a reduce never
    triggered.
18. RabbitMQ consumer: after a failed attempt whose ack fails (a retry published, or a final failure dead-lettered) the
    cancel flag stays and the redelivery is discarded without running the handler; a final failure that is acked
    clears it (`Delivery_FailedAttemptWithRetryPublished_AckFails_KeepsCancelFlagAndDiscardsRedelivery`,
    `Delivery_FinalFailure_DeadLettered_AckFails_KeepsCancelFlagAndDiscardsRedelivery`,
    `Delivery_FinalFailure_Acked_ClearsCancelFlag`). Data: a cancelled job run again.
19. Conformance, gated on `SupportsRetry` and `ReportsAttemptBudget`: a fan-out that fails on every attempt after it
    enqueued map tasks (a batch of 30 items; the kit's batch store fails the fan-out's first checkpoint) closes the
    shared notification `Failed`, and the last update to it is still that close after a map task has run past it
    (`MapReduce_FanOut_Failure_After_Enqueuing_Map_Tasks_Keeps_Notification_Failed`). One map task of the batch waits
    in its handler until the test has seen the close, so a map task is sure to run after it; the others pass at once,
    so the wait blocks one worker and the fan-out's retries still run. The scenario needs an engine that runs at least
    two jobs at once: on a single-slot engine the waiting map task can block the fan-out's retries and the scenario
    times out, and its gate is only `SupportsRetry` and `ReportsAttemptBudget`. Wrong state: the batch's bar shows
    running for good. (§3.5)
20. Conformance, gated as item 19: a reduce whose read of the map results fails on every attempt closes the shared
    notification `Failed` on the final one, with the error in the description, and the last update is still that close
    (`MapReduce_Reduce_Results_Read_Failure_On_Final_Attempt_Closes_Notification_Failed`; the kit's batch store fails
    `GetResultsAsync` for a batch whose reduce handler is the matching marker type). Wrong state: the bar stays on
    "Mapped N/N". (§3.5)
21. A map task whose first read of the close marker finds none and whose second finds one sends its report and then the
    close (`MapTask_BatchClosedDuringItsReport_SendsTheCloseAgainAfterTheReport`). Wrong state: the bar is left running
    by a report that raced the close. (§3.5)
22. A map task that fails on its final attempt closes the batch `Failed` while the batch exists and sends no close, and
    sets no marker, when the batch is gone
    (`MapTask_FailsOnFinalAttempt_ClosesFailedOnlyWhileTheBatchExists`). Wrong state: a `Succeeded` close replaced by
    `Failed`. (§3.5)

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
- **Progress page start state**: before the dispatcher's attempt-start update (AC 25) the page knows only the job id.
  A job that dies before the dispatcher runs (an envelope that does not deserialize in the executor) leaves the page
  at "queued". E2E (AC 7).
- **The vc-module-search controller has the race this design avoids** (`SearchIndexationModuleController.IndexDocuments`
  sends its notification after `EnqueueAsync`). Outside this repo; recorded for the search module.
- **Scale-out** (read-from-code at `318225a69`, not run): with Redis the history stores each notification with its
  concrete type (`PushNotificationJsonSerializer`), so `JobProgressPushNotification` with `JobId`, `Outcome` and
  attempts is what every instance serves, provided it survives the Newtonsoft round trip. E2E on the stand.
- **A running job can leave the history** (read-from-code at `318225a69`): the history keeps a user's newest 20
  notifications by creation time for 24 hours, and a job keeps its start time across updates. The progress page then
  finds no notification for the job id.
- **The platform floor rises** (§3.4): a host on an older platform cannot install this module version. The companion
  vc-platform PR ships first; the module pins its alpha, then its release.
- **Live and stored enum shapes differ** (found 2026-11-24, outside this delivery): the hub uses System.Text.Json,
  which ignores the Newtonsoft `[JsonConverter(typeof(StringEnumConverter))]` on `ProgressMessage.Level`, so the level
  arrives as a number live and as a string from the history, and admin code comparing `level === 'Error'`
  (vc-module-admin-angular-js `module-wizard-progress-step.js:34`) misses it. Unifying the hub protocol changes the
  live shape of every module's notification; a separate vc-platform ticket, not this PR.

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
- 2026-11-25 — Operator, after the Task 5b and 5c reviews and the first run against a RabbitMQ broker: (1) a
  conformance case for a cancel during an attempt whose handler then fails with another exception (§3.5, §8 item 5).
  The RabbitMQ and Hangfire behaviour it requires is read from code, not yet observed: today RabbitMQ clears the
  cancel request when the attempt settles, so its retry would run, and Hangfire classifies only an
  `OperationCanceledException`, so the notification would stay at "retrying". The case's first run decides. (2) The
  discard of a queued RabbitMQ message is checked by E2E only, on a stand with a broker; CI runs no broker. (3) AC 12's
  exception covers every close made before an attempt starts, on every engine, not only Hangfire's.
- 2026-11-25 — Operator, after the Task 6b review: on the final attempt, a requested cancel whose handler then fails
  with another exception closes `Failed` with the error on every engine (§3.5, AC 10, §8 item 5). Observed first: the
  case's first runs confirmed both read-from-code predictions of the previous entry (Hangfire stayed at "retrying",
  RabbitMQ ran the retry); after the fixes, Hangfire classified any exception by stored state on every attempt and so
  replaced the dispatcher's final `Failed` close with `Canceled`, dropping the error, while InMemory and RabbitMQ
  ended `Failed`. Chosen over `Canceled` everywhere because no retry was pending for the cancel to stop and the error
  is the only record of why the job ended.
- 2026-11-25 — Operator, after the Task 7, 9 and 11b reviews and a reworked question round: (1) the Hangfire per-job
  retry filter also continues when a global `OnAttemptsExceeded = Delete` gave up (§3.6 rows); the gap came from
  this branch (the per-job total now drives the notification), and no local VirtoCommerce code registers `Delete`.
  (2) The batch start time reaches map children through an ambient `JobEnqueueContext` scope (§3.4); the platform
  `EnqueueOptions` has no header slot, and `InternalsVisibleTo` stays removed, so the member is public. (3) A
  canceled job's toast is a warning (§3.3, AC 8 reworded: the toast shows the outcome by its kind, not as text),
  through a type-only vc-shell PR and a preview pin.
- 2026-11-25 — Operator, during E2E: the jobs page opens filtered to triggerable jobs (§3.3, AC 5 reworded); a list
  of jobs a person cannot run is noise on a page made for running them. Schedule columns (cron, next and last run)
  were asked for too and go to a separate work item: they need a Hangfire last-run read the module does not have.
- 2026-11-25 — Operator, after an E2E finding: the closing update sends `IsNew = true` (§3.4). Observed: with every
  update and the close sent as `IsNew = false`, storage kept finished jobs as read (`newCount: 0`), and the Admin
  Shell badge disappeared on reload; on dev the stored close was unread. Chosen over raising it on every update
  (dev), which marks a read notification unread again on each report. Whether the close should also move `Created`
  to the completion time (the Admin Shell reloads only the 10 most recent by `Created`) is left to pass 2.
- 2026-11-25 — Whole-branch behaviour review, operator decisions:
  (1) The dispatcher sends an attempt-start update to an owned notification before the handler runs (§3.5, new
  AC 25, test 14). Observed on the stand: a job that never reports stayed "Queued — Waiting for a worker" for its
  whole run, because the queued notification has no job id and the page finds its job by job id.
  (2) The "retrying" announcement goes only to an owned notification and only while the dispatch token is not
  cancelled (§3.5, AC 9 reworded, test 5). Before, a map task's failure wrote one task's attempt and zero counts into
  the shared bar, and on Hangfire a canceled job closed `Canceled` with "attempt N of M failed, retrying" as its
  description; with the token cancelled the dispatcher cannot know whether a retry follows.
  (3) A `Report` without a message keeps the current one (§3.4); a count-only update blanked the progress page's
  message.
  (4) The close keeps `Created` at the enqueue time (AC 12 unchanged). The Admin Shell reloads the most recent
  notifications by `Created` while its badge counts every unread one, so a long job's unread close can fall outside
  the reloaded list; that is the shell's list window, not this notification's data, and is taken up there.
  (5) Tests that pinned only the batch start time (E2E by AC 13) are removed under §7.
- 2026-11-26 — Operator, after the whole-branch review round: (1) `JobProgressPushNotification.CreateEnqueueFailed` is
  public; the shared close `TryCloseEnqueueFailedAsync` calls it, and the facade (Data) and `MapReduceJob` (Core) both
  close a failed enqueue through that extension (§3.4). (2) The dispatcher always delivers `Complete` to the context's
  progress, owned notification or not; the reporter decides whether to close (§3.4). (3) The four-argument
  `PushNotificationJobProgress` constructor is `[Obsolete]` (VC0016) instead of removed, because it was released
  (§3.4). (4) One public best-effort helper, `BestEffortNotificationSend.TrySendAsync`, and each caller logs with a
  constant message template: a template passed to a helper as a parameter trips CA2254 and was rejected (§3.4).
  (5) The "Enqueue failed" close is kept with its trade-off: a failure report may be wrong about a
  job a broker did route, and the worker's own sends then replace the close (§3.4). (6) The batch close is one shared
  internal helper (`MapReduceBatchNotification`) used by the fan-out, map and reduce coordinators, not a copy per
  coordinator; a fan-out or map failure on the final attempt now closes the batch `Failed` like the reduce (§3.5).
- 2026-11-26 — Operator, after a defect found in the previous entry's item 6: a map report sent after a fan-out or map
  failure close replaced the close and left the bar running (§3.5). (1) The batch store records that the notification
  was closed, through two `IMapReduceBatchStore` members with default implementations, and map progress reports skip a
  closed batch. Abandoning the batch on a coordinator failure (deleting it) was rejected: a replay of the dead-lettered
  job would find no batch and skip. (2) The `FanOutCoordinator` and `MapCoordinator` constructors
  that gained `IPushNotificationManager` (§3.5) stay a breaking change: they are DI-registered, and an `[Obsolete]`
  overload would need a no-op notification manager, which would silently lose the close.
- 2026-11-26 — Operator, after the review of the closed-notification marker: (1) a close handshake: the marker holds
  the close (`ProgressClose`: description, outcome, finish time) and a map task that sends its report re-reads the
  marker and sends the close again when it is set, so a report that raced the close no longer leaves the bar running
  (§3.5). Accepting the check-then-send race was rejected: a fan-out close is concurrent with running map tasks by
  construction, so the window is not the rare one it was described as. The store's marker members take and return the
  record instead of a flag, before any release. (2) A `Failed` close is skipped for a batch that is already gone.
  (3) The reduce's results read and `FailFast` check moved inside the final-attempt close scope; the success and
  `FailFast` closes and their `CompleteAsync` stay outside. Scenario 20 holds one map task until the close was seen,
  and scenario 21 covers (3).
- 2026-11-26 — After the review of the close handshake: (1) the map task re-reads the marker also when its report send
  threw, because the report may already be stored when the send fails (§3.5,
  `MapTask_ReportSendThrowsAfterItWasStored_SendsTheCloseAgain`). (2) The accepted remainder in §3.5 gains a failed
  marker write, the storage order across instances, and an orphan marker left by a `Failed` closer, and the reasoning
  of the `Failed`-after-`Succeeded` case is corrected: no `CompleteAsync` is needed for it. (3) §3.4 names the
  builder `SendCloseAsync`. (4) §8 item 19 states that its scenario needs an engine that runs two jobs at once.
- 2026-11-27 — The release requires vc-platform#3175 (operator decision). Notifications reach their creator's group
  only and the history can live in Redis, so the Admin Shell broadcast filter is removed (§3.10, AC 19, test 12), the
  platform floor of both modules rises to the platform that carries the change, and the multi-instance and scale-out
  notes are rewritten (§3.5, §9). New risk: a running job can leave the history.
