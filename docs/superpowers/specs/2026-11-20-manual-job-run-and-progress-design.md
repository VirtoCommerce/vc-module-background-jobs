# Design — Run a job from the Admin Shell and follow its progress on every engine

**Date:** 2026-11-20
**Ticket:** VCST-6934 (also takes the Hangfire part of VCST-6850 item 3, see Change log)
**Scope:** `vc-module-background-jobs` (this repo). Companion PR: `vc-module-admin-shell` (broadcast filter, §3.9).
No Platform.Core change: the module keeps its floor of platform **3.1097.0**.
**Code read at:** `vc-module-background-jobs` `5ba4054` (dev), `vc-platform` `7b08741c6` (dev, 3.1102.0),
`vc-module-admin-shell` `b8fa323c5` (dev), `@vc-shell/framework` tags `v2.7.0` and `v2.12.0`.

> **Status (2026-11-20): draft before spec review.** The external contract (§5)
> was measured on platform 3.1097.0. Open before revising: how the progress page correlates a started job with its
> notification (the `JobId` re-send of §3.4 races a fast worker) — first check how existing modules link a job id to
> their notification.

## 1. Context and problem

A job registered with `AddBackgroundJob<THandler, TPayload>(triggerable: true)` is meant to be started by a person,
but nothing in the admin lets a person start it, and once it runs there is no reliable place to watch it.

- The only entry point is `POST api/background-jobs/enqueue`. The module ships no admin UI
  (`Web/Scripts/module.js` registers nothing; there is no `vue/` plugin). The Hangfire dashboard is a developer tool
  behind `background_jobs:manage`, exists only on the Hangfire engine, and cannot create a job.
- On platform 3.1097–3.1099, the module's supported floor, the enqueue payload does not bind: `EnqueueJobRequest.Payload`
  is a `System.Text.Json.JsonElement` while the platform's MVC formatter is Newtonsoft, so an object payload arrives
  `Undefined` and the controller enqueues `{}`. The job runs with a default payload and ends Succeeded. Observed on
  platform 3.1097.0 with this module's `dev` (§5): a `DeleteDocumentsJobHandler` payload with both fields set was
  stored as `{"documentType":null,"documentIds":null}` and the job ended Succeeded. Platform 3.1100
  added a global `JsonElementJsonConverter` (`Startup.cs:204`, commit `48e8f0c82`, for OpenIddict) that makes it bind,
  but it still rounds large decimals.
- The progress notification loses data on every update: each `Report`/`Complete` builds a new
  `JobProgressPushNotification` and storage replaces it by id, so `Started` is dropped, `Created` and `IsNew` reset,
  `ProgressLog` holds one message, counts drop to 0 when a `Report` omits them, and `Complete` overwrites the last
  message with "Completed".
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
2. The progress notification tells the truth on every engine: stable start time, counts, the message log, the
   attempt number out of the total, and a final outcome (succeeded, failed with the error, canceled).
3. A failed attempt that will be retried does not report the job as failed.
4. The enqueue payload reaches the handler unchanged on every supported platform version.
5. A per-job retry limit means the same on every engine, raising included.
6. Hangfire dashboard shows job progress again.
7. A person sees only their own jobs' progress in the Admin Shell.

**Non-goals (this delivery)**
- AngularJS UI of any kind. AngularJS is legacy; new functionality goes to the Admin Shell only. AngularJS keeps the
  catch-all notification templates as they are.
- Engine-agnostic job-state store, a job list endpoint, progress fields on `GET api/platform/jobs/{id}`, and a truthful
  status on RabbitMQ (follows in the next delivery, on top of the completion seam of §3.4).
- Anything needing a Platform.Core release: recurring-schedule progress/title, a handler-visible "skipped" outcome,
  per-job permissions, the attempt number on `IJobExecutionContext`, the `EnqueueOptions.UniqueKey` xmldoc.
- VCST-6850 items other than the Hangfire raise: backoff delays, retryable exception lists, docs page.
- Real `UniqueKey` de-duplication.
- Changing how the global retry default resolves (`VirtoCommerce:BackgroundJobs:MaxRetryAttempts`, else the legacy
  `VirtoCommerce:Hangfire:AutomaticRetryCount`).
- The search module's triggerable `DeleteDocumentsJobHandler` (§8, risk).

**Anti-goals**
- A notification that reads "failed" while a retry is pending, or "in progress" after the job has ended.
- A REST caller able to write into a notification it does not own.

## 3. Design

### Glossary

| Term | Meaning | Code |
|---|---|---|
| attempt | one run of the handler; the first run is attempt 1 | `JobEnvelope.Attempt` |
| total attempts | how many attempts the engine will make at most: retry limit + 1 | new `JobEnvelope.TotalAttempts`, `JobProgressPushNotification.TotalAttempts` |
| retry | an attempt after a failure | — |
| retry limit | how many retries are allowed: the job's `MaxRetryAttempts`, else the engine default | `JobEnvelope.MaxRetryAttempts` |
| final attempt | `Attempt == TotalAttempts`, or `TotalAttempts` unknown | derived in the dispatcher |
| outcome | how the job ended: `Succeeded`, `Failed`, `Canceled` | new `JobOutcome` (module Core) |
| close the notification | set `Finished` and `Outcome`, so the UI stops showing it as running | completion seam §3.4 |

### 3.1 Enqueue payload binding

- `EnqueueJobRequest.Payload` becomes a Newtonsoft `JToken` with a property-level `[JsonConverter]` owned by the module
  that reads with `DateParseHandling.None` and `FloatParseHandling.Decimal`. Analog:
  `vc-platform/src/VirtoCommerce.Platform.Web/Model/SettingValues.cs:13-47` (same problem, same fix). The token goes
  to the registered `IJobPayloadSerializer` as raw text (`ToString(Formatting.None)`), so the worker and the controller
  deserialize with the same settings. It does not depend on the platform's global converter.
- `BackgroundJobsAdminController.Enqueue` checks `ModelState` **before** the name check and answers a payload that does
  not bind with a 400 that names `payload`, instead of today's "A registered job 'name' is required.".
- An absent or `null` payload still means "default instance of the payload type", as today.

### 3.2 REST enqueue: ownership of the notification

- A request whose `options.progressNotificationId` is set is refused with a 400 that names the field. A REST caller
  cannot own a notification created elsewhere; in-process callers (`IBackgroundJob`, map/reduce) keep the option.

### 3.3 Run page and progress page (Admin Shell plugin)

New `src/VirtoCommerce.BackgroundJobs.Web/vue/` Module Federation plugin for the Admin Shell. Skeleton mirrors any
module's `vue/` (`vite.config.mts` with `getDynamicModuleConfiguration`, `public/plugin.json`, csproj
`BuildAdminShellPlugin` target guarded by `SkipAdminShellPlugin`, generated API client); the job-following part mirrors
vc-module-search (`pages/index-progress.vue`, `composables/useJob.ts`, `components/JobProgress.vue`), the only module
whose plugin follows a background job.

- **Menu item** "Background jobs", permission `background-jobs:access` (the module convention: menu on `:access`, data
  on `:read`; 7 of 8 module plugins and 6 of 7 modules' permission sets follow it).
- **Jobs page** lists `GET api/background-jobs/registered` (`background-jobs:read`): name, payload type, triggerable.
- **Run** (shown for triggerable jobs, enabled with `background-jobs:execute`) opens a small form with an optional raw
  JSON payload (empty means default payload) and enqueues with `options: { reportProgress: true, title: <job name> }`.
  The server default for a request without options is unchanged.
- **Progress page** opens with the job id from the enqueue response and follows the `JobProgressPushNotification` whose
  `JobId` matches: title, bar from processed/total, "attempt N of M", the message log, elapsed time, outcome and error.
  A Cancel command is shown only with the platform permission `background_jobs:manage` (it guards
  `POST api/platform/jobs/{id}/cancel` today) and only while the job has not finished: cancelling a finished Hangfire
  job answers 200 and turns its state from `Succeeded` into `Deleted` (observed, §5), which rewrites the job's history.
- **Notification template** for `JobProgressPushNotification` registered through `defineAppModule({ notifications })`
  with toast mode `progress`; the toast completes with the outcome (`Failed` → error toast).

### 3.4 Completion seam and the notification builder

- New interface in `BackgroundJobs.Core`, implemented by `PushNotificationJobProgress` and by every progress wrapper:
  `Task Complete(JobOutcome outcome, string? error)`. `DefaultJobDispatcher.TryCompleteProgress` tests the interface,
  not the concrete type (today `context.Progress is PushNotificationJobProgress`).
- One builder produces every `JobProgressPushNotification` (today built in `PushNotificationJobProgress.Report`/
  `Complete`, `MapReduceJob.cs:131-142`, `ReduceCoordinator.cs:120-127`):
  - `Started` and `Created` come from the envelope's `JobHeaders.EnqueuedAt` and stay the same on every update;
    `IsNew` is not raised again by updates.
  - `ProcessedCount`/`TotalCount` keep their last known values when a `Report` omits them.
  - `ProgressLog` accumulates messages, capped (the whole log travels over SignalR on every update). Only a job that
    owns its notification (`CompletesProgressNotification`) accumulates; map tasks reporting into a shared
    notification do not.
  - `Complete` keeps the last progress message as the description and records the outcome in `Outcome`, `Finished`
    and a final log entry.
  - New fields: `JobId`, `Outcome`, `Attempt`, `TotalAttempts`, `Error`.
- `JobId`: the facade creates and sends the notification before the engine returns the job id, so it re-sends the
  notification with `JobId` right after `IJobEngine.Enqueue` returns; every worker-side update carries `context.JobId`.

### 3.5 Attempts and outcome on every engine

- **The engine fills `Attempt` and `TotalAttempts` on the envelope before `IJobDispatcher.Dispatch`.** `Dispatch`'s
  signature does not change, so custom engines keep compiling.
  - RabbitMQ (`RabbitMqJobConsumer`) and InMemory (`InMemoryJobEngine.RunAsync`): `TotalAttempts =
    Math.Max(0, envelope.MaxRetryAttempts ?? global) + 1`, the formula both already use to decide a retry.
  - Hangfire (`HangfireJobExecutor`): `Attempt = RetryCount + 1` from the job parameter; `TotalAttempts` = the retry
    limit that §3.6 applies to this job + 1.
  - An engine that does not set `TotalAttempts` leaves it null: every attempt is treated as final (today's behaviour,
    so a notification never stays open forever).
- **The dispatcher closes or keeps the notification:**
  - handler returned → close `Succeeded`;
  - exception on the final attempt → close `Failed` with the error;
  - exception with a retry pending → keep open, message "attempt N of M failed, retrying" with the error;
  - `OperationCanceledException` → keep open; the dispatcher cannot tell a requested cancel from a host shutdown or a
    lost distributed lock, which the engine re-runs.
- **The engine closes the notification `Canceled` when the cancel was requested**, through the completion seam
  (§3.4), because only the engine knows who tripped the token:
  - Hangfire (`HangfireJobExecutor`): the run ended because the job was deleted or its state changed (Hangfire's
    `JobAbortedException`), not because the server is shutting down;
  - RabbitMQ (`RabbitMqJobConsumer`): the job's token was tripped by the cancellation store (`RabbitMqJobEngine.Delete`
    is the only writer of `IJobCancellationStore`);
  - InMemory: the job's own `CancellationTokenSource` was canceled by `Delete`;
  - an engine that does nothing keeps today's behaviour (the notification stays open).
- Failures before the handler runs (payload deserialization, type or handler resolution) follow the same rules: the
  `try` in `Dispatch` starts before them, and they reach telemetry too.
- InMemory `GetStatus`: `Failed` only after the final attempt; `Cancelled` is terminal and reported completed.

### 3.6 Per-job retry limit on Hangfire

- `PerJobRetryFilterAttribute` stops being "tighten only": for an envelope job whose `MaxRetryAttempts` is set, the
  filter makes the retry decision itself by applying a Hangfire `AutomaticRetryAttribute` configured with the job's
  limit to the election (raising and lowering alike), so the delay formula and the `RetryCount` bookkeeping stay
  Hangfire's. Jobs without a per-job value and non-envelope jobs keep the global filter's decision.
- The `RetryCount` parameter name is already asserted against the shipped Hangfire assembly
  (`PerJobRetryFilterTests`); the new path keeps that guard.

### 3.7 Hangfire console mirror

- `HangfireJobExecutor` wraps the context's progress (the push-notification one and `NoOpJobProgress` alike) in a
  Hangfire-project wrapper that also writes to the job's console: a line when the message changes, a progress bar
  updated when the integer percentage changes (`TotalCount > 0`, clamped to 0..100). The wrapper forwards `Complete`
  (§3.4) and writes a final line with the outcome.
- Console errors are caught and logged; they never fail the job. Without `UseConsole()` (Memory storage) the
  Hangfire.Console calls are silent no-ops (`ConsoleContext.FromPerformContext` returns null, v1.4.3).

### 3.8 Documentation in this repo

- README "Retries": `EnqueueOptions.MaxRetryAttempts` is honored and may raise or lower the count on every engine
  (today's text says "not yet honored").
- README and `JobEnvelope.UniqueKey` doc: no shipped engine (Hangfire, RabbitMQ, InMemory) de-duplicates by
  `UniqueKey`; correct `docs/benchmark-plan.md:100,109`.

### 3.9 Companion PR: Admin Shell broadcast filter (`vc-module-admin-shell`)

- The host `App.vue` installs `setBroadcastFilter(x => x.creator === <current user name>)`, matching the AngularJS
  per-user behaviour and the vendor-portal and import apps. The platform keeps broadcasting (the scale-out synchronizer
  and vendor-portal need it).

## 4. Approaches considered

| Decision | Chosen | Rejected and why |
|---|---|---|
| Admin UI | Admin Shell Vue plugin only | AngularJS blade: legacy, no new functionality goes there |
| After Run | progress page linked by `JobId` on the notification | notification only: details hidden in the dropdown; returning the notification id from `enqueue`: changes the response for integrators |
| Payload binding | `JToken` + module-owned converter | raise the floor to 3.1100: depends on an unrelated converter and still loses decimals; read the raw body: lose model binding and the Swagger schema |
| Completion | interface the dispatcher tests | dispatcher unwraps a wrapper: ties it to wrapper types |
| Final attempt | engine supplies attempt and total, dispatcher derives "final" | engine supplies a final flag: cannot show "attempt N of M"; Hangfire state filter on `Failed`: Hangfire-only and loses counts and the last message |
| Hangfire raise | filter applies `AutomaticRetryAttribute` with the job's limit | re-implement scheduling and delays: duplicates Hangfire |
| User cancel vs shutdown | each engine classifies its own cancel and closes `Canceled` | any `OperationCanceledException` in the dispatcher: a lost lock or host shutdown would close a job the engine re-runs; `IJobCancellationStore.IsCancelRequested` in the dispatcher: only RabbitMQ writes that store |
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
| same, other failure codes | — | 404 unknown/non-triggerable job, 503 no engine, 500 handler load failure | read-from-code (`BackgroundJobsAdminController.cs:54-112`) |
| Stored notification, `POST api/platform/pushnotifications` `{ take, skip }` | — | `{ totalCount, newCount, notifyEvents[] }`; the probe's item: `notifyType: "JobProgressPushNotification"`, `id`, `creator`, `title`, `description: "Completed"`, `started: null`, `finished`, `created` (= completion time), `isNew: true`, `processedCount: 0`, `totalCount: 0`, `progressLog: []`, `repeatCount: 0`, `serverId: null` | observed (MVC serialization of the stored copy) |
| SignalR `Send` (creator) / `SendSystemEvents` (all) | — | same members; the hub protocol's serializer settings differ from MVC (camelCase, `$type` only in scale-out mode) | read-from-code |
| `GET api/platform/jobs/{id}` | (`background_jobs:manage`) | 200 `{ state: "Succeeded", completed: true, id }`; unknown id → 200 `{ state: null, completed: true, id }` (never 404) | observed |
| `POST api/platform/jobs/{id}/cancel` | (`background_jobs:manage`) | on a **finished** Hangfire job: 200, and the job's state becomes `Deleted` (`GET` then answers `state: "Deleted"`); 404 when the engine rejects, 501 when the engine cannot cancel | observed (finished job); read-from-code (404/501, `JobsController.cs:57-68`) |
| Admin Shell plugin discovery | `{module}/plugins/admin-shell/remoteEntry.js` + `plugin.json`; served through the host app's manifest (`GET /api/apps/<admin-shell app id>/manifest`), not `/api/apps/platform/manifest` | menu contribution with `permission` (forwarded opaque by the platform) | read-from-code; Admin Shell not on this stand |
| Admin Shell host framework | — | `@vc-shell/framework` 2.12.0, shared as MF singleton | read-from-code (host `package.json` on GitHub) |

## 6. Acceptance criteria

Each criterion names its check: `test` only where the failure is silent and harmful (§7), otherwise `E2E`.

1. WHEN a job is enqueued through `POST api/background-jobs/enqueue` with an object payload on platform 3.1097–3.1099
   or 3.1100+, THEN the handler SHALL receive that payload with every field, date string and decimal unchanged. — test
2. WHEN the payload in an enqueue request does not bind, THEN the endpoint SHALL answer 400 naming `payload` and SHALL
   NOT enqueue a job. — test
3. WHEN an enqueue request carries `options.progressNotificationId`, THEN the endpoint SHALL answer 400 naming that
   field and SHALL NOT enqueue a job. — test
4. WHERE the user has `background-jobs:access`, THE Admin Shell SHALL show a "Background jobs" menu item. — E2E
5. WHEN the user opens the Background jobs page, THEN it SHALL list every registered job with its triggerable flag.
   — E2E
6. WHERE a job is triggerable and the user has `background-jobs:execute`, THE page SHALL offer Run with an optional
   JSON payload; Run SHALL enqueue with progress reporting and the job name as title, and open the job's progress
   page. — E2E
7. WHILE a job runs, THE progress page SHALL show its title, processed and total counts, "attempt N of M", the message
   log and elapsed time, updated as the job reports. — E2E
8. WHEN a job ends, THEN its notification SHALL be closed with outcome `Succeeded`, `Failed` with the error, or
   `Canceled`, and the progress page and toast SHALL show that outcome. — test (dispatcher, all engines); E2E (UI)
9. WHEN an attempt fails and a retry will follow, THEN the notification SHALL stay open with "attempt N of M failed,
   retrying" and SHALL NOT be closed as failed, on every engine. — test
10. WHEN a job is canceled by the user, THEN its notification SHALL be closed as `Canceled`; WHEN an
    `OperationCanceledException` arises without a cancel request, THEN the notification SHALL stay open. — test
11. WHEN the dispatcher fails before the handler runs (payload, type or handler resolution), THEN the failure SHALL be
    handled like a handler failure: closed `Failed` on the final attempt, open while a retry is pending. — test
12. THE notification SHALL keep `Started`, `Created` and `JobId` stable across updates, keep the last known counts when
    a report omits them, accumulate the message log (capped) for a job that owns its notification, and keep the last
    message on completion. — test
13. WHEN a job reports into a shared notification (`CompletesProgressNotification` false), THEN its updates SHALL NOT
    close that notification or replace its log. — test
14. WHEN a job enqueued with `MaxRetryAttempts = N` fails every time, THEN every engine SHALL run it N + 1 times,
    including when N exceeds the global default on Hangfire. — test
15. WHEN a Hangfire job runs attempt K, THEN the envelope the dispatcher receives SHALL carry `Attempt = K` and the
    total the engine will make. — test
16. WHILE a Hangfire job reports progress, THE dashboard console SHALL show its messages and a progress bar; WHEN
    writing to the console fails, THEN the job SHALL continue. — E2E
17. WHEN a job's progress is wrapped for the console, THEN its notification SHALL still be closed on completion.
    — test
18. WHILE an InMemory job waits for a retry, THE engine SHALL NOT report it completed; WHEN it is canceled, THEN the
    engine SHALL report it completed. — test
19. WHERE the Admin Shell host has the broadcast filter (companion PR), THE user SHALL NOT see notifications created
    by another user. — test (in `vc-module-admin-shell`); E2E
20. THE README SHALL state that the per-job retry limit is honored on every engine and that no shipped engine
    de-duplicates by `UniqueKey`. — E2E (review)

## 7. Testing rule

Test only a branch whose failure is silent and harmful: data (a duplicate, a lost or wrong value, an item left in a
wrong state), security or privacy (another role, another person's data, a token leak), or a lost message the person
needs. Loud or harmless failures get no test; E2E covers them. A surviving mutant alone is not a reason.

## 8. Test list

The silent-and-harmful branches this design creates or fixes. Cross-engine items go into the conformance kit
(`tests/VirtoCommerce.BackgroundJobs.Conformance`), so Hangfire, InMemory, Reference (and RabbitMQ when enabled) run
the same assertion.

1. Payload through the MVC pipeline (TestHost, Newtonsoft configured as the platform does), with and without the
   3.1100 global `JsonElementJsonConverter`: an object payload reaches the stored envelope unchanged, dates and decimals
   included. Data, wrong value. (AC 1)
2. Payload that does not bind → 400 naming `payload`, no enqueue. Data, job runs with a default payload. (AC 2)
3. `options.progressNotificationId` on REST → 400, no enqueue. Security, another person's notification. (AC 3)
4. Conformance: failing attempt with a retry pending leaves the notification open with attempt N of M; the final
   failure closes it `Failed` with the error; success closes `Succeeded`. Lost message. (AC 8, 9)
5. Conformance: user cancel closes `Canceled`; an `OperationCanceledException` without a cancel request leaves it
   open. Wrong state. (AC 10)
6. Dispatcher: payload that does not deserialize, on final and non-final attempts. Item left in a wrong state. (AC 11)
7. Notification builder: stable `Started`/`Created`/`JobId`, counts kept, log accumulated and capped, last message
   kept on completion. Lost or wrong value. (AC 12)
8. Shared notification: a map task's update neither closes it nor replaces its log. Wrong state. (AC 13)
9. Conformance: `MaxRetryAttempts = N` above the global default gives N + 1 runs on every engine. Data, a job given up
   early. (AC 14)
10. Hangfire executor: `Attempt` and `TotalAttempts` from `RetryCount` and the applied limit. Wrong value. (AC 15)
11. Console wrapper forwards completion. Lost message. (AC 17)
12. InMemory `GetStatus` between retries and after cancel. Wrong state. (AC 18)
13. `vc-module-admin-shell`: the host filter drops a broadcast from another creator and keeps one from the current
    user. Privacy. (AC 19)

## 9. Risks and open points

- **Cancel on a finished job rewrites history** (observed on Hangfire): `POST api/platform/jobs/{id}/cancel` on a
  Succeeded job answers 200 and the job becomes `Deleted`. Out of scope to change in `JobsController`; the progress
  page hides Cancel once the job has finished.

- **A live surface appears.** Once payload binding works on every platform, `api/background-jobs/enqueue` with
  `background-jobs:execute` reaches every triggerable handler with a caller-built payload. Today the bind defect is
  the only thing keeping vc-module-search's `DeleteDocumentsJobHandler` inert through this route. Out of scope here;
  to be raised with the search module.
- **Every recurring handler is triggerable** (`AddRecurringJob` registers with `triggerable: true`), including the
  platform's token and sign-in-log cleanups and auto lockout. Run will list and start them. Recorded, not changed.
- **Hangfire cancel classification** rests on Hangfire raising `JobAbortedException` for a deleted job and a plain
  cancellation for shutdown (`CoreBackgroundJobPerformer`, v1.8.23), read from source by the analysis, not run.
- **Hangfire raise** depends on applying `AutomaticRetryAttribute` inside another filter's election; proven only by
  test 9 on the Hangfire conformance fixture (MemoryStorage, instant retries).
- **Notification initial state on the progress page**: the page opens after `enqueue` returns; it must find the
  notification already received for that `JobId` in the vc-shell store, not only future ones. E2E (AC 7).
- **Scale-out** (read-from-code, not run): with `PushNotifications:ScalabilityMode` set, the hub protocol serializes
  with `TypeNameHandling.Objects` ("store full type information in JSON to deserialize push notifications on other
  instances", `SignalRBuilderExtensions.cs:29-34`), and `PushNotificationSynchronizerTask` reads with the same setting
  (`:35-39`) before saving into the local store (`:74-82`). So the derived `JobProgressPushNotification`, with
  `JobId`, `Outcome` and attempts, is what other instances store and serve. The same path already carries other
  modules' derived notifications. Not reproduced on a multi-instance stand; the single-instance 3.1097 stand cannot
  observe it.

## 10. Change log

- 2026-11-20 — Scope extended (operator): the Hangfire part of VCST-6850 item 3 — a per-job `MaxRetryAttempts` may
  raise the retry count on Hangfire — and correct attempt numbering on Hangfire. VCST-6850 keeps backoff, retryable
  exceptions, the attempt number on `IJobExecutionContext`, and its docs.
