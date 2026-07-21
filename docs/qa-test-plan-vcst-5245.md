# QA Test Plan — VCST-5245 Background Jobs (by release cycle)

## 1. Context & release strategy

The module introduces an **engine-agnostic** background-job abstraction (`IBackgroundJob` / `IBackgroundJobHandler<T>` in `Platform.Core`), a shared dispatcher, map/reduce, and recurring jobs. The engine is chosen by configuration (`VirtoCommerce:BackgroundJobs:Provider`). **Hangfire stays the default engine**; RabbitMQ is the new engine; there is also In-Memory (dev/test) and a sample Google Cloud Tasks engine.

The rollout happens in **three stages** — this plan is organized around them:

| Stage | Jira | Summary | QA owner | RabbitMQ role |
|---|---|---|---|---|
| **1** | **VCST-5245** | Release on **Hangfire**. Priority is "works exactly as before" (regression, no breaking changes). | **Elena Kutasina** | **Preview** (announce + smoke only) |
| **2** | [VCST-5490](https://virtocommerce.atlassian.net/browse/VCST-5490) | Remove direct Hangfire usage in VC modules (move to the abstraction, search → Map/Reduce, recurring → `AddRecurringJob`). | — | unchanged (still Hangfire) |
| **3** | [VCST-5493](https://virtocommerce.atlassian.net/browse/VCST-5493) | Full **RabbitMQ** verification and promotion out of Preview. | **Dimitri Kargapolov** | primary engine |

**Key principle for Stage 1:** upgrading an existing deployment **requires no `appsettings.json` change** and **breaks nothing** — Hangfire remains the default engine and `EnableLegacyHangfire` defaults to `true`.

## 2. Configuration / environment matrix

| Scenario | `…BackgroundJobs:Provider` | `Mode` | `EnableLegacyHangfire` | Infrastructure |
|---|---|---|---|---|
| Hangfire (default, **release**) | `Hangfire` or empty | `Both` | `true` | SQL |
| RabbitMQ + legacy Hangfire | `RabbitMQ` | `Both` | `true` | RabbitMQ + Redis + SQL (Hangfire) |
| Pure RabbitMQ | `RabbitMQ` | `Both` | `false` | RabbitMQ + Redis |
| In-Memory (dev/CI) | `InMemory` | — | `false` | none |

Case priorities: **P1** — release blocker, **P2** — important, **P3** — nice-to-have.

---

## 3. Stage 1 — VCST-5245 (release focus) · owner **Elena Kutasina**

**Goal:** confirm that on **Hangfire** the platform and all existing scenarios work **as they did before the module** (regression), that the new agnostic API and admin API work on Hangfire, and that the upgrade is seamless. RabbitMQ is preview-smoke only and **does not block the release**.

**Preconditions:** production-like stand (SQL), default configuration (Provider unset or `Hangfire`, `Mode=Both`). Administrator role for the UI/dashboard.

### 1.A — Hangfire regression (no breaking changes) — P1

| ID | Check | Expected result |
|---|---|---|
| TC-1.A-01 | Start the platform with a **default** `appsettings.json` (Provider unset). | Starts with no errors; active engine is Hangfire; `/hangfire` is available. |
| TC-1.A-02 | Platform recurring jobs (`PruneExpiredTokens`, `AutoAccountLockout`) on schedule. | Fire on schedule; visible in `/hangfire` → Recurring Jobs. |
| TC-1.A-03 | Catalog indexing (full and incremental). | Runs as a background job, progress in the admin UI, completes successfully. |
| TC-1.A-04 | Bulk operations (export/import, mass edits). | Run in the background, progress and result correct. |
| TC-1.A-05 | Module install / update / uninstall. | Runs as a background job, progress in UI, status correct. |
| TC-1.A-06 | Progress of a long-running job in the admin UI. | Progress bar with message and count, plus a final state. |
| TC-1.A-07 | `GET api/platform/jobs/{id}`. | Returns status; for completed/unknown → `Completed`, and the UI poller stops. |
| TC-1.A-08 | Job failure → Hangfire retries (`Hangfire:AutomaticRetryCount`). | Retries as before, then Failed in the dashboard. |
| TC-1.A-09 | Restart the platform with in-flight jobs. | Jobs are picked up from Hangfire SQL and resume. |

### 1.B — Legacy: modules using Hangfire directly — P1

| ID | Check | Expected result |
|---|---|---|
| TC-1.B-01 | A module calls `Hangfire.BackgroundJob.Enqueue` / `RecurringJob.AddOrUpdate` / `IBackgroundJobClient` / `IRecurringJobManager`. | Jobs run (with `EnableLegacyHangfire=true`, the default). |
| TC-1.B-02 | A module built against the **old** `VirtoCommerce.Platform.Hangfire.dll` is installed without recompiling. | Loads and works (type-forwarding shim). |
| TC-1.B-03 | Legacy `IRecurringJobService` (expression-based). | Recurring registration works on Hangfire. |

### 1.C — New engine-agnostic API on Hangfire — P1/P2

| ID | Check | Expected result |
|---|---|---|
| TC-1.C-01 | `IBackgroundJob.Enqueue<THandler>(payload)` (fire-and-forget). | Job runs on Hangfire. |
| TC-1.C-02 | Enqueue with `EnqueueOptions{ReportProgress=true}`. | Progress in the admin UI. |
| TC-1.C-03 | `AddRecurringJob` (fixed cron and `FromSettings`); the **Cron** setting type in the UI. | Job runs on schedule; the UI shows a cron editor with presets and a live description, plus validation on save. |
| TC-1.C-04 | Map/Reduce (`IMapReduceJob.Enqueue<TMap,TReduce>`), e.g. `POST api/background-jobs-sample/index`. | Fan-out over items, reduce fires **exactly once**, result correct. |
| TC-1.C-05 | Static `BackgroundJob.Enqueue<THandler>(payload)` (without injecting `IBackgroundJob`). | Job is enqueued/executed. |
| TC-1.C-06 | A partner module overrides a handler (registered later). | Last registration wins. |

### 1.D — New admin REST endpoints & permissions — P2

| ID | Check | Expected result |
|---|---|---|
| TC-1.D-01 | `GET api/background-jobs/registered` with the `background-jobs:read` permission. | List of registered handlers; internal ones (map/reduce coordinators, module management) have `triggerable=false`. |
| TC-1.D-02 | `GET api/background-jobs/recurring`. | List of recurring jobs: cron, enabled, timezone, handler/payload, last/next run. |
| TC-1.D-03 | `POST api/background-jobs/enqueue {name,payload}` with the `background-jobs:execute` permission. | Job is triggered by name; returns a job id. |
| TC-1.D-04 | `POST enqueue` for an **internal** handler (e.g. `MapCoordinator`). | **400** — internal handlers are not triggerable. |
| TC-1.D-05 | `POST enqueue` with an unknown name. | **404**. |
| TC-1.D-06 | Access the endpoints without the required permission. | 401/403; `read` does not grant `execute`, and vice versa. |
| TC-1.D-07 | `background-jobs:read` / `:execute` permissions in the roles UI. | Visible under the **BackgroundJobs** group, assignable to a role. |

### 1.E — Upgrade of an existing deployment — P1

| ID | Check | Expected result |
|---|---|---|
| TC-1.E-01 | Upgrade a production-like stand (Hangfire + SQL) with no `appsettings.json` edits. | Migration succeeds, everything works as before, no regressions. |
| TC-1.E-02 | `EnableLegacyHangfire` not set. | Defaults to `true` — Hangfire initialized, prior behavior preserved. |

### 1.F — RabbitMQ Preview (smoke, opt-in) — P3 (non-blocking)

| ID | Check | Expected result |
|---|---|---|
| TC-1.F-01 | `Provider=RabbitMQ` (+ RabbitMQ + Redis), a simple job end-to-end. | Starts and executes the job. Result is **informational**, does not block the release. |
| TC-1.F-02 | Documentation/announcement. | RabbitMQ is clearly marked **Preview / not for production**; enablement instructions exist. |

### ✅ Stage 1 exit criteria
- All **P1** (1.A, 1.B, 1.C-01…03, 1.E) — **PASS**.
- Admin API (1.D) — PASS (P2).
- Upgrade is seamless, `appsettings.json` unchanged.
- RabbitMQ preview smoke — informational (defects are logged but do not block the release).

---

## 4. Stage 2 — VCST-5490 · remove direct Hangfire usage from VC modules

**Goal:** VC modules are moved from direct Hangfire calls to the agnostic abstraction; indexing/search → **Map/Reduce**; recurring → `AddRecurringJob`. Functionality **must not regress** (still on Hangfire).

| ID | Check | Expected result |
|---|---|---|
| TC-2-01 | Indexing moved to Map/Reduce. | Works on Hangfire; result identical to the previous implementation. |
| TC-2-02 | VC-module recurring jobs moved to `AddRecurringJob`. | Fire on schedule; configurable ones via the Cron setting. |
| TC-2-03 | No direct Hangfire API calls remain in VC modules. | Codebase check; no functional regressions. |
| TC-2-04 | Custom/partner modules that still use Hangfire directly. | Still work (`EnableLegacyHangfire=true`) — coexistence preserved. |
| TC-2-05 | Full regression of migrated modules (catalog, search, export, etc.). | No functional regressions. |

### ✅ Stage 2 exit criteria
Migration with no functional regressions on Hangfire; legacy coexistence for custom modules preserved.

---

## 5. Stage 3 — VCST-5493 · full RabbitMQ verification · owner **Dimitri Kargapolov**

**Goal:** RabbitMQ as the primary engine — end-to-end, multi-instance fleet, resilience; promote out of Preview.

**Preconditions:** RabbitMQ broker + Redis (required for map/reduce state and the recurring occurrence marker in a cluster); for scale tests — K8s + KEDA. **Do not run against the same broker used for tests** (see Risks).

| ID | Check | Expected result |
|---|---|---|
| TC-3-01 | `Provider=RabbitMQ`, `Mode=Both` — end-to-end. | enqueue → consume → execute; status/progress correct. |
| TC-3-02 | **Parallelism** (`ConsumerDispatchConcurrency`, default `0`=auto). Run a map/reduce `…/index`. | Multiple map handlers run **concurrently** (log `N map handler(s) running in parallel`), **not one at a time** (regression check for the fixed serial-dispatch bug). |
| TC-3-03 | Retry + delayed retry of a failing job. | Re-published with a delay (`RetryDelaySeconds`, default 5s) up to `MaxRetryAttempts`. |
| TC-3-04 | Dead-letter after retries are exhausted. | Message goes to `{queue}.dlq` with headers `x-original-queue` / `x-attempts` / `x-death-reason`. |
| TC-3-05 | Map/Reduce on RabbitMQ+Redis; **worker crash** mid fan-out. | Fan-out, checkpoint/resume — the worker **resumes, does not duplicate**; reduce fires exactly once. |
| TC-3-06 | Recurring on RabbitMQ (in-process scheduler + Redis occurrence marker) across **multiple instances**. | Each occurrence fires **exactly once** across the fleet. |
| TC-3-07 | `Producer` / `Worker` / `Both` modes. | Producer only enqueues (no consumer), Worker processes; they scale independently. |
| TC-3-08 | RabbitMQ primary + `EnableLegacyHangfire=true`. | New jobs via RabbitMQ, legacy via Hangfire; `/hangfire` available; stores are disjoint. |
| TC-3-09 | Pure RabbitMQ (`EnableLegacyHangfire=false`). | No Hangfire server, no `/hangfire`, no Hangfire SQL schema. |
| TC-3-10 | **KEDA**: queue depth grows. | Worker pod scales `0→N`; scales back to zero when idle. |
| TC-3-11 | Broker connection drop. | Consumer reconnects; unacked jobs are not lost (redelivered). |
| TC-3-12 | `GET api/platform/jobs/{id}` on RabbitMQ (no ledger). | Unknown id → treated as completed (poller doesn't hang); delete unsupported. |
| TC-3-13 | A foreign/invalid message in the queue (e.g. a payload from another stand). | Platform doesn't crash: the message is retried and dead-lettered; an error is logged but the service stays up. |
| TC-3-14 | RabbitMQ vs Hangfire performance (see `docs/benchmark-plan.md`, `docs/benchmark-kql.md`). | RabbitMQ ≥ target throughput, stable p95, ~1% CPU under load. |

### ✅ Stage 3 exit criteria
All P1 RabbitMQ cases on a **multi-instance** stand — PASS; parallelism, retry/DLQ, map/reduce resume, recurring exactly-once, legacy coexistence — PASS; performance confirmed → **promote out of Preview**.

---

## 6. Quick regression checklist (per engine)

- [ ] Platform starts; the active engine in the logs matches `Provider`.
- [ ] `/health` → Background jobs: Healthy (Degraded only for a queue engine on an in-memory store).
- [ ] Indexing / bulk / module install run, with progress in the UI.
- [ ] Platform recurring jobs fire.
- [ ] `GET api/platform/jobs/{id}` does not "hang".
- [ ] Admin API: registered / recurring / enqueue + permissions.
- [ ] No unexpected ERRORs in the log at startup and under load.

## 7. Risks & notes

- **Leftover messages in a durable RabbitMQ queue.** Do not run the platform against the same broker used for the RabbitMQ/conformance tests: their messages (`ConformancePayload`, etc.) survive a restart and land in the DLQ with a "Cannot resolve job payload type" error. Before a run, purge `default`, `default.dlq`, `default.retry.*` (or use a dedicated vhost).
- **Redis is required for RabbitMQ** in a fleet: map/reduce state and the recurring occurrence marker. Without Redis — single-instance only (the health check reports Degraded).
- **Concurrency defaults to auto** (`PrefetchCount=0`, `ConsumerDispatchConcurrency=0` → `ProcessorCount × ConcurrencyPerCore`). For CPU-bound jobs set `ConcurrencyPerCore` to ~1–2.
- **Upgrading a RabbitMQ-only stand:** with `EnableLegacyHangfire` defaulting to `true`, the instance will start bootstrapping Hangfire (server/schema/`/hangfire`) — set it to `false` if that's not wanted.

## 8. References
- Module README — configuration, permissions, Telemetry (Application Insights), admin API.
- `docs/benchmark-plan.md`, `docs/benchmark-kql.md`, `docs/benchmark-results-template.md` — methodology and KQL for performance tests.
- Overview deck: `docs/background-processing-overview-for-partners.html`.
