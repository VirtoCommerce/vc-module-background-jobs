# Local testing runbook — background-job cancellation (VCST-5490)

Tests `IBackgroundJob.Cancel` / `SupportsCancellation` end-to-end via image-tools' thumbnail cancel, across all three
engines. All code is built; this covers deploy → configure → run → exercise.

## 0. Build state (already done)

- **Platform.Core** — `IBackgroundJob.Cancel` + `SupportsCancellation` and static `BackgroundJob.Cancel` +
  `SupportsCancellation`. Packed to `..\local-nuget` as **3.1058.0-alpha.cancel**.
- **vc-module-background-jobs** — built, unit + conformance tests green (3 pre-existing RabbitMQ broker-timing flakes
  only). Pinned to platform 3.1058.0-alpha.cancel. Output: `src/*/bin/Debug/net10.0`.
- **vc-module-image-tools** — built against the local platform; `ThumbnailsTasksController` now calls
  `BackgroundJob.Cancel(jobId, ct)`; handler already honors `CancellationToken`. `nuget.config` added → local feed.

> **The `Cancel` API lives in `Platform.Core.dll`**, so the running platform must include the rebuilt platform, not
> just the modules. If you run from `artifacts/publish`, **republish the platform** first; if you `dotnet run` from
> `src/VirtoCommerce.Platform.Web`, it recompiles automatically.

## 1. Deploy the rebuilt bits (your usual module-drop)

Deploy the **BackgroundJobs** and **image-tools** modules into the instance's `modules` folder (DiscoveryPath is
`./modules`, i.e. `artifacts/publish/modules/<ModuleId>/` when running from publish). Each module folder needs its
`module.manifest` at the root and the rebuilt DLLs under `bin/`. Use whatever local module-deploy you normally use
(symlink / copy / `vc-build CompressModule`), pointing at:

- `vc-module-background-jobs/src/VirtoCommerce.BackgroundJobs.Web` (manifest **3.1051.0**)
- `vc-module-image-tools/src/VirtoCommerce.ImageToolsModule.Web`

Both engines' assemblies (`VirtoCommerce.BackgroundJobs.Hangfire/RabbitMQ/InMemory.dll`) ship in the BackgroundJobs
module's `bin` — the active one is chosen at runtime by config, below.

## 2. Configure the engine (edit `appsettings.json` → `VirtoCommerce:BackgroundJobs`)

**Hangfire (native cancel)** — SQL only, current default:
```jsonc
"BackgroundJobs": { "Provider": "Hangfire" }
```

**InMemory (infra-free)** — no SQL/broker/Redis:
```jsonc
"BackgroundJobs": { "Provider": "InMemory", "EnableLegacyHangfire": false }
```

**RabbitMQ (cooperative cancel)** — needs a broker **and Redis** (fleet-wide cancel store):
```jsonc
"BackgroundJobs": { "Provider": "RabbitMQ", "EnableLegacyHangfire": false }
```
```jsonc
// top-level Connection strings / Redis:
"RedisConnectionString": "127.0.0.1:6379,ssl=False",
"VirtoCommerce": { "RabbitMQ": { "Uri": "amqp://guest:guest@localhost:5672/" } }
```
> Without Redis, RabbitMQ falls back to the **in-memory** cancellation store — cancel still works on a single instance
> but is not fleet-wide.

## 3. Run

From source:
```bash
dotnet run --project src/VirtoCommerce.Platform.Web -c Debug
```
Then open `https://localhost:5001`.

## 4. Get a bearer token (you run this — I won't POST your password)

```bash
curl -sk -X POST https://localhost:5001/connect/token -H "Content-Type: application/x-www-form-urlencoded" -d "grant_type=password&username=admin&password=<YOUR_PASSWORD>&client_id=default&scope=offline_access"
```
Copy `access_token`; export it:
```bash
export TOK="<access_token>"
```

## 5. Exercise cancellation (repeat per engine)

Make the run long enough to still be in-flight when you cancel — a task with `regenerate:true` over a large asset set.

**a. Start a thumbnail run** (returns a notification containing `jobId`):
```bash
curl -sk -X POST https://localhost:5001/api/image/thumbnails/tasks/run -H "Authorization: Bearer $TOK" -H "Content-Type: application/json" -d '{"taskIds":["<THUMBNAIL_TASK_ID>"],"regenerate":true}'
```

**b. Cancel it while running** (`jobId` from step a):
```bash
curl -sk -i -X POST "https://localhost:5001/api/image/thumbnails/tasks/<JOB_ID>/cancel" -H "Authorization: Bearer $TOK"
```

**c. (Optional) generic platform endpoint** — same effect, engine-agnostic:
```bash
curl -sk -i -X POST "https://localhost:5001/api/platform/jobs/<JOB_ID>/cancel" -H "Authorization: Bearer $TOK"
```

## 6. Expected results

| Engine | Cancel response | What happens | Log / notification signal |
|---|---|---|---|
| **Hangfire** | `200` | job removed (not started) or transitioned to *Deleted* (running); handler token trips | thumbnail notification stops progressing, not "completed successfully" |
| **InMemory** | `200` | running task's token trips | same |
| **RabbitMQ** | `200` | not-started → discarded on dequeue; running → token trips within ~3s (poll interval) | consumer log: `Cancellation requested for running job {JobId}; signaling the handler.` and/or `was cancelled before start; discarding.` |

- All three built-in engines report `SupportsCancellation = true`, so `/cancel` returns **200** (or **404** if the id
  isn't found on a status-tracking engine), never **501**. The **501** path only appears if no engine is installed.
- Cancel is best-effort: on RabbitMQ `Cancel` returns accepted even for an unknown id (no job ledger); on
  Hangfire/InMemory it reflects whether a job was actually found.

## 7. Quick sanity without image-tools (any engine)

`GET https://localhost:5001/api/platform/jobs/<bogus-id>/cancel` isn't valid (POST only). To confirm the endpoint is
wired: `POST /api/platform/jobs/does-not-exist/cancel` → `200`/`404` on Hangfire/InMemory, `200` on RabbitMQ (recorded).
