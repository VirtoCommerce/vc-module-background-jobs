# Background Jobs — Google Cloud Tasks engine (worked example)

A **custom background-job engine** for the Virto Commerce Background Jobs module, backed by
[Google Cloud Tasks](https://docs.cloud.google.com/tasks/docs). It is the worked example referenced by the host
module's "Writing a custom engine" guide: it proves the engine extension points against a **push** provider — unlike
Hangfire (in-process server) and RabbitMQ (in-process consumer), Cloud Tasks POSTs each job to an HTTP endpoint, so it
exercises a different processing shape and confirms the contracts don't assume a pull/consumer model.

> This is a sample. It compiles in the solution and demonstrates the full authoring pattern, but running it needs a
> real GCP project, a Cloud Tasks queue, and a publicly reachable callback URL.

## How it works

```
Producer (any instance)                Google Cloud Tasks                 Callback (processing instance)
  IBackgroundJob.Enqueue(payload)
    → GoogleCloudTasksJobEngine
        → CreateTask (HTTP target)  ──►  queue (retries, backoff)  ──►  POST /api/.../callback
                                                                          → validate OIDC token
                                                                          → IJobEnvelopeRunner.Run(envelope)
                                                                          → 200 ack  /  5xx retry
```

- **`GoogleCloudTasksJobEngine : IJobEngine`** — `Enqueue` creates an HTTP-target Cloud Task that POSTs the serialized
  `JobEnvelope` (plus a signed OIDC token) to the callback; the task resource name is the job id. `GetStatus` maps to
  `GetTask` (exists ⇒ scheduled/in-flight, NotFound ⇒ delivered/done); `Delete` maps to `DeleteTask`. It does **not**
  implement `IExpressionJobEngine`.
- **`GoogleCloudTasksCallbackController`** — the processing host (a push engine has no `IHostedService` consumer).
  Anonymous to the platform, authenticated by the Cloud Tasks OIDC token; on success it runs the shared
  `IJobEnvelopeRunner` (build context → dispatch) and returns 200, on handler failure 5xx so Cloud Tasks retries per
  the queue config.
- **Recurring** — reuses the host module's in-process cron scheduler via `AddInProcessRecurringScheduler()` (a cron
  tick enqueues a Cloud Task per occurrence, fleet-safe via the shared occurrence marker).
- **`PlatformStartup`** — self-activates only when `VirtoCommerce:BackgroundJobs:Provider` is `GoogleCloudTasks`, so
  the module is inert when another engine is selected.

## Configuration

Select the provider (engine-agnostic selector):

```json
{
  "VirtoCommerce": {
    "BackgroundJobs": { "Provider": "GoogleCloudTasks", "DefaultQueue": "default" }
  }
}
```

Engine-specific settings (`VirtoCommerce:GoogleCloudTasks`):

| Key | Required | Description |
|---|---|---|
| `ProjectId` | yes | GCP project that owns the queue. |
| `LocationId` | yes | Queue region, e.g. `us-central1`. |
| `QueueId` | yes | Cloud Tasks queue id tasks are created in. |
| `CallbackBaseUrl` | yes | Publicly reachable platform base URL; the engine appends `/api/background-jobs/google-cloud-tasks/callback`. Must reach an instance able to run handlers. |
| `OidcServiceAccountEmail` | **yes** | Service account Cloud Tasks signs the push token with; the callback verifies it. The callback **fails closed** (rejects every request) when this is unset — validating only the audience would accept any Google-signed token aimed at the public callback URL. |
| `OidcAudience` | no | Audience the token is minted for/validated against. Defaults to the full callback URL. |
| `ConsoleUri` | no | GCP console link surfaced as a developer tool. Composed from project/location/queue when unset. |

**Credentials** use Application Default Credentials (ADC): on GCP use Workload Identity or the attached service
account; locally point the standard `GOOGLE_APPLICATION_CREDENTIALS` environment variable at a service-account JSON
key file. No credential path is configured in code on purpose — ADC is the recommended, rotation-friendly mechanism.

### Queue & de-duplication semantics

- **Single queue.** Tasks are always created in the configured `QueueId`; `EnqueueOptions.Queue` / the envelope's
  logical queue are **not** used for routing. Cloud Tasks queues are GCP resources, so multi-queue routing would
  require pre-provisioned queues and a mapping — out of scope for this example.
- **De-duplication.** `EnqueueOptions.UniqueKey` is mapped to Cloud Tasks' name-based dedup: the task is created with
  a deterministic name (a hash of the key), so re-enqueuing the same key within the dedup window collapses to one task
  (a duplicate create returns `AlreadyExists`, which the engine treats as success).

## Authoring notes (what makes this a "custom engine")

- The project references `VirtoCommerce.BackgroundJobs.Core` (the engine port + agnostic services) and
  `.Data` (the reusable recurring scheduler) **by project reference** because it lives in this repo. A real
  out-of-repo module would consume them as the published **NuGet packages** and declare the runtime dependency in
  `module.manifest` (`<dependency id="VirtoCommerce.BackgroundJobs" .. />`).
- It implements only `IJobEngine` and adds an inbound callback controller; everything else (`IBackgroundJob`,
  `IJobEnvelopeRunner`/`IJobDispatcher`, serializer, progress, `RecurringJobsApplier`, the recurring state store) is
  reused from the host module. **No new platform/Core contract was required** to support a push engine.
- The callback runs a received envelope through the host module's shared `IJobEnvelopeRunner` (build context →
  dispatch), so any push engine reuses one execution path instead of re-implementing it.
