# Background Jobs — Sample Module

A minimal, runnable **consumer module** that demonstrates how to use the Virto Commerce background-jobs
abstraction: define a payload, implement a handler, register it, enqueue fire-and-forget jobs **with or without
progress**, and declare a **recurring (cron) job** — on whichever engine is active (Hangfire or RabbitMQ).

It is a single project and references **only `VirtoCommerce.Platform.Core`** — there is no compile-time dependency
on the Background Jobs module or any engine. The runtime dependency on the engine is declared in `module.manifest`.

## What it shows

| File | Demonstrates |
|---|---|
| [`Jobs/SampleJobPayload.cs`](Jobs/SampleJobPayload.cs) | A serializable payload (`ValueObject`), created via `AbstractTypeFactory` so partners can extend it. |
| [`Jobs/SampleJob.cs`](Jobs/SampleJob.cs) | An `IBackgroundJobHandler<SampleJobPayload>` handler that reports progress step-by-step via `context.Progress.Report(...)`. |
| [`Jobs/SampleRecurringJob.cs`](Jobs/SampleRecurringJob.cs) | A recurring job — the same `IBackgroundJobHandler<TPayload>` contract, no recurring-specific code. |
| [`Module.cs`](Module.cs) | Registering the handler (`AddBackgroundJob`) and a recurring schedule (`AddRecurringJob`). |
| [`Controllers/Api/SampleJobsController.cs`](Controllers/Api/SampleJobsController.cs) | Enqueuing via the `IBackgroundJob` facade, with and without progress. |

## Run it

1. Make sure the **`VirtoCommerce.BackgroundJobs`** module is installed and an engine is configured
   (`VirtoCommerce:BackgroundJobs:Provider = Hangfire`, `Mode = Both`).
2. Build and deploy this module into your platform instance (`app_data/modules/VirtoCommerce.BackgroundJobs.SampleModule/`),
   or build the package with `vc-build Compress`.
3. Start the platform and call the endpoint (Swagger or curl), authenticated as an admin:

```http
POST /api/background-jobs-sample/enqueue?withProgress=true&steps=5&message=hello
```

* **`withProgress=true`** → watch the steps stream into the admin notification dropdown (SignalR) and the job in
  the `/hangfire` dashboard.
* **`withProgress=false`** → the job runs silently.
* The call returns the **job id**; poll its status via `GET /api/platform/jobs/{id}`.

## How it works

```csharp
// Define a payload (overridable via AbstractTypeFactory).
public class SampleJobPayload : ValueObject
{
    public string? Message { get; set; }
    public int StepCount { get; set; } = 3;
}

// Implement the handler.
public class SampleJob(ILogger<SampleJob> logger) : IBackgroundJobHandler<SampleJobPayload>
{
    public async Task Execute(SampleJobPayload payload, IJobExecutionContext context, CancellationToken cancellationToken = default)
    {
        for (var i = 1; i <= payload.StepCount; i++)
        {
            await context.Progress.Report(
                new() { Message = $"Step {i} of {payload.StepCount}", ProcessedCount = i, TotalCount = payload.StepCount },
                cancellationToken);
            await Task.Delay(1000, cancellationToken);
        }
    }
}

// Register (Module.Initialize).
AbstractTypeFactory<SampleJobPayload>.RegisterType<SampleJobPayload>();
services.AddBackgroundJob<SampleJobPayload, SampleJob>();

// Enqueue (anywhere, via the IBackgroundJob facade).
var payload = AbstractTypeFactory<SampleJobPayload>.TryCreateInstance();
payload.Message = "hello";
await backgroundJob.Enqueue(payload, new EnqueueOptions { ReportProgress = true });
```

### Recurring job

A recurring job is just a handler + a schedule — declared once at registration, no recurring-specific contract.
It runs on whichever engine is active (Hangfire-native scheduling, or the in-process cron scheduler for RabbitMQ).

```csharp
// A plain handler.
public class SampleRecurringJob(ILogger<SampleRecurringJob> logger) : IBackgroundJobHandler<SampleRecurringJobPayload>
{
    public Task Execute(SampleRecurringJobPayload payload, IJobExecutionContext context, CancellationToken ct = default)
    {
        logger.LogInformation("SampleRecurringJob fired at {Utc:o}", DateTime.UtcNow);
        return Task.CompletedTask;
    }
}

// Register handler + schedule (Module.Initialize).
services.AddRecurringJob<SampleRecurringJobPayload, SampleRecurringJob>(s => s
    .WithId("BackgroundJobs.Sample.Heartbeat")
    .WithCron("*/5 * * * *"));   // every 5 minutes
```
With Hangfire it appears in the `/hangfire` Recurring Jobs dashboard; with RabbitMQ the in-process scheduler fires
it (exactly once across the fleet) and enqueues it for a worker.

## License

Copyright (c) Virto Solutions LTD.  All rights reserved.

This software is licensed under the Virto Commerce Open Software License (the "License"); you
may not use this file except in compliance with the License. You may
obtain a copy of the License at http://virtocommerce.com/opensourcelicense.

Unless required to applicable law or written form, the software
distributed under the License is provided on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied.
