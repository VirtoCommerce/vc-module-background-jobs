#nullable enable
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using VirtoCommerce.BackgroundJobs;
using VirtoCommerce.BackgroundJobs.Core;
using VirtoCommerce.BackgroundJobs.Core.Models;
using VirtoCommerce.BackgroundJobs.Core.Notifications;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.PushNotifications;
using VirtoCommerce.Platform.Core.Security;

namespace VirtoCommerce.BackgroundJobs.Data.Services;

/// <summary>
/// Engine-agnostic implementation of the developer-facing <see cref="IBackgroundJob"/> facade. Validates the
/// handler/payload pairing, builds a <see cref="JobEnvelope"/>, and delegates to the active <see cref="IJobEngine"/>.
/// </summary>
public sealed class JobEngineBackgroundJob(
    IJobPayloadSerializer serializer,
    IOptions<BackgroundJobsOptions> options,
    IPushNotificationManager pushNotificationManager,
    IUserNameResolver userNameResolver,
    // Optional: provided by the active engine module. Null when no engine is installed — enqueue then throws the
    // actionable BackgroundJobEngineNotInstalledException instead of failing DI resolution of this facade.
    IJobEngine? engine = null,
    // Last and optional, like the dispatcher's: a caller that builds the facade by hand stays source-compatible, and DI
    // injects the logger in production. A failed send that closes the notification of a job whose enqueue failed is reported through it.
    ILogger<JobEngineBackgroundJob>? logger = null) : IBackgroundJob, IBulkBackgroundJob
{
    private readonly BackgroundJobsOptions _options = options.Value;
    private readonly ILogger _logger = logger ?? NullLogger<JobEngineBackgroundJob>.Instance;

    public Task<string> Enqueue<THandler>(object payload, EnqueueOptions? options = null,
        CancellationToken cancellationToken = default)
        where THandler : class
        => EnqueueCore(typeof(THandler), payload, options, cancellationToken);

    public Task<string> Enqueue(Type handlerType, object payload, EnqueueOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(handlerType);
        return EnqueueCore(handlerType, payload, options, cancellationToken);
    }

    /// <summary>Surfaces the active engine's cancellation capability; <c>false</c> when no engine is installed.</summary>
    public bool SupportsCancellation => engine?.SupportsCancellation ?? false;

    /// <summary>
    /// Delegates cancellation to the active engine's <see cref="IJobEngine.Delete"/>. Returns <c>false</c> (rather than
    /// throwing) when no engine is installed, so a caller can treat "no engine" like "not supported".
    /// </summary>
    public Task<bool> Cancel(string jobId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(jobId);
        return engine is null ? Task.FromResult(false) : engine.Delete(jobId, cancellationToken);
    }

    private async Task<string> EnqueueCore(Type handlerType, object payload, EnqueueOptions? options,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(payload);

        if (engine is null)
        {
            throw new BackgroundJobEngineNotInstalledException();
        }

        // Validate the handler actually handles this payload, and record both the payload contract type (JobType, so
        // the worker knows which Execute to call) and the concrete handler type to resolve.
        var payloadContractType = ResolveHandlerPayloadType(handlerType, payload.GetType());

        var (payloadType, payloadJson) = serializer.Serialize(payload);
        var userName = userNameResolver.GetCurrentUserName();
        var title = string.IsNullOrEmpty(options?.Title) ? $"Background job: {payload.GetType().Name}" : options.Title;

        var enqueuedAt = DateTime.UtcNow;

        var progressNotificationId = options?.ProgressNotificationId;
        var ownsNotification = false;
        JobProgressPushNotification? queued = null;
        if (options?.ReportProgress == true && string.IsNullOrEmpty(progressNotificationId))
        {
            // The only send the facade makes once the engine has the job: from here the worker owns the notification (it
            // keeps this Started and adds the job id), so nothing is sent after the engine takes the job — that would race
            // the worker. The one exception is a job the engine reported a failure for, closed below.
            queued = JobProgressPushNotification.CreateQueued(userName ?? "system", title, enqueuedAt, totalCount: 0);
            await pushNotificationManager.SendAsync(queued);
            progressNotificationId = queued.Id;
            ownsNotification = true;
        }

        try
        {
            // Stamp the enqueue time (for queue-latency telemetry) and, if a producer set an ambient correlation id
            // (e.g. a load-test run), the run id — so every job on every engine carries the same measurable headers.
            var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [JobHeaders.EnqueuedAt] = enqueuedAt.Ticks.ToString(CultureInfo.InvariantCulture),
            };

            var runId = JobEnqueueContext.RunId;
            if (!string.IsNullOrEmpty(runId))
            {
                headers[JobHeaders.RunId] = runId;
            }

            // A map task of a shared (map/reduce) notification cannot learn the batch start time from its own envelope, so the
            // fan-out passes it through the ambient scope. Only an enqueue that reports into an existing notification needs it.
            var batchStartedAt = JobEnqueueContext.BatchStartedAt;
            if (batchStartedAt.HasValue && !string.IsNullOrEmpty(options?.ProgressNotificationId))
            {
                headers[JobHeaders.BatchStartedAt] = batchStartedAt.Value.Ticks.ToString(CultureInfo.InvariantCulture);
            }

            var envelope = new JobEnvelope
            {
                JobType = payloadContractType.AssemblyQualifiedName!,
                HandlerType = handlerType.AssemblyQualifiedName!,
                PayloadType = payloadType,
                PayloadJson = payloadJson,
                Queue = options?.Queue ?? _options.DefaultQueue,
                MaxRetryAttempts = options?.MaxRetryAttempts,
                UniqueKey = options?.UniqueKey,
                ProgressNotificationId = progressNotificationId,
                Title = title,
                CompletesProgressNotification = ownsNotification,
                UserName = userName,
                Headers = headers,
            };

            return await engine.Enqueue(envelope, options ?? new EnqueueOptions(), cancellationToken);
        }
        catch (Exception ex) when (queued is not null)
        {
            // The engine reported a failure, so no worker may ever close the notification sent above: close it here as
            // failed. If the engine did take the job after all (a broker publish can time out on its confirm after the
            // message was routed), the worker's own sends replace this close. A failed close must not hide the error the
            // caller needs.
            await pushNotificationManager.TryCloseEnqueueFailedAsync(queued, ex.Message, _logger);

            throw;
        }
    }

    public async Task<IReadOnlyList<string>> EnqueueBatch<THandler>(IReadOnlyCollection<object> payloads,
        EnqueueOptions? options = null, CancellationToken cancellationToken = default)
        where THandler : class
    {
        ArgumentNullException.ThrowIfNull(payloads);

        if (engine is null)
        {
            throw new BackgroundJobEngineNotInstalledException();
        }

        if (payloads.Count == 0)
        {
            return [];
        }

        var userName = userNameResolver.GetCurrentUserName();
        var runId = JobEnqueueContext.RunId;
        var handlerTypeName = typeof(THandler).AssemblyQualifiedName!;
        var queue = options?.Queue ?? _options.DefaultQueue;

        // Bulk enqueue is fire-and-forget only: no per-job progress notification (that would create N of them).
        var envelopes = new List<JobEnvelope>(payloads.Count);
        var index = 0;
        foreach (var payload in payloads)
        {
            ArgumentNullException.ThrowIfNull(payload);

            var payloadContractType = ResolveHandlerPayloadType(typeof(THandler), payload.GetType());
            var (payloadType, payloadJson) = serializer.Serialize(payload);

            var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [JobHeaders.EnqueuedAt] = DateTime.UtcNow.Ticks.ToString(CultureInfo.InvariantCulture),
            };

            if (!string.IsNullOrEmpty(runId))
            {
                headers[JobHeaders.RunId] = runId;
            }

            envelopes.Add(new JobEnvelope
            {
                JobType = payloadContractType.AssemblyQualifiedName!,
                HandlerType = handlerTypeName,
                PayloadType = payloadType,
                PayloadJson = payloadJson,
                Queue = queue,
                MaxRetryAttempts = options?.MaxRetryAttempts,
                // Suffix the batch-level key with the item index so a single dedup key can't collapse N distinct jobs
                // into one on dedup-honoring engines (e.g. Google Cloud Tasks) — each item stays uniquely addressable.
                UniqueKey = string.IsNullOrEmpty(options?.UniqueKey) ? null : $"{options.UniqueKey}:{index}",
                Title = options?.Title,
                UserName = userName,
                Headers = headers,
            });

            index++;
        }

        return await engine.EnqueueBatch(envelopes, options ?? new EnqueueOptions(), cancellationToken);
    }

    // Picks the IBackgroundJobHandler<T> the handler implements for this payload (exact T first, then an assignable
    // base — which covers AbstractTypeFactory-derived payloads). Throws if the handler doesn't handle the payload.
    private static Type ResolveHandlerPayloadType(Type handlerType, Type payloadConcreteType)
    {
        var candidates = handlerType.GetInterfaces()
            .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IBackgroundJobHandler<>))
            .Select(i => i.GetGenericArguments()[0])
            .ToArray();

        var match = Array.Find(candidates, t => t == payloadConcreteType)
            ?? Array.Find(candidates, t => t.IsAssignableFrom(payloadConcreteType));

        return match ?? throw new ArgumentException(
            $"Handler '{handlerType.Name}' does not handle a payload of type '{payloadConcreteType.Name}'. " +
            $"It must implement IBackgroundJobHandler<{payloadConcreteType.Name}> (or a base type of it).");
    }
}
