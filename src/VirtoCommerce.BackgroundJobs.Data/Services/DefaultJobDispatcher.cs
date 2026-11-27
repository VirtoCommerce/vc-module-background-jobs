#nullable enable
using System;
using System.Diagnostics;
using System.Globalization;
using System.Reflection;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using VirtoCommerce.BackgroundJobs;
using VirtoCommerce.BackgroundJobs.Core;
using VirtoCommerce.BackgroundJobs.Core.Models;
using VirtoCommerce.BackgroundJobs.Core.Notifications;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.Security;

namespace VirtoCommerce.BackgroundJobs.Data.Services;

/// <summary>
/// Default shared execution path: deserialize the payload, resolve the registered
/// <c>IBackgroundJob&lt;T&gt;</c> handler from a fresh DI scope, and invoke it. Used by every engine.
/// Times each invocation and emits engine-agnostic telemetry (<see cref="JobTelemetry"/>) so engines compare fairly.
/// </summary>
public sealed class DefaultJobDispatcher(
    IServiceProvider serviceProvider,
    IJobPayloadSerializer serializer,
    // Optional so the dispatcher stays constructible in minimal harnesses (the shipped conformance TestKit builds it
    // with just the two required deps). Production DI injects both; when absent, telemetry is simply skipped.
    JobTelemetry? telemetry = null,
    IOptions<BackgroundJobsOptions>? options = null,
    ILogger<DefaultJobDispatcher>? logger = null) : IJobDispatcher
{
    private readonly string _engine = options?.Value.Provider ?? "unknown";
    private readonly ILogger _logger = logger ?? NullLogger<DefaultJobDispatcher>.Instance;

    public async Task Dispatch(JobEnvelope envelope, IJobExecutionContext context, CancellationToken cancellationToken = default)
    {
        var stopwatch = Stopwatch.StartNew();
        var outcome = "success";
        try
        {
            try
            {
                if (envelope.CompletesProgressNotification)
                {
                    await TrySend(() => context.Progress.Report(new JobProgressInfo()), "report the start of the attempt", envelope, context, cancellationToken);
                }

                await Invoke(envelope, context, cancellationToken);
            }
            catch (Exception ex)
            {
                // MethodInfo.Invoke wraps a SYNCHRONOUS throw from the handler (a guard before its first await, or a
                // non-async handler) in TargetInvocationException — unwrap so cancellation is recognized and the real
                // cause (not the "Exception has been thrown by the target of an invocation" wrapper) is surfaced.
                var actual = (ex as TargetInvocationException)?.InnerException ?? ex;
                outcome = actual is OperationCanceledException && cancellationToken.IsCancellationRequested ? "canceled" : "failure";

                // Cancellation (shutdown / token) is NOT a job failure: leave the progress notification open and let
                // the engine honor it (RabbitMQ requeue, Hangfire re-run) rather than marking the job finished/failed.
                if (!(actual is OperationCanceledException && cancellationToken.IsCancellationRequested))
                {
                    await ReportFailedAttempt(envelope, context, actual.Message, cancellationToken);
                }

                ExceptionDispatchInfo.Throw(actual); // preserves the original stack; never returns
            }

            await TrySend(() => context.Progress.Complete(JobOutcome.Succeeded), "close the notification as succeeded", envelope, context, cancellationToken);
        }
        finally
        {
            stopwatch.Stop();
            telemetry?.JobCompleted(_engine, HandlerName(envelope), outcome, GetRunId(envelope),
                stopwatch.Elapsed.TotalMilliseconds, GetQueueLatencyMs(envelope));
        }
    }

    private async Task Invoke(JobEnvelope envelope, IJobExecutionContext context, CancellationToken cancellationToken)
    {
        var payload = serializer.Deserialize(envelope.PayloadType, envelope.PayloadJson);

        var payloadType = Type.GetType(envelope.JobType)
            ?? throw new InvalidOperationException($"Cannot resolve job type '{envelope.JobType}'.");

        var handlerInterfaceType = typeof(IBackgroundJobHandler<>).MakeGenericType(payloadType);

        // Run the handler in its own DI scope so scoped dependencies (repositories, DbContext, …) resolve correctly.
        using var scope = serviceProvider.CreateScope();

        // Restore the enqueuing user so handlers see the same user context as the producer (engine-agnostic — Hangfire
        // also sets this via its own filter; doing it here covers RabbitMQ, the push callback, and any custom engine).
        if (!string.IsNullOrEmpty(envelope.UserName))
        {
            scope.ServiceProvider.GetService<IUserNameResolver>()?.SetCurrentUserName(envelope.UserName);
        }

        // Handler-explicit enqueue (Enqueue<THandler>) names the concrete handler, so resolve it by type — this is
        // how one payload type drives several handlers. Otherwise resolve by payload type (IBackgroundJobHandler<T>).
        object handler;
        if (!string.IsNullOrEmpty(envelope.HandlerType))
        {
            var concreteHandlerType = Type.GetType(envelope.HandlerType)
                ?? throw new InvalidOperationException($"Cannot resolve handler type '{envelope.HandlerType}'.");

            handler = scope.ServiceProvider.GetService(concreteHandlerType)
                ?? throw new InvalidOperationException(
                    $"No background-job handler '{concreteHandlerType.Name}' is registered.");
        }
        else
        {
            handler = scope.ServiceProvider.GetService(handlerInterfaceType)
                ?? throw new InvalidOperationException(
                    $"No background-job handler 'IBackgroundJobHandler<{payloadType.Name}>' is registered.");
        }

        // Invoke through the IBackgroundJobHandler<payload> interface either way — the concrete handler implements it.
        var execute = handlerInterfaceType.GetMethod(nameof(IBackgroundJobHandler<object>.Execute))
            ?? throw new InvalidOperationException("IBackgroundJob<T>.Execute was not found.");

        if (execute.Invoke(handler, [payload, context, cancellationToken]) is Task task)
        {
            await task;
        }
    }

    // Short, readable handler name for telemetry dimensions (concrete handler when named, else the payload type).
    private static string HandlerName(JobEnvelope envelope)
    {
        var qualified = envelope.HandlerType ?? envelope.JobType;
        var comma = qualified.IndexOf(',');
        var typeName = comma >= 0 ? qualified[..comma] : qualified;
        var dot = typeName.LastIndexOf('.');
        return dot >= 0 ? typeName[(dot + 1)..] : typeName;
    }

    private static string? GetRunId(JobEnvelope envelope) =>
        envelope.Headers.TryGetValue(JobHeaders.RunId, out var runId) ? runId : null;

    // Enqueue → dispatch delay from the enqueue-time header; null when absent (e.g. a legacy/redelivered message).
    private static double? GetQueueLatencyMs(JobEnvelope envelope)
    {
        if (envelope.Headers.TryGetValue(JobHeaders.EnqueuedAt, out var raw)
            && long.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var ticks))
        {
            var elapsedMs = (DateTime.UtcNow.Ticks - ticks) / (double)TimeSpan.TicksPerMillisecond;
            return elapsedMs >= 0 ? elapsedMs : null;
        }

        return null;
    }

    // No retry announcement once the token is cancelled: a cancel may be about to stop the retry.
    private async Task ReportFailedAttempt(JobEnvelope envelope, IJobExecutionContext context, string message, CancellationToken cancellationToken)
    {
        if (envelope.IsFinalAttempt())
        {
            await TrySend(() => context.Progress.Complete(JobOutcome.Failed, message), "close the notification as failed", envelope, context, cancellationToken);
        }
        else if (envelope.CompletesProgressNotification && !cancellationToken.IsCancellationRequested)
        {
            await TrySend(
                () => context.Progress.Report(new JobProgressInfo
                {
                    Message = $"Attempt {envelope.Attempt} of {envelope.TotalAttempts} failed, retrying: {message}",
                }),
                "announce the retry",
                envelope,
                context,
                cancellationToken);
        }
    }

    private Task TrySend(Func<Task> send, string operation, JobEnvelope envelope, IJobExecutionContext context, CancellationToken cancellationToken)
    {
        return BestEffortNotificationSend.TrySendAsync(
            send,
            ex => _logger.LogError(ex, "Could not {Operation} for job {JobId} (engine {Engine}, handler {Handler}); the job's outcome is unchanged.",
                operation, context.JobId, _engine, HandlerName(envelope)),
            ex => _logger.LogInformation(ex, "Could not {Operation} for job {JobId} (engine {Engine}, handler {Handler}): the job was canceled.",
                operation, context.JobId, _engine, HandlerName(envelope)),
            cancellationToken);
    }
}
