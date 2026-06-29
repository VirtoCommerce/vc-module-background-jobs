using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using VirtoCommerce.BackgroundJobs.Core.Models;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.Security;

namespace VirtoCommerce.BackgroundJobs.Core.Services;

/// <summary>
/// Default shared execution path: deserialize the payload, resolve the registered
/// <c>IBackgroundJob&lt;T&gt;</c> handler from a fresh DI scope, and invoke it. Used by every engine.
/// </summary>
public sealed class DefaultJobDispatcher(IServiceProvider serviceProvider, IJobPayloadSerializer serializer) : IJobDispatcher
{
    public async Task Dispatch(JobEnvelope envelope, IJobExecutionContext context, CancellationToken cancellationToken = default)
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

        try
        {
            if (execute.Invoke(handler, [payload, context, cancellationToken]) is Task task)
            {
                await task;
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            await TryCompleteProgress(envelope, context, ex.Message);
            throw;
        }

        await TryCompleteProgress(envelope, context, error: null);
    }

    // When the job owns its progress notification end-to-end, close it (set Finished) so the admin UI bar doesn't
    // stay open. Jobs reporting into a shared notification (map/reduce) leave it to the owner.
    private static async Task TryCompleteProgress(JobEnvelope envelope, IJobExecutionContext context, string? error)
    {
        if (envelope.CompletesProgressNotification && context.Progress is PushNotificationJobProgress progress)
        {
            await progress.Complete(error);
        }
    }
}
