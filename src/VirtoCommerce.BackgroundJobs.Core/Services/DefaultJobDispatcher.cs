using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using VirtoCommerce.BackgroundJobs.Core.Models;
using VirtoCommerce.Platform.Core.Jobs;

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

        var handlerType = typeof(IBackgroundJobHandler<>).MakeGenericType(payloadType);

        // Run the handler in its own DI scope so scoped dependencies (repositories, DbContext, …) resolve correctly.
        using var scope = serviceProvider.CreateScope();

        var handler = scope.ServiceProvider.GetService(handlerType)
            ?? throw new InvalidOperationException(
                $"No background-job handler 'IBackgroundJobHandler<{payloadType.Name}>' is registered.");

        var execute = handlerType.GetMethod(nameof(IBackgroundJobHandler<object>.Execute))
            ?? throw new InvalidOperationException("IBackgroundJob<T>.Execute was not found.");

        if (execute.Invoke(handler, [payload, context, cancellationToken]) is Task task)
        {
            await task;
        }
    }
}
