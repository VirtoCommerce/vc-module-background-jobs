using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using VirtoCommerce.BackgroundJobs.Core.Models;
using VirtoCommerce.BackgroundJobs.Core.Notifications;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.PushNotifications;
using VirtoCommerce.Platform.Core.Security;

namespace VirtoCommerce.BackgroundJobs.Core.Services;

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
    IJobEngine? engine = null) : IBackgroundJob
{
    private readonly BackgroundJobsOptions _options = options.Value;

    public async Task<string> Enqueue<THandler>(object payload, EnqueueOptions? options = null,
        CancellationToken cancellationToken = default)
        where THandler : class
    {
        ArgumentNullException.ThrowIfNull(payload);

        if (engine is null)
        {
            throw new BackgroundJobEngineNotInstalledException();
        }

        // Validate the handler actually handles this payload, and record both the payload contract type (JobType, so
        // the worker knows which Execute to call) and the concrete handler type to resolve.
        var payloadContractType = ResolveHandlerPayloadType(typeof(THandler), payload.GetType());

        var (payloadType, payloadJson) = serializer.Serialize(payload);
        var userName = userNameResolver.GetCurrentUserName();
        var title = string.IsNullOrEmpty(options?.Title) ? $"Background job: {payload.GetType().Name}" : options.Title;

        var progressNotificationId = options?.ProgressNotificationId;
        var ownsNotification = false;
        if (options?.ReportProgress == true && string.IsNullOrEmpty(progressNotificationId))
        {
            var notification = new JobProgressPushNotification(userName ?? "system")
            {
                Title = title,
                Description = "Queued",
                Started = DateTime.UtcNow,
            };
            await pushNotificationManager.SendAsync(notification);
            progressNotificationId = notification.Id;
            ownsNotification = true;
        }

        var envelope = new JobEnvelope
        {
            JobType = payloadContractType.AssemblyQualifiedName!,
            HandlerType = typeof(THandler).AssemblyQualifiedName!,
            PayloadType = payloadType,
            PayloadJson = payloadJson,
            Queue = options?.Queue ?? _options.DefaultQueue,
            UniqueKey = options?.UniqueKey,
            ProgressNotificationId = progressNotificationId,
            Title = title,
            CompletesProgressNotification = ownsNotification,
            UserName = userName,
        };

        return await engine.Enqueue(envelope, options ?? new EnqueueOptions(), cancellationToken);
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
