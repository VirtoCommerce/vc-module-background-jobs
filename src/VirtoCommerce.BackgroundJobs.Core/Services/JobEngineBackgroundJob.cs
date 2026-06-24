using System;
using System.Linq.Expressions;
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
/// Engine-agnostic implementation of the developer-facing <see cref="IBackgroundJob"/> facade. Builds a
/// <see cref="JobEnvelope"/> from a payload and delegates to the active <see cref="IJobEngine"/>; expression
/// enqueue is forwarded to <see cref="IExpressionJobEngine"/> when the active engine supports it.
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

    public string Enqueue(Expression<Action> methodCall) => EnqueueExpression(e => e.Enqueue(methodCall));

    public string Enqueue(Expression<Func<Task>> methodCall) => EnqueueExpression(e => e.Enqueue(methodCall));

    public async Task<string> Enqueue<TPayload>(TPayload payload, EnqueueOptions? options = null,
        CancellationToken cancellationToken = default)
        where TPayload : class
    {
        ArgumentNullException.ThrowIfNull(payload);

        if (engine is null)
        {
            throw new BackgroundJobEngineNotInstalledException();
        }

        var (payloadType, payloadJson) = serializer.Serialize(payload);
        var userName = userNameResolver.GetCurrentUserName();
        var title = string.IsNullOrEmpty(options?.Title) ? $"Background job: {typeof(TPayload).Name}" : options.Title;

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
            JobType = typeof(TPayload).AssemblyQualifiedName!,
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

    private string EnqueueExpression(Func<IExpressionJobEngine, string> enqueue)
    {
        if (engine is null)
        {
            throw new BackgroundJobEngineNotInstalledException();
        }

        if (engine is IExpressionJobEngine expressionEngine)
        {
            return enqueue(expressionEngine);
        }

        throw new NotSupportedException(
            $"Expression-based enqueue requires the Hangfire provider; the active provider is '{engine.ProviderName}'. " +
            "Use Enqueue(payload) with an IBackgroundJobHandler<TPayload> handler instead.");
    }
}
