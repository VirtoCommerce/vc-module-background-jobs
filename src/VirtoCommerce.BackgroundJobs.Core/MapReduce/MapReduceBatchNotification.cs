using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using VirtoCommerce.BackgroundJobs.Core.Notifications;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.PushNotifications;

namespace VirtoCommerce.BackgroundJobs.Core.MapReduce;

internal static class MapReduceBatchNotification
{
    public static Task CloseAsync(
        IPushNotificationManager pushNotificationManager,
        MapReduceBatch batch,
        string description,
        JobOutcome outcome,
        ILogger logger)
    {
        if (string.IsNullOrEmpty(batch.ProgressNotificationId))
        {
            return Task.CompletedTask;
        }

        return BestEffortNotificationSend.TrySendAsync(
            () => pushNotificationManager.SendAsync(JobProgressNotificationBuilder.Build(new JobProgressNotificationFields(
                batch.ProgressNotificationId,
                batch.UserName ?? "system",
                batch.Title,
                batch.Started,
                description,
                ProcessedCount: batch.Total,
                TotalCount: batch.Total,
                JobId: null,
                Attempt: null,
                TotalAttempts: null,
                outcome.ToString(),
                Error: null,
                Finished: DateTime.UtcNow,
                IsNew: true))),
            ex => logger.LogError(ex, "Could not close the batch progress notification {NotificationId} as {Outcome}; the batch's outcome is unchanged.",
                batch.ProgressNotificationId, outcome));
    }

    // A cancel leaves the bar open: the engine decides what a cancel means.
    public static Task CloseFailedOnFinalAttemptAsync(
        IPushNotificationManager pushNotificationManager,
        MapReduceBatch batch,
        IJobExecutionContext context,
        Exception exception,
        CancellationToken cancellationToken,
        string stage,
        ILogger logger)
    {
        var canceled = exception is OperationCanceledException && cancellationToken.IsCancellationRequested;
        if (context is JobExecutionContext { IsFinalAttempt: true } && !canceled)
        {
            return CloseAsync(pushNotificationManager, batch, $"{stage} failed: {exception.Message}", JobOutcome.Failed, logger);
        }

        return Task.CompletedTask;
    }
}
