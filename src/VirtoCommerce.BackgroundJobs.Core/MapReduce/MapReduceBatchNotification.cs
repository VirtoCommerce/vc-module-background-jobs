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
    public static async Task CloseAsync(
        IMapReduceBatchStore store,
        IPushNotificationManager pushNotificationManager,
        MapReduceBatch batch,
        string description,
        JobOutcome outcome,
        ILogger logger)
    {
        if (string.IsNullOrEmpty(batch.ProgressNotificationId))
        {
            return;
        }

        // Marked before the close is sent: a map report that checks the marker afterwards skips the bar.
        try
        {
            await store.MarkProgressClosedAsync(batch.BatchId);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Could not mark the progress of batch {BatchId} closed; a late map report may reopen the notification.", batch.BatchId);
        }

        await BestEffortNotificationSend.TrySendAsync(
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
        IMapReduceBatchStore store,
        IPushNotificationManager pushNotificationManager,
        MapReduceBatch batch,
        IJobExecutionContext context,
        Exception exception,
        string stage,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        var canceled = exception is OperationCanceledException && cancellationToken.IsCancellationRequested;
        if (context is JobExecutionContext { IsFinalAttempt: true } && !canceled)
        {
            return CloseAsync(store, pushNotificationManager, batch, $"{stage} failed: {exception.Message}", JobOutcome.Failed, logger);
        }

        return Task.CompletedTask;
    }
}
