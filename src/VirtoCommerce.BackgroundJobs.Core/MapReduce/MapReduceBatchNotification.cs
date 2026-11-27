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

        var close = new ProgressClose(description, outcome, DateTime.UtcNow);

        // Marked before the close is sent: a map report that checks the marker afterwards skips the bar.
        try
        {
            await store.MarkProgressClosedAsync(batch.BatchId, close);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Could not mark the progress of batch {BatchId} closed; a late map report may reopen the notification.", batch.BatchId);
        }

        await SendCloseAsync(pushNotificationManager, batch, close, logger);
    }

    public static Task SendCloseAsync(IPushNotificationManager pushNotificationManager, MapReduceBatch batch, ProgressClose close, ILogger logger)
        => BestEffortNotificationSend.TrySendAsync(
            () => pushNotificationManager.SendAsync(JobProgressNotificationBuilder.Build(new JobProgressNotificationFields(
                batch.ProgressNotificationId!,
                batch.UserName ?? "system",
                batch.Title,
                Created: batch.Started,
                Started: null,
                close.Description,
                ProcessedCount: batch.Total,
                TotalCount: batch.Total,
                JobId: null,
                Attempt: null,
                TotalAttempts: null,
                close.Outcome.ToString(),
                Error: null,
                Finished: close.Finished,
                IsNew: true))),
            ex => logger.LogError(ex, "Could not close the batch progress notification {NotificationId} as {Outcome}; the batch's outcome is unchanged.",
                batch.ProgressNotificationId, close.Outcome));

    // A cancel leaves the bar open: the engine decides what a cancel means.
    public static async Task CloseFailedOnFinalAttemptAsync(
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
        if (context is JobExecutionContext { IsFinalAttempt: true } && !canceled && !await IsGoneAsync(store, batch, logger))
        {
            await CloseAsync(store, pushNotificationManager, batch, $"{stage} failed: {exception.Message}", JobOutcome.Failed, logger);
        }
    }

    private static async Task<bool> IsGoneAsync(IMapReduceBatchStore store, MapReduceBatch batch, ILogger logger)
    {
        try
        {
            return await store.GetAsync(batch.BatchId) is null;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Could not read batch {BatchId} before closing its progress as Failed; closing it.", batch.BatchId);

            return false;
        }
    }
}
