using System;
using System.Collections.Generic;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.Modularity;
using VirtoCommerce.Platform.Core.PushNotifications;

namespace VirtoCommerce.BackgroundJobs.Core.Notifications;

/// <summary>Push notification used to stream generic background-job progress to the admin UI over SignalR.</summary>
public class JobProgressPushNotification : PushNotification
{
    public JobProgressPushNotification(string creator)
        : base(creator)
    {
        NotifyType = nameof(JobProgressPushNotification);
    }

    /// <summary>
    /// The notification a job gets when it is enqueued: new, titled, and not yet picked up. <paramref name="enqueuedAt"/>
    /// is both its start and creation time, and is written into the description so two enqueues of the same job never
    /// share a description (platform storage folds a notification with the same title, type and description into the
    /// previous one). <paramref name="totalCount"/> is the work the job already knows it has (a map/reduce batch knows its
    /// item count; a single job, 0, does not).
    /// </summary>
    public static JobProgressPushNotification CreateQueued(string creator, string title, DateTime enqueuedAt, long totalCount)
    {
        return JobProgressNotificationBuilder.Build(new JobProgressNotificationFields(
            Guid.NewGuid().ToString(),
            creator,
            title,
            enqueuedAt,
            $"Queued at {enqueuedAt:O}",
            ProcessedCount: 0,
            TotalCount: totalCount,
            JobId: null,
            Attempt: null,
            TotalAttempts: null,
            Outcome: null,
            Error: null,
            Finished: null,
            IsNew: true));
    }

    /// <summary>
    /// Closes the notification <paramref name="queued"/> as failed, for a job the engine never took: no worker will run it,
    /// so no worker will close the notification either, and the admin UI would keep showing it as queued. It replaces the
    /// queued notification (same id, creator, title, start time and total) and records <paramref name="error"/> the way the
    /// worker's own failed close does.
    /// </summary>
    public static JobProgressPushNotification CreateEnqueueFailed(JobProgressPushNotification queued, string error)
    {
        ArgumentNullException.ThrowIfNull(queued);

        var outcome = JobOutcome.Failed.ToString();
        var notification = JobProgressNotificationBuilder.Build(new JobProgressNotificationFields(
            queued.Id,
            queued.Creator,
            queued.Title,
            queued.Started,
            outcome,
            ProcessedCount: 0,
            TotalCount: queued.TotalCount,
            JobId: null,
            Attempt: null,
            TotalAttempts: null,
            Outcome: outcome,
            Error: error,
            Finished: DateTime.UtcNow,
            IsNew: true));

        notification.ProgressLog.Add(new ProgressMessage { Level = ProgressMessageLevel.Error, Message = error });

        return notification;
    }

    public DateTime? Started { get; set; }

    public DateTime? Finished { get; set; }

    public long ProcessedCount { get; set; }

    public long TotalCount { get; set; }

    /// <summary>The engine's job id, written by the worker once it runs the job (null while queued).</summary>
    public string? JobId { get; set; }

    /// <summary>
    /// The name of the <see cref="JobOutcome"/> once the job ended, null while it is queued or running. A string, not the
    /// enum: how an enum is serialized on the live channel depends on the deployment.
    /// </summary>
    public string? Outcome { get; set; }

    public int? Attempt { get; set; }

    public int? TotalAttempts { get; set; }

    public string? Error { get; set; }

    public ICollection<ProgressMessage> ProgressLog { get; set; } = new List<ProgressMessage>();
}
