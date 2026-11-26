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
    /// The notification a job gets when it is enqueued; <paramref name="enqueuedAt"/> is its start and creation time,
    /// and <paramref name="totalCount"/> the work already known (0 when none).
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
    /// Closes <paramref name="queued"/> for a job whose enqueue failed: same id, title and start, outcome Failed, and
    /// <paramref name="error"/> as the error.
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
            $"Enqueue failed: {error}",
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
    /// The <see cref="JobOutcome"/> name once the job ended; null while queued or running. A string because enum
    /// serialization on the live channel depends on the deployment.
    /// </summary>
    public string? Outcome { get; set; }

    public int? Attempt { get; set; }

    public int? TotalAttempts { get; set; }

    public string? Error { get; set; }

    public ICollection<ProgressMessage> ProgressLog { get; set; } = new List<ProgressMessage>();
}
