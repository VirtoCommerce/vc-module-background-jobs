using System;

namespace VirtoCommerce.BackgroundJobs.Core.Notifications;

/// <summary>Everything a job-progress notification carries, so every producer fills the same set of fields.</summary>
internal sealed record JobProgressNotificationFields(
    string Id,
    string Creator,
    string? Title,
    DateTime? Started,
    string? Description,
    long ProcessedCount,
    long TotalCount,
    string? JobId,
    int? Attempt,
    int? TotalAttempts,
    string? Outcome,
    string? Error,
    DateTime? Finished,
    bool IsNew);

/// <summary>
/// Builds the <see cref="JobProgressPushNotification"/> for the enqueue-time notification and for every update the
/// worker sends. Push storage replaces a notification by id, so each send must carry the whole set of display fields.
/// </summary>
internal static class JobProgressNotificationBuilder
{
    public static JobProgressPushNotification Build(JobProgressNotificationFields fields)
    {
        var notification = new JobProgressPushNotification(fields.Creator)
        {
            Id = fields.Id,
            Title = fields.Title,
            Started = fields.Started,
            Description = fields.Description,
            ProcessedCount = fields.ProcessedCount,
            TotalCount = fields.TotalCount,
            JobId = fields.JobId,
            Attempt = fields.Attempt,
            TotalAttempts = fields.TotalAttempts,
            Outcome = fields.Outcome,
            Error = fields.Error,
            Finished = fields.Finished,
            IsNew = fields.IsNew,
        };

        // The notification's creation time is when the job was queued, not when the worker last reported.
        if (fields.Started.HasValue)
        {
            notification.Created = fields.Started.Value;
        }

        return notification;
    }
}
