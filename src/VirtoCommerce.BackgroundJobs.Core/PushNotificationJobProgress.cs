using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using VirtoCommerce.BackgroundJobs.Core.Models;
using VirtoCommerce.BackgroundJobs.Core.Notifications;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.Modularity;
using VirtoCommerce.Platform.Core.PushNotifications;

namespace VirtoCommerce.BackgroundJobs;

/// <summary>
/// <see cref="IJobProgress"/> that streams updates to the admin UI by sending a
/// <see cref="JobProgressPushNotification"/> (keyed by the enqueue-time notification id) over SignalR.
/// </summary>
public sealed class PushNotificationJobProgress : IJobProgress
{
    private readonly IPushNotificationManager _pushNotificationManager;
    private readonly string _notificationId;
    private readonly string _creator;
    private readonly string? _title;
    private readonly bool _owned;
    private readonly string? _jobId;
    private readonly DateTime? _started;
    private readonly int? _attempt;
    private readonly int? _totalAttempts;

    private long _processedCount;
    private long _totalCount;
    private string? _lastMessage;

    /// <summary>
    /// Reports into the notification as its owner, without a job id or attempt.
    /// </summary>
    [Obsolete("Owns its notification whatever the job's envelope says. Build the reporter from the envelope (PushNotificationJobProgress(IPushNotificationManager, JobEnvelope, string)) or through JobExecutionContextFactory.",
        DiagnosticId = "VC0016",
        UrlFormat = "https://docs.virtocommerce.org/products/products-virto3-versions")]
    public PushNotificationJobProgress(
        IPushNotificationManager pushNotificationManager,
        string notificationId,
        string? creator,
        string? title = null)
    {
        _pushNotificationManager = pushNotificationManager;
        _notificationId = notificationId;
        _creator = creator ?? "system";
        _title = title;
        _owned = true;
    }

    /// <summary>
    /// Reports for the job <paramref name="envelope"/> describes: its own notification with its job id, or a shared one (a map/reduce batch) without it.
    /// </summary>
    public PushNotificationJobProgress(IPushNotificationManager pushNotificationManager, JobEnvelope envelope, string jobId)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        _pushNotificationManager = pushNotificationManager;
        _notificationId = envelope.ProgressNotificationId!;
        _creator = envelope.UserName ?? "system";
        _title = envelope.Title;
        _owned = envelope.CompletesProgressNotification;
        _jobId = _owned ? jobId : null;
        _started = ReadTicks(envelope, _owned ? JobHeaders.EnqueuedAt : JobHeaders.BatchStartedAt);

        // A shared bar's attempt is the batch's, not one task's.
        _attempt = _owned ? envelope.Attempt : null;
        _totalAttempts = _owned ? envelope.TotalAttempts : null;
    }

    public Task Report(JobProgressInfo progress, CancellationToken cancellationToken = default)
    {
        _processedCount = progress.ProcessedCount ?? _processedCount;
        _totalCount = progress.TotalCount ?? _totalCount;

        var notification = Build(string.IsNullOrEmpty(progress.Message) ? _lastMessage : progress.Message, outcome: null, error: null, finished: null, isNew: false);

        if (!string.IsNullOrEmpty(progress.Message))
        {
            _lastMessage = progress.Message;
            notification.ProgressLog.Add(new ProgressMessage { Message = progress.Message });
        }

        return _pushNotificationManager.SendAsync(notification);
    }

    /// <summary>
    /// Marks the notification finished, keeping the last counts, title and message, with an error entry on failure. Does nothing for a shared notification: its owner closes it.
    /// </summary>
    public Task Complete(JobOutcome outcome, string? error = null, CancellationToken cancellationToken = default)
    {
        if (!_owned)
        {
            return Task.CompletedTask;
        }

        return _pushNotificationManager.SendAsync(BuildClosing(outcome.ToString(), error));
    }

    private JobProgressPushNotification BuildClosing(string outcome, string? error)
    {
        var notification = Build(_lastMessage ?? outcome, outcome, error, finished: DateTime.UtcNow, isNew: true);

        if (!string.IsNullOrEmpty(error))
        {
            notification.ProgressLog.Add(new ProgressMessage { Level = ProgressMessageLevel.Error, Message = error });
        }

        return notification;
    }

    /// <summary>Completes as <see cref="JobOutcome.Failed"/> when <paramref name="error"/> is set, otherwise as <see cref="JobOutcome.Succeeded"/>.</summary>
    public Task Complete(string? error, CancellationToken cancellationToken = default)
    {
        return Complete(error is null ? JobOutcome.Succeeded : JobOutcome.Failed, error, cancellationToken);
    }

    private JobProgressPushNotification Build(string? description, string? outcome, string? error, DateTime? finished, bool isNew)
    {
        return JobProgressNotificationBuilder.Build(new JobProgressNotificationFields(
            _notificationId,
            _creator,
            _title,
            _started,
            description,
            _processedCount,
            _totalCount,
            _jobId,
            _attempt,
            _totalAttempts,
            outcome,
            error,
            finished,
            IsNew: isNew));
    }

    private static DateTime? ReadTicks(JobEnvelope envelope, string header)
    {
        if (envelope.Headers.TryGetValue(header, out var raw)
            && long.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var ticks)
            && ticks >= DateTime.MinValue.Ticks
            && ticks <= DateTime.MaxValue.Ticks)
        {
            return new DateTime(ticks, DateTimeKind.Utc);
        }

        return null;
    }
}
