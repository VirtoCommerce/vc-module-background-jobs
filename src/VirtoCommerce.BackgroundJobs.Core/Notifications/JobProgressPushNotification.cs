using System;
using System.Collections.Generic;
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

    public DateTime? Started { get; set; }

    public DateTime? Finished { get; set; }

    public long ProcessedCount { get; set; }

    public long TotalCount { get; set; }

    public ICollection<ProgressMessage> ProgressLog { get; set; } = new List<ProgressMessage>();
}
