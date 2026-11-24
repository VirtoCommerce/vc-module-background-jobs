using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Moq;
using VirtoCommerce.BackgroundJobs.Core.Models;
using VirtoCommerce.BackgroundJobs.Core.Notifications;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.PushNotifications;
using Xunit;

namespace VirtoCommerce.BackgroundJobs.Tests;

public class PushNotificationJobProgressTests
{
    private static readonly DateTime _enqueuedAt = new(2026, 11, 24, 10, 0, 0, DateTimeKind.Utc);

    private static JobEnvelope CreateEnvelope(int attempt = 1) => new()
    {
        JobType = "t",
        PayloadType = "t",
        PayloadJson = "{}",
        ProgressNotificationId = "notification-1",
        Title = "Import",
        UserName = "tester",
        CompletesProgressNotification = true,
        Attempt = attempt,
        TotalAttempts = 3,
        Headers = new Dictionary<string, string>
        {
            [JobHeaders.EnqueuedAt] = _enqueuedAt.Ticks.ToString(CultureInfo.InvariantCulture),
        },
    };

    private static (PushNotificationJobProgress Progress, List<JobProgressPushNotification> Sent) CreateProgress(JobEnvelope envelope)
    {
        var sent = new List<JobProgressPushNotification>();
        var manager = new Mock<IPushNotificationManager>();
        manager.Setup(x => x.SendAsync(It.IsAny<PushNotification>()))
            .Callback<PushNotification>(x => sent.Add((JobProgressPushNotification)x))
            .Returns(Task.CompletedTask);

        return (new PushNotificationJobProgress(manager.Object, envelope, "job-1"), sent);
    }

    [Fact]
    public async Task Report_KeepsStartedCreatedAndJobIdStable()
    {
        var (progress, sent) = CreateProgress(CreateEnvelope());

        await progress.Report(new JobProgressInfo { Message = "one", ProcessedCount = 1, TotalCount = 10 }, TestContext.Current.CancellationToken);
        await progress.Report(new JobProgressInfo { Message = "two", ProcessedCount = 2 }, TestContext.Current.CancellationToken);

        Assert.Equal(2, sent.Count);
        Assert.All(sent, x =>
        {
            Assert.Equal(_enqueuedAt, x.Started);
            Assert.Equal(_enqueuedAt, x.Created);
            Assert.Equal("job-1", x.JobId);
            Assert.Equal("notification-1", x.Id);
            Assert.False(x.IsNew);
        });
    }

    [Fact]
    public async Task Report_AcrossAttempts_KeepsStartedCreatedAndJobId()
    {
        var (first, firstSent) = CreateProgress(CreateEnvelope(attempt: 1));
        var (second, secondSent) = CreateProgress(CreateEnvelope(attempt: 2));

        await first.Report(new JobProgressInfo { Message = "a", ProcessedCount = 50, TotalCount = 100 }, TestContext.Current.CancellationToken);
        await second.Report(new JobProgressInfo { Message = "b", ProcessedCount = 5 }, TestContext.Current.CancellationToken);

        var a = Assert.Single(firstSent);
        var b = Assert.Single(secondSent);
        Assert.Equal(1, a.Attempt);
        Assert.Equal(2, b.Attempt);
        Assert.Equal(3, b.TotalAttempts);
        Assert.Equal(a.Started, b.Started);
        Assert.Equal(a.Created, b.Created);
        Assert.Equal(a.JobId, b.JobId);
        Assert.Equal(a.Title, b.Title);
        Assert.Equal(5, b.ProcessedCount);
        Assert.Equal(0, b.TotalCount);
    }

    // Guards the count carry-over the builder rewrite touches.
    [Fact]
    public async Task Report_WithoutCounts_KeepsLastCounts()
    {
        var (progress, sent) = CreateProgress(CreateEnvelope());

        await progress.Report(new JobProgressInfo { ProcessedCount = 7, TotalCount = 20 }, TestContext.Current.CancellationToken);
        await progress.Report(new JobProgressInfo { Message = "still going" }, TestContext.Current.CancellationToken);

        var last = sent.Last();
        Assert.Equal(7, last.ProcessedCount);
        Assert.Equal(20, last.TotalCount);
        Assert.Equal("still going", last.Description);
    }

    [Fact]
    public async Task Complete_Succeeded_KeepsLastMessageAsDescription()
    {
        var (progress, sent) = CreateProgress(CreateEnvelope());

        await progress.Report(new JobProgressInfo { Message = "Imported 900 of 1000, 100 errors", ProcessedCount = 900, TotalCount = 1000 }, TestContext.Current.CancellationToken);
        await progress.Complete(JobOutcome.Succeeded, error: null);

        var completed = sent.Last();
        Assert.Equal("Imported 900 of 1000, 100 errors", completed.Description);
        Assert.Equal(JobOutcome.Succeeded, completed.Outcome);
        Assert.NotNull(completed.Finished);
        Assert.Equal(900, completed.ProcessedCount);
        Assert.Equal("job-1", completed.JobId);
    }
}
