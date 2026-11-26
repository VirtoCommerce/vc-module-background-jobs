#nullable enable
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Hangfire;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using Moq;
using Newtonsoft.Json;
using VirtoCommerce.BackgroundJobs.Core.Models;
using VirtoCommerce.BackgroundJobs.Core.Notifications;
using VirtoCommerce.BackgroundJobs.Hangfire;
using VirtoCommerce.Platform.Core.PushNotifications;
using Xunit;
using JobOutcome = VirtoCommerce.Platform.Core.Jobs.JobOutcome;

namespace VirtoCommerce.BackgroundJobs.Tests;

public class CancelBeforeRunStateFilterTests
{
    private const string _hangfireJobId = "job-1";

    private static readonly DateTime _enqueuedAt = new(2026, 11, 24, 10, 0, 0, DateTimeKind.Utc);

    private static JobEnvelope CreateEnvelope() => new()
    {
        JobType = "t",
        PayloadType = "t",
        PayloadJson = "{}",
        ProgressNotificationId = "notification-1",
        Title = "Import",
        UserName = "tester",
        CompletesProgressNotification = true,
        MaxRetryAttempts = 3,
        Headers = new Dictionary<string, string>
        {
            [JobHeaders.EnqueuedAt] = _enqueuedAt.Ticks.ToString(CultureInfo.InvariantCulture),
        },
    };

    // storedRetryCount: Hangfire's serialized RetryCount parameter, absent until the first failure.
    private static ApplyStateContext CreateDeleteContext(string oldStateName, string? storedRetryCount)
    {
        var envelopeJson = JsonConvert.SerializeObject(CreateEnvelope());
        var job = Job.FromExpression<HangfireJobExecutor>(x => x.Execute(envelopeJson, null!, CancellationToken.None));

        var connection = new Mock<IStorageConnection>();
        connection.Setup(x => x.GetJobParameter(_hangfireJobId, "RetryCount")).Returns(storedRetryCount!);

        return new ApplyStateContext(
            new Mock<JobStorage>().Object,
            connection.Object,
            new Mock<IWriteOnlyTransaction>().Object,
            new BackgroundJob(_hangfireJobId, job, DateTime.UtcNow),
            new DeletedState(),
            oldStateName);
    }

    private static (CancelBeforeRunStateFilter Filter, List<JobProgressPushNotification> Sent) CreateFilter()
    {
        var sent = new List<JobProgressPushNotification>();
        var manager = new Mock<IPushNotificationManager>();
        manager.Setup(x => x.SendAsync(It.IsAny<PushNotification>()))
            .Callback<PushNotification>(x => sent.Add((JobProgressPushNotification)x))
            .Returns(Task.CompletedTask);

        return (new CancelBeforeRunStateFilter(manager.Object), sent);
    }

    // Scheduled with 2 retries spent is attempt 3; Enqueued with none stored is attempt 1. 3 retries allowed: 4 attempts.
    [Theory]
    [InlineData("Scheduled", "2", 3)]
    [InlineData("Enqueued", null, 1)]
    public void Delete_Of_A_Job_That_Is_Not_Running_Closes_Its_Notification_Canceled(string oldState, string? storedRetryCount, int expectedAttempt)
    {
        var (filter, sent) = CreateFilter();

        filter.OnStateApplied(CreateDeleteContext(oldState, storedRetryCount), new Mock<IWriteOnlyTransaction>().Object);

        var closing = Assert.Single(sent);
        Assert.Equal("notification-1", closing.Id);
        Assert.Equal(nameof(JobOutcome.Canceled), closing.Outcome);
        Assert.NotNull(closing.Finished);
        Assert.Equal(_hangfireJobId, closing.JobId);
        Assert.Equal(_enqueuedAt, closing.Started);
        Assert.Equal(expectedAttempt, closing.Attempt);
        Assert.Equal(4, closing.TotalAttempts);
    }

    // OnAttemptsExceeded = Delete moves a Processing job the dispatcher already closed Failed to Deleted.
    [Fact]
    public void Delete_Of_A_Processing_Job_Sends_Nothing()
    {
        var (filter, sent) = CreateFilter();

        filter.OnStateApplied(CreateDeleteContext("Processing", storedRetryCount: null), new Mock<IWriteOnlyTransaction>().Object);

        Assert.Empty(sent);
    }
}
