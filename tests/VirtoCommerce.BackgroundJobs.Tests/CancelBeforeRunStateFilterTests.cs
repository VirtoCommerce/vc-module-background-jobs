#nullable enable
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
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

namespace VirtoCommerce.BackgroundJobs.Tests;

/// <summary>
/// A job moved to <c>Deleted</c> before it runs, or while it waits for a retry, has no executor to close its
/// notification: the filter does it. Which old states count is the whole point, because closing a job that is still
/// <c>Processing</c> would stack a stray <c>Canceled</c> on the <c>Failed</c> the dispatcher already wrote.
/// </summary>
public class CancelBeforeRunStateFilterTests
{
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
        Headers = new Dictionary<string, string>
        {
            [JobHeaders.EnqueuedAt] = _enqueuedAt.Ticks.ToString(CultureInfo.InvariantCulture),
        },
    };

    // The state change of a job the engine enqueued: Execute(envelopeJson, context, token), as HangfireJobEngine builds it.
    private static ApplyStateContext CreateDeleteContext(string oldStateName)
    {
        var envelopeJson = JsonConvert.SerializeObject(CreateEnvelope());
        var job = Job.FromExpression<HangfireJobExecutor>(x => x.Execute(envelopeJson, null!, CancellationToken.None));

        return new ApplyStateContext(
            new Mock<JobStorage>().Object,
            new Mock<IStorageConnection>().Object,
            new Mock<IWriteOnlyTransaction>().Object,
            new BackgroundJob("job-1", job, DateTime.UtcNow),
            new DeletedState(),
            oldStateName);
    }

    private static (CancelBeforeRunStateFilter Filter, List<JobProgressPushNotification> Sent) CreateFilter()
    {
        var sent = new List<JobProgressPushNotification>();
        var manager = new Mock<IPushNotificationManager>();
        manager.Setup(x => x.Send(It.IsAny<PushNotification>()))
            .Callback<PushNotification>(x => sent.Add((JobProgressPushNotification)x));

        return (new CancelBeforeRunStateFilter(manager.Object), sent);
    }

    // A retry wait is Scheduled; a job not yet picked up is Enqueued. No executor will run for either.
    [Theory]
    [InlineData("Scheduled")]
    [InlineData("Enqueued")]
    public void Delete_Of_A_Job_That_Is_Not_Running_Closes_Its_Notification_Canceled(string oldState)
    {
        var (filter, sent) = CreateFilter();

        filter.OnStateApplied(CreateDeleteContext(oldState), new Mock<IWriteOnlyTransaction>().Object);

        var closing = Assert.Single(sent);
        Assert.Equal("notification-1", closing.Id);
        Assert.Equal(JobOutcome.Canceled, closing.Outcome);
        Assert.NotNull(closing.Finished);
        Assert.Null(closing.JobId);
    }

    // AutomaticRetry's OnAttemptsExceeded = Delete moves a Processing job to Deleted after the dispatcher has closed
    // it Failed; the executor owns that job, so the filter must not add a Canceled on top.
    [Fact]
    public void Delete_Of_A_Processing_Job_Sends_Nothing()
    {
        var (filter, sent) = CreateFilter();

        filter.OnStateApplied(CreateDeleteContext("Processing"), new Mock<IWriteOnlyTransaction>().Object);

        Assert.Empty(sent);
    }
}
