#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Hangfire;
using Hangfire.Common;
using Hangfire.Server;
using Hangfire.Storage;
using Moq;
using Newtonsoft.Json;
using VirtoCommerce.BackgroundJobs.Core.Models;
using VirtoCommerce.BackgroundJobs.Hangfire;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.PushNotifications;
using Xunit;
using HangfireBackgroundJob = Hangfire.BackgroundJob;
using HangfireJob = Hangfire.Common.Job;

namespace VirtoCommerce.BackgroundJobs.Tests;

/// <summary>Runs <see cref="HangfireJobExecutor"/> against a hand-built <see cref="PerformContext"/> with <see cref="GlobalJobFilters"/> emptied, hence the collection.</summary>
[Collection(GlobalJobFiltersCollection.Name)]
public sealed class HangfireJobExecutorTests : IDisposable
{
    private readonly List<object> _originalFilters = GlobalJobFilters.Filters.Select(x => x.Instance).ToList();

    public HangfireJobExecutorTests()
    {
        foreach (var filter in _originalFilters)
        {
            GlobalJobFilters.Filters.Remove(filter);
        }
    }

    public void Dispose()
    {
        foreach (var filter in _originalFilters)
        {
            GlobalJobFilters.Filters.Add(filter);
        }
    }

    // Without a registered AutomaticRetryAttribute Hangfire never retries, so a per-job value must not promise attempts.
    [Fact]
    public async Task Execute_WithoutGlobalRetryFilter_PerJobRetriesDoNotWidenTheBudget()
    {
        var envelope = new JobEnvelope { JobType = "t", PayloadType = "t", PayloadJson = "{}", MaxRetryAttempts = 5 };
        var dispatcher = new CapturingDispatcher();
        var executor = new HangfireJobExecutor(dispatcher, new Mock<IPushNotificationManager>().Object);

        var connection = new Mock<IStorageConnection>();
        var job = HangfireJob.FromExpression<HangfireJobExecutor>(x => x.Execute("{}", null!, CancellationToken.None));
        var performContext = new PerformContext(
            new Mock<JobStorage>().Object,
            connection.Object,
            new HangfireBackgroundJob("job-1", job, DateTime.UtcNow),
            new Mock<IJobCancellationToken>().Object);

        await executor.Execute(JsonConvert.SerializeObject(envelope), performContext, TestContext.Current.CancellationToken);

        Assert.NotNull(dispatcher.Seen);
        Assert.Equal(1, dispatcher.Seen!.TotalAttempts);
        Assert.Equal(1, dispatcher.Seen.Attempt);
    }

    private sealed class CapturingDispatcher : IJobDispatcher
    {
        public JobEnvelope? Seen { get; private set; }

        public Task Dispatch(JobEnvelope envelope, IJobExecutionContext context, CancellationToken cancellationToken = default)
        {
            Seen = envelope;

            return Task.CompletedTask;
        }
    }
}
