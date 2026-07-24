#nullable enable
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using VirtoCommerce.BackgroundJobs.Core.Admin;
using VirtoCommerce.BackgroundJobs.Data.Admin;
using VirtoCommerce.BackgroundJobs.Core.Recurring;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.Settings;
using Xunit;

namespace VirtoCommerce.BackgroundJobs.Tests;

public class BackgroundJobsAdminTests
{
    private sealed class SamplePayload { }

    private sealed class SampleHandler : IBackgroundJobHandler<SamplePayload>
    {
        public Task Execute(SamplePayload payload, IJobExecutionContext context, CancellationToken cancellationToken = default)
            => Task.CompletedTask;
    }

    [Fact]
    public void AddBackgroundJob_RegistersDiscoverableDescriptor()
    {
        var services = new ServiceCollection();
        services.AddBackgroundJob<SampleHandler, SamplePayload>();
        using var provider = services.BuildServiceProvider();

        var descriptor = Assert.Single(provider.GetServices<BackgroundJobDescriptor>());
        Assert.Equal(nameof(SampleHandler), descriptor.Name);
        Assert.StartsWith(typeof(SampleHandler).FullName, descriptor.HandlerType);
        Assert.StartsWith(typeof(SamplePayload).FullName, descriptor.PayloadType);
    }

    [Fact]
    public void GetRegisteredJobs_DedupesByName_AndShortensTypeNames()
    {
        // Two descriptors with the same Name (e.g. a partner override re-registering) collapse to one entry.
        var descriptors = new[]
        {
            new BackgroundJobDescriptor { Name = "SampleHandler", HandlerType = "My.Ns.SampleHandler, My.Asm", PayloadType = "My.Ns.SamplePayload, My.Asm" },
            new BackgroundJobDescriptor { Name = "SampleHandler", HandlerType = "My.Ns.SampleHandler, My.Asm", PayloadType = "My.Ns.SamplePayload, My.Asm" },
        };

        var query = new BackgroundJobsAdminQuery(descriptors, [], Mock.Of<ISettingsManager>());

        var job = Assert.Single(query.GetRegisteredJobs());
        Assert.Equal("SampleHandler", job.Name);
        Assert.Equal("My.Ns.SampleHandler", job.HandlerType);   // assembly suffix dropped
        Assert.Equal("My.Ns.SamplePayload", job.PayloadType);
    }

    [Fact]
    public async Task GetRecurringJobs_FixedCron_ResolvesScheduleAndNextRun()
    {
        var registration = new RecurringJobRegistration
        {
            Id = "daily-cleanup",
            CronExpression = "0 0 * * *",   // 00:00 UTC daily
            Enabled = true,
            HandlerTypeName = "CleanupHandler",
            PayloadTypeName = "CleanupPayload",
            Trigger = (_, _) => Task.CompletedTask,
        };

        var stateStore = new Mock<IRecurringJobStateStore>();
        var lastRun = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        stateStore.Setup(x => x.GetLastOccurrence("daily-cleanup", It.IsAny<CancellationToken>())).ReturnsAsync(lastRun);

        var query = new BackgroundJobsAdminQuery([], [registration], Mock.Of<ISettingsManager>(), stateStore.Object);

        var job = Assert.Single(await query.GetRecurringJobsAsync(TestContext.Current.CancellationToken));
        Assert.Equal("daily-cleanup", job.Id);
        Assert.Equal("0 0 * * *", job.Cron);
        Assert.True(job.Enabled);
        Assert.False(job.SettingDriven);
        Assert.Equal("CleanupHandler", job.HandlerType);
        Assert.Equal(lastRun, job.LastRunUtc);
        Assert.NotNull(job.NextRunUtc);
        Assert.Equal(0, job.NextRunUtc!.Value.Hour);   // next occurrence is a midnight
    }
}
