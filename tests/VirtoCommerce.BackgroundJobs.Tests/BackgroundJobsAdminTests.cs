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
    public void GetRegisteredJobs_ListsDistinctHandlers_SharingATypeName()
    {
        // VCST-6312: several modules register a handler class with the same simple name (LogEntityChangesJobHandler),
        // so the default Name collides. Each distinct handler must still be listed.
        var descriptors = new[]
        {
            new BackgroundJobDescriptor { Name = "LogEntityChangesJobHandler", HandlerType = "Catalog.LogEntityChangesJobHandler, Catalog", PayloadType = "Catalog.LogEntityChangesJobPayload, Catalog", Triggerable = false },
            new BackgroundJobDescriptor { Name = "LogEntityChangesJobHandler", HandlerType = "Pricing.LogEntityChangesJobHandler, Pricing", PayloadType = "Pricing.LogEntityChangesJobPayload, Pricing", Triggerable = false },
        };

        var query = new BackgroundJobsAdminQuery(descriptors, [], Mock.Of<ISettingsManager>());

        var jobs = query.GetRegisteredJobs();
        Assert.Equal(["Catalog.LogEntityChangesJobHandler", "Pricing.LogEntityChangesJobHandler"], jobs.Select(x => x.HandlerType));
        Assert.Equal(["Catalog.LogEntityChangesJobHandler", "Pricing.LogEntityChangesJobHandler"], jobs.Select(x => x.Name));

        // Each is addressable by its listed (qualified) name; the shared short name is ambiguous and addresses none.
        Assert.Equal("Catalog.LogEntityChangesJobHandler, Catalog", query.FindRegisteredJob("Catalog.LogEntityChangesJobHandler")?.HandlerType);
        Assert.Equal("Pricing.LogEntityChangesJobHandler, Pricing", query.FindRegisteredJob("Pricing.LogEntityChangesJobHandler")?.HandlerType);
        Assert.Null(query.FindRegisteredJob("LogEntityChangesJobHandler"));
    }

    [Fact]
    public void GetRegisteredJobs_SameHandlerForSeveralPayloads_UnderOneName_QualifiesWithPayload()
    {
        var descriptors = new[]
        {
            new BackgroundJobDescriptor { Name = "multi", HandlerType = "My.Ns.MultiHandler, My.Asm", PayloadType = "My.Ns.PayloadA, My.Asm" },
            new BackgroundJobDescriptor { Name = "multi", HandlerType = "My.Ns.MultiHandler, My.Asm", PayloadType = "My.Ns.PayloadB, My.Asm" },
        };

        var query = new BackgroundJobsAdminQuery(descriptors, [], Mock.Of<ISettingsManager>());

        Assert.Equal(["My.Ns.MultiHandler:My.Ns.PayloadA", "My.Ns.MultiHandler:My.Ns.PayloadB"], query.GetRegisteredJobs().Select(x => x.Name));
        Assert.Equal("My.Ns.PayloadB, My.Asm", query.FindRegisteredJob("My.Ns.MultiHandler:My.Ns.PayloadB")?.PayloadType);
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
