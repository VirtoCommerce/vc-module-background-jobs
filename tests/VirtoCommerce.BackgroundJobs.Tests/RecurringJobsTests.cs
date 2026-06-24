using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using VirtoCommerce.BackgroundJobs.Core.Recurring;
using VirtoCommerce.BackgroundJobs.Data.Recurring;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.Settings;
using Xunit;

namespace VirtoCommerce.BackgroundJobs.Tests;

public class RecurringJobsTests
{
    public class SamplePayload
    {
        public string Value { get; set; }
    }

    private sealed class SampleHandler : IBackgroundJobHandler<SamplePayload>
    {
        public Task Execute(SamplePayload payload, IJobExecutionContext context, CancellationToken cancellationToken = default)
            => Task.CompletedTask;
    }

    [Fact]
    public void AddRecurringJob_Registers_Handler_And_Registration()
    {
        var services = new ServiceCollection();

        services.AddRecurringJob<SamplePayload, SampleHandler>(s => s
            .WithId("sample")
            .WithCron("0 2 * * *")
            .WithQueue("maintenance"));

        using var provider = services.BuildServiceProvider();

        Assert.NotNull(provider.GetService<IBackgroundJobHandler<SamplePayload>>());

        var registration = Assert.Single(provider.GetServices<RecurringJobRegistration>());
        Assert.Equal("sample", registration.Id);
        Assert.Equal("0 2 * * *", registration.CronExpression);
        Assert.Null(registration.EnablerSetting);
    }

    [Fact]
    public void AddRecurringJob_WithoutSchedule_Throws()
    {
        var services = new ServiceCollection();

        Assert.Throws<InvalidOperationException>(() =>
            services.AddRecurringJob<SamplePayload, SampleHandler>(s => s.WithId("no-schedule")));
    }

    [Fact]
    public void AddRecurringJob_Infers_Payload_From_Handler()
    {
        var services = new ServiceCollection();

        services.AddRecurringJob<SampleHandler>(s => s.WithId("inferred").WithCron("0 2 * * *"));

        using var provider = services.BuildServiceProvider();

        Assert.IsType<SampleHandler>(provider.GetService<IBackgroundJobHandler<SamplePayload>>());
        var registration = provider.GetServices<RecurringJobRegistration>().Single();
        Assert.Equal("inferred", registration.Id);
    }

    [Fact]
    public void AddRecurringJob_WithBothCronAndSettings_Throws()
    {
        var services = new ServiceCollection();
        var enabler = new SettingDescriptor { Name = "Enable" };
        var cron = new SettingDescriptor { Name = "Cron" };

        // WithCron and FromSettings are mutually exclusive.
        Assert.Throws<InvalidOperationException>(() =>
            services.AddRecurringJob<SamplePayload, SampleHandler>(s => s
                .WithId("both")
                .WithCron("0 2 * * *")
                .FromSettings(enabler, cron)));
    }

    [Fact]
    public async Task Registration_Trigger_Enqueues_Payload_On_Configured_Queue()
    {
        var services = new ServiceCollection();
        services.AddRecurringJob<SamplePayload, SampleHandler>(s => s
            .WithId("sample")
            .WithCron("0 2 * * *")
            .WithQueue("maintenance"));

        using var provider = services.BuildServiceProvider();
        var registration = provider.GetServices<RecurringJobRegistration>().Single();

        SamplePayload capturedPayload = null;
        EnqueueOptions capturedOptions = null;
        var backgroundJob = new Mock<IBackgroundJob>();
        backgroundJob
            .Setup(x => x.Enqueue(It.IsAny<SamplePayload>(), It.IsAny<EnqueueOptions>(), It.IsAny<CancellationToken>()))
            .Callback<SamplePayload, EnqueueOptions, CancellationToken>((payload, options, _) =>
            {
                capturedPayload = payload;
                capturedOptions = options;
            })
            .ReturnsAsync("job-1");

        await registration.Trigger(backgroundJob.Object, TestContext.Current.CancellationToken);

        Assert.NotNull(capturedPayload);
        Assert.Equal("maintenance", capturedOptions?.Queue);
    }

    [Fact]
    public async Task Registration_Trigger_Enqueues_Factory_Payload_With_Parameters()
    {
        var services = new ServiceCollection();
        services.AddRecurringJob<SamplePayload, SampleHandler>(
            () => new SamplePayload { Value = "configured" },
            s => s.WithId("sample").WithCron("0 2 * * *"));

        using var provider = services.BuildServiceProvider();
        var registration = provider.GetServices<RecurringJobRegistration>().Single();

        SamplePayload capturedPayload = null;
        var backgroundJob = new Mock<IBackgroundJob>();
        backgroundJob
            .Setup(x => x.Enqueue(It.IsAny<SamplePayload>(), It.IsAny<EnqueueOptions>(), It.IsAny<CancellationToken>()))
            .Callback<SamplePayload, EnqueueOptions, CancellationToken>((payload, _, _) => capturedPayload = payload)
            .ReturnsAsync("job-1");

        await registration.Trigger(backgroundJob.Object, TestContext.Current.CancellationToken);

        Assert.NotNull(capturedPayload);
        Assert.Equal("configured", capturedPayload.Value); // parameters flow through to each occurrence
    }

    [Fact]
    public async Task Registration_Trigger_Parameterless_Enqueues_NonNull_Payload()
    {
        var services = new ServiceCollection();
        services.AddRecurringJob<SamplePayload, SampleHandler>(s => s.WithId("sample").WithCron("0 2 * * *"));

        using var provider = services.BuildServiceProvider();
        var registration = provider.GetServices<RecurringJobRegistration>().Single();

        SamplePayload capturedPayload = null;
        var backgroundJob = new Mock<IBackgroundJob>();
        backgroundJob
            .Setup(x => x.Enqueue(It.IsAny<SamplePayload>(), It.IsAny<EnqueueOptions>(), It.IsAny<CancellationToken>()))
            .Callback<SamplePayload, EnqueueOptions, CancellationToken>((payload, _, _) => capturedPayload = payload)
            .ReturnsAsync("job-1");

        await registration.Trigger(backgroundJob.Object, TestContext.Current.CancellationToken);

        Assert.NotNull(capturedPayload); // AbstractTypeFactory-created default instance
    }

    [Fact]
    public void AddRecurringJob_WithEnabledFalse_MarksRegistrationDisabled()
    {
        var services = new ServiceCollection();

        services.AddRecurringJob<SamplePayload, SampleHandler>(s => s
            .WithId("lockout")
            .WithCron("0 2 * * *")
            .WithEnabled(false));

        using var provider = services.BuildServiceProvider();

        var registration = provider.GetServices<RecurringJobRegistration>().Single();
        Assert.False(registration.Enabled);
    }

    [Fact]
    public async Task Applier_RemovesDisabledFixedCronJob_AndSchedulesEnabledOne()
    {
        var enabled = new RecurringJobRegistration
        {
            Id = "on",
            CronExpression = "0 2 * * *",
            Enabled = true,
            Trigger = (_, _) => Task.CompletedTask,
        };
        var disabled = new RecurringJobRegistration
        {
            Id = "off",
            CronExpression = "0 2 * * *",
            Enabled = false,
            Trigger = (_, _) => Task.CompletedTask,
        };

        var services = new ServiceCollection();
        services.AddSingleton(Mock.Of<ISettingsManager>());
        using var provider = services.BuildServiceProvider();

        var scheduler = new Mock<IRecurringJobScheduler>();
        scheduler.Setup(x => x.AddOrUpdate(It.IsAny<RecurringJobRegistration>(), It.IsAny<string>(), It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
        scheduler.Setup(x => x.Remove(It.IsAny<string>(), It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);

        var applier = new RecurringJobsApplier(
            [enabled, disabled], provider, NullLogger<RecurringJobsApplier>.Instance, scheduler.Object);

        await applier.StartAsync(TestContext.Current.CancellationToken);
        await applier.StopAsync(TestContext.Current.CancellationToken);

        // Enabled fixed-cron job is scheduled; disabled one is removed (clears any leftover from a previous run).
        scheduler.Verify(x => x.AddOrUpdate(It.Is<RecurringJobRegistration>(r => r.Id == "on"), "0 2 * * *", It.IsAny<CancellationToken>()), Times.Once);
        scheduler.Verify(x => x.Remove("off", It.IsAny<CancellationToken>()), Times.Once);
        scheduler.Verify(x => x.AddOrUpdate(It.Is<RecurringJobRegistration>(r => r.Id == "off"), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task InMemoryStateStore_Tracks_LastOccurrence()
    {
        var store = new InMemoryRecurringJobStateStore();

        Assert.Null(await store.GetLastOccurrence("job", TestContext.Current.CancellationToken));

        var occurrence = new DateTime(2026, 6, 18, 2, 0, 0, DateTimeKind.Utc);
        await store.SetLastOccurrence("job", occurrence, TestContext.Current.CancellationToken);

        Assert.Equal(occurrence, await store.GetLastOccurrence("job", TestContext.Current.CancellationToken));
    }
}
