using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using VirtoCommerce.Platform.Core.Jobs;
using Xunit;

namespace VirtoCommerce.BackgroundJobs.Conformance;

/// <summary>
/// Recurring-scheduler conformance — part of <see cref="JobEngineConformanceTests{TFixture}"/>. Certifies the
/// <see cref="IRecurringJobScheduler"/> the engine registers (native, like Hangfire, or the in-process generic
/// scheduler) accepts add/update/remove, and that a recurring occurrence's trigger enqueues a job that actually
/// runs on the engine. Real cron-timer firing is the scheduler's own concern (covered by its unit tests); this
/// asserts the recurring → enqueue → dispatch wiring deterministically.
/// </summary>
public abstract partial class JobEngineConformanceTests<TFixture>
{
    // 15 — the scheduler port accepts a registration and removes it without error.
    [Fact]
    public async Task Recurring_Scheduler_AddOrUpdate_Then_Remove_DoNotThrow()
    {
        RequireEngine();
        var scheduler = Fixture.Services.GetService<IRecurringJobScheduler>();
        Assert.SkipUnless(Fixture.Capabilities.SupportsRecurringScheduler && scheduler is not null, "engine registers no IRecurringJobScheduler.");

        var registration = new RecurringJobRegistration
        {
            Id = "conformance-recurring-" + NewId(),
            CronExpression = "* * * * *",
            Trigger = (jobs, ct) => jobs.Enqueue(new ConformancePayload { CorrelationId = NewId(), Value = "recur" }, null, ct),
        };

        await scheduler!.AddOrUpdate(registration, "* * * * *", TestContext.Current.CancellationToken);
        await scheduler.Remove(registration.Id, TestContext.Current.CancellationToken);
    }

    // 16 — a recurring occurrence's trigger enqueues a payload that the engine processes.
    [Fact]
    public async Task Recurring_Trigger_Enqueues_Runnable_Job()
    {
        RequireEngine();
        var id = NewId();

        var registration = new RecurringJobRegistration
        {
            Id = "conformance-recurring-" + NewId(),
            CronExpression = "* * * * *",
            Trigger = (jobs, ct) => jobs.Enqueue(new ConformancePayload { CorrelationId = id, Value = "recur" }, null, ct),
        };

        using var scope = Fixture.Services.CreateScope();
        await registration.Trigger(scope.ServiceProvider.GetRequiredService<IBackgroundJob>(), TestContext.Current.CancellationToken);

        await Fixture.Probe.WaitAsync(id, ConformanceConstants.DefaultTimeout, TestContext.Current.CancellationToken);
        Assert.NotNull(Fixture.Probe.Job(id));
    }
}
