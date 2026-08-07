#nullable enable
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Moq;
using VirtoCommerce.BackgroundJobs.Core;
using VirtoCommerce.BackgroundJobs.Core.Models;
using VirtoCommerce.BackgroundJobs.Data.Services;
using VirtoCommerce.BackgroundJobs.InMemory.Extensions;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.PushNotifications;
using VirtoCommerce.Platform.Core.Security;
using Xunit;

namespace VirtoCommerce.BackgroundJobs.Tests;

/// <summary>
/// Covers the per-enqueue retry override (<see cref="EnqueueOptions.MaxRetryAttempts"/>), the engine-agnostic
/// replacement for Hangfire's <c>[AutomaticRetry(Attempts = N)]</c>: the facade must put it on the envelope, and an
/// engine must prefer it over the engine-wide default.
/// </summary>
public class PerJobRetryTests
{
    public class RetryPayload
    {
    }

    private sealed class AttemptCounter
    {
        private int _count;

        public int Count => Volatile.Read(ref _count);

        public void Increment() => Interlocked.Increment(ref _count);
    }

    private sealed class AlwaysFailingHandler(AttemptCounter counter) : IBackgroundJobHandler<RetryPayload>
    {
        public Task Execute(RetryPayload payload, IJobExecutionContext context, CancellationToken cancellationToken = default)
        {
            counter.Increment();
            throw new InvalidOperationException("boom");
        }
    }

    [Theory]
    [InlineData(null, 3)]  // no override: engine-wide default (2 retries) → 3 attempts
    [InlineData(0, 1)]     // override 0: no retries at all → 1 attempt, the [AutomaticRetry(Attempts = 0)] equivalent
    [InlineData(1, 2)]     // override tightens the default
    public async Task InMemoryEngine_Prefers_PerJob_MaxRetryAttempts_Over_Engine_Default(int? maxRetryAttempts, int expectedAttempts)
    {
        var counter = new AttemptCounter();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(counter);
        services.AddSingleton<IPushNotificationManager>(Mock.Of<IPushNotificationManager>());
        services.AddSingleton<IUserNameResolver>(Mock.Of<IUserNameResolver>());
        services.AddSingleton<IJobPayloadSerializer, JsonJobPayloadSerializer>();
        services.AddSingleton<IJobDispatcher, DefaultJobDispatcher>();
        services.AddScoped<IBackgroundJob, JobEngineBackgroundJob>();
        services.AddBackgroundJob<AlwaysFailingHandler, RetryPayload>();
        services.Configure<BackgroundJobsOptions>(options => options.MaxRetryAttempts = 2);
        services.AddInMemoryJobEngine();

        using var provider = services.BuildServiceProvider();
        var jobs = provider.GetRequiredService<IBackgroundJob>();

        await jobs.Enqueue<AlwaysFailingHandler>(
            new RetryPayload(),
            new EnqueueOptions { MaxRetryAttempts = maxRetryAttempts },
            TestContext.Current.CancellationToken);

        // The in-memory engine runs the job on a background task, so wait for the attempt count to settle rather than
        // for a handle we don't get back. Retries carry no delay, so "unchanged for a grace period" means "finished".
        var attempts = await WaitForStableCountAsync(counter, TestContext.Current.CancellationToken);

        Assert.Equal(expectedAttempts, attempts);
    }

    [Fact]
    public async Task Facade_Puts_MaxRetryAttempts_On_The_Envelope()
    {
        JobEnvelope? captured = null;
        var engine = new Mock<IJobEngine>();
        engine.SetupGet(x => x.ProviderName).Returns("Test");
        engine.Setup(x => x.Enqueue(It.IsAny<JobEnvelope>(), It.IsAny<EnqueueOptions>(), It.IsAny<CancellationToken>()))
            .Callback<JobEnvelope, EnqueueOptions, CancellationToken>((envelope, _, _) => captured = envelope)
            .ReturnsAsync("job-1");

        var sut = new JobEngineBackgroundJob(
            new JsonJobPayloadSerializer(),
            Options.Create(new BackgroundJobsOptions()),
            Mock.Of<IPushNotificationManager>(),
            Mock.Of<IUserNameResolver>(),
            engine.Object);

        await sut.Enqueue<AlwaysFailingHandler>(new RetryPayload(), new EnqueueOptions { MaxRetryAttempts = 0 },
            TestContext.Current.CancellationToken);

        Assert.NotNull(captured);
        Assert.Equal(0, captured!.MaxRetryAttempts);
    }

    [Fact]
    public async Task Facade_Leaves_MaxRetryAttempts_Null_When_Not_Requested()
    {
        JobEnvelope? captured = null;
        var engine = new Mock<IJobEngine>();
        engine.SetupGet(x => x.ProviderName).Returns("Test");
        engine.Setup(x => x.Enqueue(It.IsAny<JobEnvelope>(), It.IsAny<EnqueueOptions>(), It.IsAny<CancellationToken>()))
            .Callback<JobEnvelope, EnqueueOptions, CancellationToken>((envelope, _, _) => captured = envelope)
            .ReturnsAsync("job-2");

        var sut = new JobEngineBackgroundJob(
            new JsonJobPayloadSerializer(),
            Options.Create(new BackgroundJobsOptions()),
            Mock.Of<IPushNotificationManager>(),
            Mock.Of<IUserNameResolver>(),
            engine.Object);

        await sut.Enqueue<AlwaysFailingHandler>(new RetryPayload(), cancellationToken: TestContext.Current.CancellationToken);

        Assert.NotNull(captured);
        // Null, not 0 — the engine must fall back to its configured default rather than read "no retries".
        Assert.Null(captured!.MaxRetryAttempts);
    }

    private static async Task<int> WaitForStableCountAsync(AttemptCounter counter, CancellationToken cancellationToken)
    {
        var grace = TimeSpan.FromMilliseconds(300);
        var timeout = TimeSpan.FromSeconds(10);
        var stopwatch = Stopwatch.StartNew();
        var last = -1;

        while (stopwatch.Elapsed < timeout)
        {
            var current = counter.Count;
            if (current > 0 && current == last)
            {
                return current;
            }

            last = current;
            await Task.Delay(grace, cancellationToken);
        }

        return counter.Count;
    }
}
