using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Moq;
using VirtoCommerce.BackgroundJobs.Core;
using VirtoCommerce.BackgroundJobs.Core.Notifications;
using VirtoCommerce.BackgroundJobs.Data.Cancellation;
using VirtoCommerce.BackgroundJobs.Data.MapReduce;
using VirtoCommerce.BackgroundJobs.Data.Services;
using VirtoCommerce.Platform.Core.Common;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.PushNotifications;
using VirtoCommerce.Platform.Core.Security;
using Xunit;

namespace VirtoCommerce.BackgroundJobs.Conformance;

/// <summary>
/// Base class an engine implementer subclasses to certify their engine. It wires the engine-agnostic services the
/// standard depends on (the same set the module's <c>PlatformStartup</c> registers — serializer, dispatcher,
/// <see cref="IBackgroundJob"/> facade, map/reduce orchestration, the conformance handlers) and leaves three hooks:
/// register the real engine (<see cref="TryConfigureEngine"/>), and start/stop its real worker
/// (<see cref="StartWorkerAsync"/>/<see cref="StopWorkerAsync"/>, which by default start every registered
/// <see cref="IHostedService"/>). Used as an xUnit <c>IClassFixture</c> by the conformance test classes.
/// </summary>
public abstract class JobEngineConformanceFixture : IAsyncLifetime
{
    /// <summary>The engine's optional-capability profile; drives capability-gated scenarios.</summary>
    public abstract EngineCapabilities Capabilities { get; }

    /// <summary>True when the engine could be configured (e.g. its connection string is present). When false the
    /// conformance scenarios skip with <see cref="UnavailableReason"/>.</summary>
    public bool Available { get; private set; }

    public string? UnavailableReason { get; private set; }

    public IServiceProvider Services { get; private set; } = default!;

    public ConformanceProbe Probe { get; } = new();

    public ConformanceUserNameResolver UserResolver { get; } = new();

    /// <summary>Every <see cref="JobProgressPushNotification"/> the engine pushed during the run (progress capture).</summary>
    public ConcurrentBag<JobProgressPushNotification> ProgressNotifications { get; } = [];

    // ProgressNotifications in arrival order, which a bag does not keep.
    private readonly ConcurrentQueue<JobProgressPushNotification> _progressUpdates = new();

    private static readonly TimeSpan _pollInterval = TimeSpan.FromMilliseconds(25);

    /// <summary>
    /// The engine's global retry count; a job gets <c>RetryBudget + 1</c> attempts. Scenarios state expectations from
    /// it, never as a literal.
    /// </summary>
    public virtual int RetryBudget => 3;

    /// <summary>A queue no worker of the engine under test drains.</summary>
    public virtual string UnservedQueue => "conformance-unserved";

    /// <summary>The sends to one notification, in arrival order.</summary>
    public IReadOnlyList<JobProgressPushNotification> ProgressUpdates(string notificationId)
        => _progressUpdates.Where(x => x.Id == notificationId).ToList();

    /// <summary>The latest send whose title is <paramref name="title"/>, or null when none arrived.</summary>
    public JobProgressPushNotification? LastProgressUpdate(string title)
        => _progressUpdates.LastOrDefault(x => x.Title == title);

    /// <summary>
    /// Waits for the first send to <paramref name="notificationId"/> matching <paramref name="predicate"/>.
    /// </summary>
    public async Task<JobProgressPushNotification> WaitForProgressAsync(
        string notificationId,
        Func<JobProgressPushNotification, bool> predicate,
        TimeSpan timeout,
        [CallerArgumentExpression(nameof(predicate))] string predicateText = "")
    {
        var stopwatch = Stopwatch.StartNew();
        while (true)
        {
            var updates = ProgressUpdates(notificationId);
            var match = updates.FirstOrDefault(predicate);
            if (match is not null)
            {
                return match;
            }

            if (stopwatch.Elapsed >= timeout)
            {
                var seen = updates.Count == 0
                    ? "none"
                    : string.Join("; ", updates.Select(x =>
                        $"[attempt {x.Attempt}/{x.TotalAttempts}, outcome {x.Outcome ?? "-"}, finished {(x.Finished.HasValue ? "yes" : "no")}, \"{x.Description}\"]"));
                throw new TimeoutException(
                    $"Timed out after {timeout} waiting for a push update of notification '{notificationId}' matching `{predicateText}`. Updates seen: {seen}.");
            }

            await Task.Delay(_pollInterval, TestContext.Current.CancellationToken);
        }
    }

    /// <summary>
    /// Waits until the engine's <c>GetStatus</c> for <paramref name="jobId"/> satisfies <paramref name="predicate"/>.
    /// </summary>
    public async Task<Job> WaitForStatusAsync(
        string jobId,
        Func<Job, bool> predicate,
        TimeSpan timeout,
        [CallerArgumentExpression(nameof(predicate))] string predicateText = "")
    {
        var engine = Services.GetRequiredService<IJobEngine>();
        var stopwatch = Stopwatch.StartNew();
        var lastState = "unknown (null)";
        while (true)
        {
            var status = await engine.GetStatus(jobId, TestContext.Current.CancellationToken);
            if (status is not null)
            {
                if (predicate(status))
                {
                    return status;
                }

                lastState = status.State ?? "(no state)";
            }

            if (stopwatch.Elapsed >= timeout)
            {
                throw new TimeoutException(
                    $"Timed out after {timeout} waiting for the status of job '{jobId}' to match `{predicateText}`. Last state read: {lastState}.");
            }

            await Task.Delay(_pollInterval, TestContext.Current.CancellationToken);
        }
    }

    /// <summary>Register the engine under test and its worker host. Return false (with a reason) when the engine
    /// cannot run here — e.g. its connection env var is not set — so the suite skips instead of failing.</summary>
    protected abstract bool TryConfigureEngine(IServiceCollection services, out string? unavailableReason);

    protected virtual Task StartWorkerAsync(IServiceProvider services, CancellationToken cancellationToken)
        => StartHostedServicesAsync(services, cancellationToken);

    protected virtual Task StopWorkerAsync(IServiceProvider services)
        => StopHostedServicesAsync(services);

    public async ValueTask InitializeAsync()
    {
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.SetMinimumLevel(LogLevel.Warning));

        // Engine-agnostic services — the exact set the module's PlatformStartup.ConfigureServices registers.
        services.AddSingleton<IJobPayloadSerializer, JsonJobPayloadSerializer>();
        services.AddSingleton<IJobDispatcher, DefaultJobDispatcher>();
        services.AddScoped<IBackgroundJob, JobEngineBackgroundJob>();
        services.Configure<BackgroundJobsOptions>(options =>
        {
            options.DefaultQueue = "default";
            options.MaxRetryAttempts = RetryBudget;
        });
        services.AddMapReduce();
        services.AddJobCancellationStore();

        // Conformance test doubles / capture.
        services.AddSingleton(Probe);
        services.AddSingleton<IUserNameResolver>(UserResolver);
        services.AddSingleton(CreatePushNotificationManager());

        // The handlers the scenarios exercise.
        services.AddBackgroundJob<RecordingConformanceHandler>();
        services.AddMapReduceJob<ConformanceMapHandler, ConformanceReduceHandler>();

        // Used by the payload-fidelity scenario to prove the concrete (derived) type round-trips.
        if (AbstractTypeFactory<ConformancePayload>.AllTypeInfos.All(t => t.Type != typeof(ExtendedConformancePayload)))
        {
            AbstractTypeFactory<ConformancePayload>.RegisterType<ExtendedConformancePayload>();
        }

        Available = TryConfigureEngine(services, out var reason);
        UnavailableReason = reason;
        if (!Available)
        {
            return;
        }

        Services = services.BuildServiceProvider();
        await StartWorkerAsync(Services, CancellationToken.None);
    }

    public async ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);

        if (!Available)
        {
            return;
        }

        await StopWorkerAsync(Services);

        // Dispose the container asynchronously: it may hold IAsyncDisposable-only singletons (e.g. the RabbitMQ
        // connection provider). Calling the synchronous ServiceProvider.Dispose() on such a container throws.
        switch (Services)
        {
            case IAsyncDisposable asyncDisposable:
                await asyncDisposable.DisposeAsync();
                break;
            case IDisposable disposable:
                disposable.Dispose();
                break;
        }
    }

    protected static async Task StartHostedServicesAsync(IServiceProvider services, CancellationToken cancellationToken)
    {
        foreach (var hostedService in services.GetServices<IHostedService>())
        {
            await hostedService.StartAsync(cancellationToken);
        }
    }

    protected static async Task StopHostedServicesAsync(IServiceProvider services)
    {
        foreach (var hostedService in services.GetServices<IHostedService>().Reverse())
        {
            await hostedService.StopAsync(CancellationToken.None);
        }
    }

    private IPushNotificationManager CreatePushNotificationManager()
    {
        var mock = new Mock<IPushNotificationManager>();
        mock.Setup(x => x.SendAsync(It.IsAny<PushNotification>()))
            .Callback<PushNotification>(Capture)
            .Returns(Task.CompletedTask);
        mock.Setup(x => x.Send(It.IsAny<PushNotification>()))
            .Callback<PushNotification>(Capture);
        return mock.Object;

        void Capture(PushNotification notification)
        {
            if (notification is JobProgressPushNotification progress)
            {
                ProgressNotifications.Add(progress);
                _progressUpdates.Enqueue(progress);
            }
        }
    }
}
