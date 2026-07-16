using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Moq;
using VirtoCommerce.BackgroundJobs.Conformance;
using VirtoCommerce.BackgroundJobs.Core;
using VirtoCommerce.BackgroundJobs.Core.Notifications;
using VirtoCommerce.BackgroundJobs.Core.Services;
using VirtoCommerce.BackgroundJobs.Data.Services;
using VirtoCommerce.BackgroundJobs.Data.MapReduce;
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
            options.MaxRetryAttempts = 3;
        });
        services.AddMapReduce();

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
            }
        }
    }
}
