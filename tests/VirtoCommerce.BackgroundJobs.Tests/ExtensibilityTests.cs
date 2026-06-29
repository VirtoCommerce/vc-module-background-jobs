using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using VirtoCommerce.BackgroundJobs.Core;
using VirtoCommerce.BackgroundJobs.Core.Models;
using VirtoCommerce.BackgroundJobs.Core.Services;
using VirtoCommerce.Platform.Core.Common;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.PushNotifications;
using VirtoCommerce.Platform.Core.Security;
using Xunit;

namespace VirtoCommerce.BackgroundJobs.Tests;

/// <summary>
/// Proves the message-based background-job flow honors the Virto Commerce extension model end-to-end: a 3rd-party
/// module can extend the payload via <see cref="AbstractTypeFactory{T}"/>, and handler-explicit enqueue
/// (<c>jobs.Enqueue&lt;THandler&gt;(payload)</c>) routes the (possibly extended) payload to the chosen handler —
/// with the concrete extended type surviving serialization across the engine boundary.
/// </summary>
public class ExtensibilityTests
{
    // Base payload shipped by a "vendor" module.
    public class OrderEmailPayload
    {
        public string OrderId { get; set; }
    }

    // 3rd-party extension of the payload.
    public class ExtendedOrderEmailPayload : OrderEmailPayload
    {
        public string Locale { get; set; }
    }

    private sealed class Recorder
    {
        public string HandlerName;
        public OrderEmailPayload Payload;
    }

    // Vendor handler.
    private sealed class OrderEmailJob(Recorder recorder) : IBackgroundJobHandler<OrderEmailPayload>
    {
        public Task Execute(OrderEmailPayload payload, IJobExecutionContext context, CancellationToken ct = default)
        {
            recorder.HandlerName = nameof(OrderEmailJob);
            recorder.Payload = payload;
            return Task.CompletedTask;
        }
    }

    // 3rd-party handler override (same payload contract).
    private sealed class CustomOrderEmailJob(Recorder recorder) : IBackgroundJobHandler<OrderEmailPayload>
    {
        public Task Execute(OrderEmailPayload payload, IJobExecutionContext context, CancellationToken ct = default)
        {
            recorder.HandlerName = nameof(CustomOrderEmailJob);
            recorder.Payload = payload;
            return Task.CompletedTask;
        }
    }

    // Runs the enqueued job synchronously through the real dispatcher — simulating a worker, so the test exercises
    // the full enqueue -> serialize -> dispatch -> deserialize -> resolve handler -> run chain.
    private sealed class InlineJobEngine(IServiceProvider serviceProvider) : IJobEngine
    {
        public string ProviderName => "Inline";

        public async Task<string> Enqueue(JobEnvelope envelope, EnqueueOptions options, CancellationToken cancellationToken = default)
        {
            var dispatcher = serviceProvider.GetRequiredService<IJobDispatcher>();
            var context = new JobExecutionContext("test", NoOpJobProgress.Instance, envelope.Headers);
            await dispatcher.Dispatch(envelope, context, cancellationToken);
            return "job-1";
        }

        public Task<Job> GetStatus(string jobId, CancellationToken cancellationToken = default) => Task.FromResult<Job>(null);
        public Task<bool> Delete(string jobId, CancellationToken cancellationToken = default) => Task.FromResult(false);
    }

    private static (IBackgroundJob jobs, Recorder recorder) Build(Action<IServiceCollection> registerHandlers)
    {
        var services = new ServiceCollection();
        services.AddSingleton<Recorder>();
        services.AddSingleton<IJobPayloadSerializer, JsonJobPayloadSerializer>();
        services.AddSingleton<IJobDispatcher, DefaultJobDispatcher>();
        services.AddSingleton<IJobEngine, InlineJobEngine>();
        services.AddScoped<IBackgroundJob, JobEngineBackgroundJob>();
        services.Configure<BackgroundJobsOptions>(_ => { });

        var userResolver = new Mock<IUserNameResolver>();
        userResolver.Setup(x => x.GetCurrentUserName()).Returns("tester");
        services.AddSingleton(userResolver.Object);
        services.AddSingleton(Mock.Of<IPushNotificationManager>());

        registerHandlers(services);

        var provider = services.BuildServiceProvider();
        return (provider.GetRequiredService<IBackgroundJob>(), provider.GetRequiredService<Recorder>());
    }

    [Fact]
    public async Task ThirdParty_Can_Extend_Payload_Via_AbstractTypeFactory()
    {
        AbstractTypeFactory<OrderEmailPayload>.RegisterType<OrderEmailPayload>();
        AbstractTypeFactory<OrderEmailPayload>.OverrideType<OrderEmailPayload, ExtendedOrderEmailPayload>();

        // Only the vendor handler is registered; the payload is extended.
        var (jobs, recorder) = Build(services => services.AddBackgroundJob<OrderEmailPayload, OrderEmailJob>());

        // Create via the factory (returns the extended type), set base + extended fields, enqueue as the base type.
        OrderEmailPayload payload = AbstractTypeFactory<OrderEmailPayload>.TryCreateInstance();
        payload.OrderId = "ORD-2";
        ((ExtendedOrderEmailPayload)payload).Locale = "fr-FR";

        await jobs.Enqueue<OrderEmailJob>(payload, cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(nameof(OrderEmailJob), recorder.HandlerName);
        // The concrete extended type survived serialization across the engine boundary.
        var received = Assert.IsType<ExtendedOrderEmailPayload>(recorder.Payload);
        Assert.Equal("ORD-2", received.OrderId);
        Assert.Equal("fr-FR", received.Locale);
    }

    [Fact]
    public async Task ThirdParty_Can_Extend_Payload_And_Route_To_Chosen_Handler()
    {
        AbstractTypeFactory<OrderEmailPayload>.RegisterType<OrderEmailPayload>();
        AbstractTypeFactory<OrderEmailPayload>.OverrideType<OrderEmailPayload, ExtendedOrderEmailPayload>();

        var (jobs, recorder) = Build(services =>
        {
            services.AddBackgroundJob<OrderEmailPayload, OrderEmailJob>();
            services.AddBackgroundJob<OrderEmailPayload, CustomOrderEmailJob>();
        });

        OrderEmailPayload payload = AbstractTypeFactory<OrderEmailPayload>.TryCreateInstance();
        payload.OrderId = "ORD-3";
        ((ExtendedOrderEmailPayload)payload).Locale = "de-DE";

        // Enqueue explicitly to the chosen handler; the extended payload reaches it intact.
        await jobs.Enqueue<CustomOrderEmailJob>(payload, cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(nameof(CustomOrderEmailJob), recorder.HandlerName);
        var received = Assert.IsType<ExtendedOrderEmailPayload>(recorder.Payload);
        Assert.Equal("ORD-3", received.OrderId);
        Assert.Equal("de-DE", received.Locale);
    }
}
