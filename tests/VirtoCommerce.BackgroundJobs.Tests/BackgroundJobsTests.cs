using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Moq;
using VirtoCommerce.BackgroundJobs.Core;
using VirtoCommerce.BackgroundJobs.Core.Models;
using VirtoCommerce.BackgroundJobs.Core.Services;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.PushNotifications;
using VirtoCommerce.Platform.Core.Security;
using Xunit;

namespace VirtoCommerce.BackgroundJobs.Tests;

public class BackgroundJobsTests
{
    public class TestPayload
    {
        public string Value { get; set; }
    }

    private sealed class RecordingHandler : IBackgroundJobHandler<TestPayload>
    {
        public TestPayload Received { get; private set; }

        public Task Execute(TestPayload payload, IJobExecutionContext context, CancellationToken cancellationToken = default)
        {
            Received = payload;
            return Task.CompletedTask;
        }
    }

    [Fact]
    public void Serializer_RoundTrips_Payload()
    {
        var serializer = new JsonJobPayloadSerializer();

        var (payloadType, payloadJson) = serializer.Serialize(new TestPayload { Value = "abc" });

        Assert.Equal(typeof(TestPayload).AssemblyQualifiedName, payloadType);

        var restored = Assert.IsType<TestPayload>(serializer.Deserialize(payloadType, payloadJson));
        Assert.Equal("abc", restored.Value);
    }

    [Fact]
    public async Task Dispatcher_Resolves_And_Invokes_Handler()
    {
        var handler = new RecordingHandler();
        var services = new ServiceCollection();
        services.AddSingleton<IBackgroundJobHandler<TestPayload>>(handler);
        using var provider = services.BuildServiceProvider();

        var serializer = new JsonJobPayloadSerializer();
        var (payloadType, payloadJson) = serializer.Serialize(new TestPayload { Value = "from-dispatch" });
        var envelope = new JobEnvelope
        {
            JobType = typeof(TestPayload).AssemblyQualifiedName!,
            PayloadType = payloadType,
            PayloadJson = payloadJson,
        };
        var context = new JobExecutionContext("job-1", NoOpJobProgress.Instance, new Dictionary<string, string>());

        var dispatcher = new DefaultJobDispatcher(provider, serializer);
        await dispatcher.Dispatch(envelope, context, TestContext.Current.CancellationToken);

        Assert.NotNull(handler.Received);
        Assert.Equal("from-dispatch", handler.Received.Value);
    }

    [Fact]
    public async Task EnqueuePayload_Builds_Envelope_And_Delegates_To_Engine()
    {
        JobEnvelope captured = null;
        var engine = new Mock<IJobEngine>();
        engine.SetupGet(x => x.ProviderName).Returns("Test");
        engine.Setup(x => x.Enqueue(It.IsAny<JobEnvelope>(), It.IsAny<EnqueueOptions>(), It.IsAny<CancellationToken>()))
            .Callback<JobEnvelope, EnqueueOptions, CancellationToken>((env, _, _) => captured = env)
            .ReturnsAsync("job-42");

        var userResolver = new Mock<IUserNameResolver>();
        userResolver.Setup(x => x.GetCurrentUserName()).Returns("tester");

        var sut = new JobEngineBackgroundJob(
            engine.Object,
            new JsonJobPayloadSerializer(),
            Options.Create(new BackgroundJobsOptions { DefaultQueue = "default" }),
            Mock.Of<IPushNotificationManager>(),
            userResolver.Object);

        var jobId = await sut.Enqueue(new TestPayload { Value = "hi" }, cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal("job-42", jobId);
        Assert.NotNull(captured);
        Assert.Equal(typeof(TestPayload).AssemblyQualifiedName, captured.JobType);
        Assert.Equal(typeof(TestPayload).AssemblyQualifiedName, captured.PayloadType);
        Assert.Equal("default", captured.Queue);
        Assert.Equal("tester", captured.UserName);
        Assert.Contains("hi", captured.PayloadJson);
    }

    [Fact]
    public void Expression_Enqueue_Throws_When_Engine_Is_Not_ExpressionCapable()
    {
        var engine = new Mock<IJobEngine>();
        engine.SetupGet(x => x.ProviderName).Returns("RabbitMQ");

        var sut = new JobEngineBackgroundJob(
            engine.Object,
            new JsonJobPayloadSerializer(),
            Options.Create(new BackgroundJobsOptions()),
            Mock.Of<IPushNotificationManager>(),
            Mock.Of<IUserNameResolver>());

        Assert.Throws<NotSupportedException>(() => sut.Enqueue(() => Noop()));
    }

    private static void Noop()
    {
    }
}
