using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Moq;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;
using VirtoCommerce.BackgroundJobs.Core;
using VirtoCommerce.BackgroundJobs.Core.Models;
using VirtoCommerce.BackgroundJobs;
using VirtoCommerce.BackgroundJobs.Data.Services;
using VirtoCommerce.Platform.Core.Common;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.JsonConverters;
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

    private sealed class NotAHandler
    {
    }

    private sealed class OtherPayload
    {
    }

    // Two distinct handlers for the SAME payload type — used to prove handler-explicit enqueue/dispatch.
    private sealed class HandlerRecorder
    {
        public List<string> Ran { get; } = [];
    }

    private sealed class FirstHandler(HandlerRecorder recorder) : IBackgroundJobHandler<TestPayload>
    {
        public Task Execute(TestPayload payload, IJobExecutionContext context, CancellationToken cancellationToken = default)
        {
            recorder.Ran.Add($"first:{payload.Value}");
            return Task.CompletedTask;
        }
    }

    private sealed class SecondHandler(HandlerRecorder recorder) : IBackgroundJobHandler<TestPayload>
    {
        public Task Execute(TestPayload payload, IJobExecutionContext context, CancellationToken cancellationToken = default)
        {
            recorder.Ran.Add($"second:{payload.Value}");
            return Task.CompletedTask;
        }
    }

    // Stand-in engine: dispatches synchronously on enqueue (like a worker) so a test exercises the full
    // facade -> envelope -> dispatcher -> handler chain end to end.
    private sealed class InlineEngine(IServiceProvider serviceProvider) : IJobEngine
    {
        public string ProviderName => "Inline";

        public async Task<string> Enqueue(JobEnvelope envelope, EnqueueOptions options, CancellationToken cancellationToken = default)
        {
            var dispatcher = serviceProvider.GetRequiredService<IJobDispatcher>();
            var context = new JobExecutionContext("job", NoOpJobProgress.Instance, envelope.Headers);
            await dispatcher.Dispatch(envelope, context, cancellationToken);
            return "job-1";
        }

        public Task<Job> GetStatus(string jobId, CancellationToken cancellationToken = default) => Task.FromResult<Job>(null);
        public Task<bool> Delete(string jobId, CancellationToken cancellationToken = default) => Task.FromResult(false);
    }

    [Fact]
    public void AddBackgroundJob_Infers_Payload_From_Handler()
    {
        var services = new ServiceCollection();

        services.AddBackgroundJob<RecordingHandler>();

        using var provider = services.BuildServiceProvider();
        Assert.IsType<RecordingHandler>(provider.GetService<IBackgroundJobHandler<TestPayload>>());
    }

    [Fact]
    public void AddBackgroundJob_Throws_When_Handler_Does_Not_Implement_Interface()
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentException>(() => services.AddBackgroundJob<NotAHandler>());
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

    public abstract class PolymorphicQuery
    {
        public string Kind => GetType().Name;
        public string Keyword { get; set; }
    }

    public class ConcreteQuery : PolymorphicQuery
    {
        public string[] Currencies { get; set; }
    }

    public class PayloadWithAbstractMember
    {
        public PolymorphicQuery Query { get; set; }
    }

    // A payload may nest an abstract type the API accepted through the platform's PolymorphJsonConverter
    // (the generic export's ExportDataQuery); the job must be able to read its own payload back.
    [Fact]
    public void Serializer_RoundTrips_Nested_Polymorphic_Type()
    {
        PolymorphJsonConverter.RegisterTypeForDiscriminator(typeof(PolymorphicQuery), nameof(PolymorphicQuery.Kind));
        AbstractTypeFactory<PolymorphicQuery>.RegisterType<ConcreteQuery>();
        var serializer = new JsonJobPayloadSerializer();

        var (payloadType, payloadJson) = serializer.Serialize(new PayloadWithAbstractMember
        {
            Query = new ConcreteQuery { Keyword = "k", Currencies = ["USD"] },
        });

        var restored = Assert.IsType<PayloadWithAbstractMember>(serializer.Deserialize(payloadType, payloadJson));
        var query = Assert.IsType<ConcreteQuery>(restored.Query);
        Assert.Equal("k", query.Keyword);
        Assert.Equal(["USD"], query.Currencies);
    }

    // Nulls are kept in payloads, so an unset abstract member (the export's ProviderConfig) reaches the converter as a null token.
    [Fact]
    public void Serializer_RoundTrips_Null_Polymorphic_Member()
    {
        AbstractTypeFactory<PolymorphicQuery>.RegisterType<ConcreteQuery>();
        var serializer = new JsonJobPayloadSerializer();

        var (payloadType, payloadJson) = serializer.Serialize(new PayloadWithAbstractMember());

        var restored = Assert.IsType<PayloadWithAbstractMember>(serializer.Deserialize(payloadType, payloadJson));
        Assert.Null(restored.Query);
    }

    public interface IShape
    {
        string Name { get; set; }
    }

    public class Square : IShape
    {
        public string Name { get; set; }
    }

    public class PayloadWithInterfaceMembers
    {
        public IShape[] Shapes { get; set; }

        public IShape Unset { get; set; }
    }

    // What a module registers on the host's MVC settings (e.g. the order module's IOperation converter, built on
    // PolymorphJsonConverter).
    private sealed class ShapeConverter : PolymorphJsonConverter
    {
        public override bool CanConvert(Type objectType) => objectType == typeof(IShape);

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            var square = new Square();
            serializer.Populate(JObject.Load(reader).CreateReader(), square);
            return square;
        }
    }

    // Any other host converter: writes its own shape and maps a null token to a value of its own.
    private sealed class NameConverter : JsonConverter
    {
        public override bool CanConvert(Type objectType) => objectType == typeof(string);

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer) =>
            reader.TokenType == JsonToken.Null ? "(none)" : (string)reader.Value;

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer) => writer.WriteValue($"name:{value}");
    }

    // Payloads are written with the converters they are read with.
    [Fact]
    public void Serializer_Writes_With_Host_Converters_And_Leaves_Their_Null_Handling()
    {
        var serializer = new JsonJobPayloadSerializer([new NameConverter()]);

        var (payloadType, payloadJson) = serializer.Serialize(new TestPayload { Value = "x" });
        var (_, nullJson) = serializer.Serialize(new TestPayload());

        Assert.Contains("name:x", payloadJson);
        Assert.Equal("(none)", Assert.IsType<TestPayload>(serializer.Deserialize(payloadType, nullJson)).Value);
    }

    public enum Status
    {
        None,
        Paid,
    }

    public class PayloadWithStatus
    {
        public Status Status { get; set; }

        public Dictionary<string, string> Labels { get; set; }
    }

    // The host converters read the API's JSON: enums by name (the order module's IOperation converter turns an unknown
    // PaymentStatus into Custom) and camelCase (ModuleIdentityJsonConverter reads "major"). Dictionary keys are kept.
    [Fact]
    public void Serializer_Writes_The_Wire_Format_The_Host_Converters_Read()
    {
        var serializer = new JsonJobPayloadSerializer([new StringEnumConverter()]);

        var (payloadType, payloadJson) = serializer.Serialize(new PayloadWithStatus { Status = Status.Paid, Labels = new() { ["Key"] = "v" } });

        Assert.Contains("\"status\":\"Paid\"", payloadJson);
        Assert.Contains("\"Key\":\"v\"", payloadJson);
        Assert.Equal(Status.Paid, Assert.IsType<PayloadWithStatus>(serializer.Deserialize(payloadType, payloadJson)).Status);
    }

    // Payloads enqueued before (PascalCase, enums as numbers) stay readable.
    [Fact]
    public void Serializer_Reads_Payloads_Written_In_The_Earlier_Format()
    {
        var serializer = new JsonJobPayloadSerializer([new StringEnumConverter()]);

        var restored = Assert.IsType<PayloadWithStatus>(serializer.Deserialize(
            typeof(PayloadWithStatus).AssemblyQualifiedName!, "{\"Status\":1,\"Labels\":{\"Key\":\"v\"}}"));

        Assert.Equal(Status.Paid, restored.Status);
        Assert.Equal("v", restored.Labels["Key"]);
    }

    // A job processed before startup ends must not wait for it; it uses the converters registered so far.
    [Fact]
    public void Serializer_Does_Not_Wait_For_Startup_And_Picks_Up_Converters_Added_Meanwhile()
    {
        var lifetime = new Mock<IHostApplicationLifetime>();
        lifetime.Setup(x => x.ApplicationStarted).Returns(CancellationToken.None); // never starts
        var hostConverters = new List<JsonConverter>();
        var serializer = new JsonJobPayloadSerializer(hostConverters, lifetime.Object);
        var (payloadType, payloadJson) = serializer.Serialize(new PayloadWithInterfaceMembers { Shapes = [new Square { Name = "s" }] });

        Assert.Throws<JsonSerializationException>(() => serializer.Deserialize(payloadType, payloadJson));
        hostConverters.Add(new ShapeConverter()); // a module registers its converter later in startup

        Assert.IsType<PayloadWithInterfaceMembers>(serializer.Deserialize(payloadType, payloadJson));
    }

    // A payload can nest interface types only the host's converters can read (an order's IOperation children): the
    // serializer must use them, and must not hand them null tokens.
    [Fact]
    public void Serializer_RoundTrips_Interface_Members_With_Host_Converters()
    {
        var serializer = new JsonJobPayloadSerializer([new ShapeConverter()]);

        var (payloadType, payloadJson) = serializer.Serialize(new PayloadWithInterfaceMembers { Shapes = [new Square { Name = "s" }] });

        var restored = Assert.IsType<PayloadWithInterfaceMembers>(serializer.Deserialize(payloadType, payloadJson));
        Assert.Equal("s", Assert.IsType<Square>(Assert.Single(restored.Shapes)).Name);
        Assert.Null(restored.Unset);
        Assert.Throws<JsonSerializationException>(() => new JsonJobPayloadSerializer().Deserialize(payloadType, payloadJson));
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
            new JsonJobPayloadSerializer(),
            Options.Create(new BackgroundJobsOptions { DefaultQueue = "default" }),
            Mock.Of<IPushNotificationManager>(),
            userResolver.Object,
            engine.Object);

        var jobId = await sut.Enqueue<RecordingHandler>(new TestPayload { Value = "hi" }, cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal("job-42", jobId);
        Assert.NotNull(captured);
        Assert.Equal(typeof(TestPayload).AssemblyQualifiedName, captured.JobType);
        Assert.Equal(typeof(TestPayload).AssemblyQualifiedName, captured.PayloadType);
        Assert.Equal("default", captured.Queue);
        Assert.Equal("tester", captured.UserName);
        Assert.Contains("hi", captured.PayloadJson);
    }

    [Fact]
    public async Task Enqueue_Throws_BackgroundJobEngineNotInstalled_When_No_Engine()
    {
        // The facade is always registered; with no engine installed (IJobEngine == null) enqueue must throw the
        // actionable BackgroundJobEngineNotInstalledException instead of failing DI resolution.
        var sut = new JobEngineBackgroundJob(
            new JsonJobPayloadSerializer(),
            Options.Create(new BackgroundJobsOptions()),
            Mock.Of<IPushNotificationManager>(),
            Mock.Of<IUserNameResolver>(),
            engine: null);

        await Assert.ThrowsAsync<BackgroundJobEngineNotInstalledException>(
            () => sut.Enqueue<RecordingHandler>(new TestPayload { Value = "x" }, cancellationToken: TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task EnqueueWithHandler_Sets_HandlerType_And_Payload_Contract_On_Envelope()
    {
        JobEnvelope captured = null;
        var engine = new Mock<IJobEngine>();
        engine.SetupGet(x => x.ProviderName).Returns("Test");
        engine.Setup(x => x.Enqueue(It.IsAny<JobEnvelope>(), It.IsAny<EnqueueOptions>(), It.IsAny<CancellationToken>()))
            .Callback<JobEnvelope, EnqueueOptions, CancellationToken>((env, _, _) => captured = env)
            .ReturnsAsync("job-7");

        var sut = new JobEngineBackgroundJob(
            new JsonJobPayloadSerializer(),
            Options.Create(new BackgroundJobsOptions { DefaultQueue = "default" }),
            Mock.Of<IPushNotificationManager>(),
            Mock.Of<IUserNameResolver>(),
            engine.Object);

        var jobId = await sut.Enqueue<RecordingHandler>(new TestPayload { Value = "hi" }, cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal("job-7", jobId);
        Assert.NotNull(captured);
        Assert.Equal(typeof(RecordingHandler).AssemblyQualifiedName, captured.HandlerType);
        Assert.Equal(typeof(TestPayload).AssemblyQualifiedName, captured.JobType); // the handler's payload contract
    }

    [Fact]
    public async Task EnqueueWithHandler_Throws_When_Handler_Does_Not_Handle_Payload()
    {
        var engine = new Mock<IJobEngine>();
        engine.SetupGet(x => x.ProviderName).Returns("Test");

        var sut = new JobEngineBackgroundJob(
            new JsonJobPayloadSerializer(),
            Options.Create(new BackgroundJobsOptions()),
            Mock.Of<IPushNotificationManager>(),
            Mock.Of<IUserNameResolver>(),
            engine.Object);

        // RecordingHandler handles TestPayload, not OtherPayload — enqueue must fail fast with an actionable error.
        await Assert.ThrowsAsync<ArgumentException>(
            () => sut.Enqueue<RecordingHandler>(new OtherPayload(), cancellationToken: TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Dispatcher_Resolves_Explicit_Handler_When_Multiple_Share_A_Payload()
    {
        // Requirement: one payload type can drive several handlers; the explicit handler on the envelope selects which.
        var recorder = new HandlerRecorder();
        var services = new ServiceCollection();
        services.AddSingleton(recorder);
        services.AddBackgroundJob<FirstHandler, TestPayload>();
        services.AddBackgroundJob<SecondHandler, TestPayload>();
        using var provider = services.BuildServiceProvider();

        var serializer = new JsonJobPayloadSerializer();
        var (payloadType, payloadJson) = serializer.Serialize(new TestPayload { Value = "shared" });
        var dispatcher = new DefaultJobDispatcher(provider, serializer);

        JobEnvelope EnvelopeFor(Type handler) => new()
        {
            JobType = typeof(TestPayload).AssemblyQualifiedName!,
            HandlerType = handler.AssemblyQualifiedName!,
            PayloadType = payloadType,
            PayloadJson = payloadJson,
        };
        var context = new JobExecutionContext("job-1", NoOpJobProgress.Instance, new Dictionary<string, string>());

        await dispatcher.Dispatch(EnvelopeFor(typeof(SecondHandler)), context, TestContext.Current.CancellationToken);
        await dispatcher.Dispatch(EnvelopeFor(typeof(FirstHandler)), context, TestContext.Current.CancellationToken);

        // Each explicit dispatch ran exactly its named handler, against the same payload type.
        Assert.Equal(["second:shared", "first:shared"], recorder.Ran);
    }

    [Fact]
    public async Task Same_Payload_Runs_On_Two_Handlers_When_Enqueued_To_Each()
    {
        // End-to-end through the public facade: one payload TYPE, two handlers, enqueued to each by name.
        var recorder = new HandlerRecorder();
        var services = new ServiceCollection();
        services.AddSingleton(recorder);
        services.AddSingleton<IJobPayloadSerializer, JsonJobPayloadSerializer>();
        services.AddSingleton<IJobDispatcher, DefaultJobDispatcher>();
        services.AddSingleton<IJobEngine, InlineEngine>();
        services.AddScoped<IBackgroundJob, JobEngineBackgroundJob>();
        services.Configure<BackgroundJobsOptions>(_ => { });
        services.AddSingleton(Mock.Of<IUserNameResolver>());
        services.AddSingleton(Mock.Of<IPushNotificationManager>());
        services.AddBackgroundJob<FirstHandler, TestPayload>();
        services.AddBackgroundJob<SecondHandler, TestPayload>();

        using var provider = services.BuildServiceProvider();
        var jobs = provider.GetRequiredService<IBackgroundJob>();

        var payload = new TestPayload { Value = "shared" };
        await jobs.Enqueue<FirstHandler>(payload, cancellationToken: TestContext.Current.CancellationToken);
        await jobs.Enqueue<SecondHandler>(payload, cancellationToken: TestContext.Current.CancellationToken);

        // The same payload ran on BOTH handlers, each selected by the handler named at enqueue time.
        Assert.Equal(["first:shared", "second:shared"], recorder.Ran);
    }
}
