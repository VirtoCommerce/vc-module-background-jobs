using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using VirtoCommerce.BackgroundJobs.Core.Models;
using VirtoCommerce.BackgroundJobs.Core.Services;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.Security;
using Xunit;

namespace VirtoCommerce.BackgroundJobs.Tests;

public class DispatcherTests
{
    public class UserPayload
    {
        public string Value { get; set; }
    }

    private sealed class CapturingUserResolver : IUserNameResolver
    {
        public string Current { get; private set; }
        public string GetCurrentUserName() => Current;
        public void SetCurrentUserName(string userName) => Current = userName;
    }

    private sealed class Recorder
    {
        public string UserSeenDuringExecute;
    }

    // Reads the current user inside Execute to prove the dispatcher restored it from the envelope.
    private sealed class UserRecordingHandler(IUserNameResolver userNameResolver, Recorder recorder) : IBackgroundJobHandler<UserPayload>
    {
        public Task Execute(UserPayload payload, IJobExecutionContext context, CancellationToken cancellationToken = default)
        {
            recorder.UserSeenDuringExecute = userNameResolver.GetCurrentUserName();
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task Dispatch_Restores_User_Context_From_Envelope()
    {
        var recorder = new Recorder();
        var services = new ServiceCollection();
        services.AddSingleton<IUserNameResolver, CapturingUserResolver>();
        services.AddSingleton(recorder);
        services.AddTransient<IBackgroundJobHandler<UserPayload>, UserRecordingHandler>();
        using var provider = services.BuildServiceProvider();

        var serializer = new JsonJobPayloadSerializer();
        var (payloadType, payloadJson) = serializer.Serialize(new UserPayload { Value = "x" });
        var envelope = new JobEnvelope
        {
            JobType = typeof(UserPayload).AssemblyQualifiedName!,
            PayloadType = payloadType,
            PayloadJson = payloadJson,
            UserName = "alice",
        };

        var dispatcher = new DefaultJobDispatcher(provider, serializer);
        var context = new JobExecutionContext("job-1", NoOpJobProgress.Instance, new Dictionary<string, string>());

        await dispatcher.Dispatch(envelope, context, TestContext.Current.CancellationToken);

        // The enqueuing user is restored for the handler (engine-agnostic — covers RabbitMQ / push callbacks).
        Assert.Equal("alice", recorder.UserSeenDuringExecute);
    }
}
