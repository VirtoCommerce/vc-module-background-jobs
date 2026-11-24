using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using VirtoCommerce.BackgroundJobs;
using VirtoCommerce.BackgroundJobs.Core.Models;
using VirtoCommerce.BackgroundJobs.Data.Services;
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

    // Records what the dispatcher tells the job's progress channel; implements the completion seam like the push reporter.
    private sealed class RecordingProgress : IJobProgress, IJobProgressComplete
    {
        public List<JobProgressInfo> Reports { get; } = [];
        public List<(string Outcome, string Error)> Completions { get; } = [];

        public Task Report(JobProgressInfo progress, CancellationToken cancellationToken = default)
        {
            Reports.Add(progress);
            return Task.CompletedTask;
        }

        public Task Complete(string outcome, string error)
        {
            Completions.Add((outcome, error));
            return Task.CompletedTask;
        }
    }

    private sealed class HandlerBehavior
    {
        public Func<CancellationToken, Task> Run { get; set; } = _ => Task.CompletedTask;
    }

    private sealed class ConfigurableHandler(HandlerBehavior behavior) : IBackgroundJobHandler<UserPayload>
    {
        public Task Execute(UserPayload payload, IJobExecutionContext context, CancellationToken cancellationToken = default)
            => behavior.Run(cancellationToken);
    }

    private static JobEnvelope CreateEnvelope(JsonJobPayloadSerializer serializer, int attempt, int? totalAttempts, string payloadJson = null)
    {
        var (payloadType, json) = serializer.Serialize(new UserPayload { Value = "x" });

        return new JobEnvelope
        {
            JobType = typeof(UserPayload).AssemblyQualifiedName!,
            PayloadType = payloadType,
            PayloadJson = payloadJson ?? json,
            Attempt = attempt,
            TotalAttempts = totalAttempts,
        };
    }

    private static async Task<(RecordingProgress Progress, Exception Thrown)> Dispatch(
        JobEnvelope envelope, JsonJobPayloadSerializer serializer, HandlerBehavior behavior, CancellationToken cancellationToken)
    {
        var services = new ServiceCollection();
        services.AddSingleton(behavior);
        services.AddTransient<IBackgroundJobHandler<UserPayload>, ConfigurableHandler>();
        using var provider = services.BuildServiceProvider();

        var progress = new RecordingProgress();
        var context = new JobExecutionContext("job-1", progress, new Dictionary<string, string>());
        var dispatcher = new DefaultJobDispatcher(provider, serializer);

        Exception thrown = null;
        try
        {
            await dispatcher.Dispatch(envelope, context, cancellationToken);
        }
        catch (Exception ex)
        {
            thrown = ex;
        }

        return (progress, thrown);
    }

    [Fact]
    public async Task Dispatch_UndeserializablePayload_FinalAttempt_ClosesFailed()
    {
        var serializer = new JsonJobPayloadSerializer();
        var envelope = CreateEnvelope(serializer, attempt: 1, totalAttempts: 1, payloadJson: "{");

        var (progress, thrown) = await Dispatch(envelope, serializer, new HandlerBehavior(), TestContext.Current.CancellationToken);

        Assert.NotNull(thrown);
        var completion = Assert.Single(progress.Completions);
        Assert.Equal(JobOutcome.Failed, completion.Outcome);
        Assert.False(string.IsNullOrEmpty(completion.Error));
    }

    [Fact]
    public async Task Dispatch_UndeserializablePayload_RetryPending_StaysOpen()
    {
        var serializer = new JsonJobPayloadSerializer();
        var envelope = CreateEnvelope(serializer, attempt: 1, totalAttempts: 3, payloadJson: "{");

        var (progress, thrown) = await Dispatch(envelope, serializer, new HandlerBehavior(), TestContext.Current.CancellationToken);

        Assert.NotNull(thrown);
        Assert.Empty(progress.Completions);
        var report = Assert.Single(progress.Reports);
        Assert.StartsWith("Attempt 1 of 3 failed, retrying: ", report.Message);
    }

    [Fact]
    public async Task Dispatch_OperationCanceledWithoutCancelledToken_FinalAttempt_ClosesFailed()
    {
        var serializer = new JsonJobPayloadSerializer();
        var envelope = CreateEnvelope(serializer, attempt: 1, totalAttempts: 1);
        var behavior = new HandlerBehavior { Run = _ => throw new OperationCanceledException("handler timeout") };

        var (progress, thrown) = await Dispatch(envelope, serializer, behavior, TestContext.Current.CancellationToken);

        Assert.IsType<OperationCanceledException>(thrown);
        var completion = Assert.Single(progress.Completions);
        Assert.Equal(JobOutcome.Failed, completion.Outcome);
        Assert.Equal("handler timeout", completion.Error);
    }

    [Fact]
    public async Task Dispatch_CancelledToken_DoesNotComplete()
    {
        var serializer = new JsonJobPayloadSerializer();
        var envelope = CreateEnvelope(serializer, attempt: 1, totalAttempts: 1);
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        var behavior = new HandlerBehavior { Run = ct => throw new OperationCanceledException(ct) };

        var (progress, thrown) = await Dispatch(envelope, serializer, behavior, cts.Token);

        Assert.IsType<OperationCanceledException>(thrown);
        Assert.Empty(progress.Completions);
        Assert.Empty(progress.Reports);
    }
}
