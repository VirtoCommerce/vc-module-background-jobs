using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using VirtoCommerce.BackgroundJobs;
using VirtoCommerce.BackgroundJobs.Core;
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

    // Records what the dispatcher tells the progress channel.
    private sealed class RecordingProgress : IJobProgress
    {
        public List<JobProgressInfo> Reports { get; } = [];
        public List<(JobOutcome Outcome, string Error)> Completions { get; } = [];

        // Sends that fail the way a down push channel does; the attempt is still recorded before it throws.
        public Func<int, bool> ThrowOnReport { get; set; } = _ => false;
        public Func<JobOutcome, bool> ThrowOnComplete { get; set; } = _ => false;

        public Task Report(JobProgressInfo progress, CancellationToken cancellationToken = default)
        {
            Reports.Add(progress);

            return ThrowOnReport(Reports.Count) ? throw new InvalidOperationException("push channel down") : Task.CompletedTask;
        }

        public Task Complete(JobOutcome outcome, string error = null, CancellationToken cancellationToken = default)
        {
            Completions.Add((outcome, error));

            return ThrowOnComplete(outcome) ? throw new InvalidOperationException("push channel down") : Task.CompletedTask;
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

    private static JobEnvelope CreateEnvelope(JsonJobPayloadSerializer serializer, int attempt, int? totalAttempts, string payloadJson = null, bool completesProgressNotification = false)
    {
        var (payloadType, json) = serializer.Serialize(new UserPayload { Value = "x" });

        return new JobEnvelope
        {
            JobType = typeof(UserPayload).AssemblyQualifiedName!,
            PayloadType = payloadType,
            PayloadJson = payloadJson ?? json,
            Attempt = attempt,
            TotalAttempts = totalAttempts,
            CompletesProgressNotification = completesProgressNotification,
        };
    }

    private static async Task<(RecordingProgress Progress, Exception Thrown)> Dispatch(
        JobEnvelope envelope, JsonJobPayloadSerializer serializer, HandlerBehavior behavior, CancellationToken cancellationToken,
        RecordingProgress progress = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton(behavior);
        services.AddTransient<IBackgroundJobHandler<UserPayload>, ConfigurableHandler>();
        using var provider = services.BuildServiceProvider();

        progress ??= new RecordingProgress();
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
        var envelope = CreateEnvelope(serializer, attempt: 1, totalAttempts: 1, payloadJson: "{", completesProgressNotification: true);

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
        var envelope = CreateEnvelope(serializer, attempt: 1, totalAttempts: 3, payloadJson: "{", completesProgressNotification: true);

        var (progress, thrown) = await Dispatch(envelope, serializer, new HandlerBehavior(), TestContext.Current.CancellationToken);

        Assert.NotNull(thrown);
        Assert.Empty(progress.Completions);
        Assert.StartsWith("Attempt 1 of 3 failed, retrying: ", progress.Reports[^1].Message);
    }

    // An owned notification is updated when the attempt starts: the queued one carries no job id, so a silent handler would look queued all run.
    [Fact]
    public async Task Dispatch_OwnedNotification_ReportsBeforeHandlerRuns()
    {
        var reportsSeenByHandler = await CountReportsSeenByHandler(completesProgressNotification: true);

        Assert.Equal(1, reportsSeenByHandler);
    }

    [Fact]
    public async Task Dispatch_SharedNotification_DoesNotReportBeforeHandlerRuns()
    {
        var reportsSeenByHandler = await CountReportsSeenByHandler(completesProgressNotification: false);

        Assert.Equal(0, reportsSeenByHandler);
    }

    private static async Task<int> CountReportsSeenByHandler(bool completesProgressNotification)
    {
        var serializer = new JsonJobPayloadSerializer();
        var envelope = CreateEnvelope(serializer, attempt: 1, totalAttempts: 1, completesProgressNotification: completesProgressNotification);
        var progress = new RecordingProgress();
        var reportsSeenByHandler = -1;
        var behavior = new HandlerBehavior
        {
            Run = _ =>
            {
                reportsSeenByHandler = progress.Reports.Count;

                return Task.CompletedTask;
            },
        };

        var (_, thrown) = await Dispatch(envelope, serializer, behavior, TestContext.Current.CancellationToken, progress);

        Assert.Null(thrown);

        return reportsSeenByHandler;
    }

    [Fact]
    public async Task Dispatch_OperationCanceledWithoutCancelledToken_FinalAttempt_ClosesFailed()
    {
        var serializer = new JsonJobPayloadSerializer();
        var envelope = CreateEnvelope(serializer, attempt: 1, totalAttempts: 1, completesProgressNotification: true);
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

    [Fact]
    public async Task Dispatch_StartReportFails_HandlerStillRuns()
    {
        var serializer = new JsonJobPayloadSerializer();
        var envelope = CreateEnvelope(serializer, attempt: 1, totalAttempts: 1, completesProgressNotification: true);
        var progress = new RecordingProgress { ThrowOnReport = count => count == 1 };
        var ran = false;
        var behavior = new HandlerBehavior { Run = _ => { ran = true; return Task.CompletedTask; } };

        var (_, thrown) = await Dispatch(envelope, serializer, behavior, TestContext.Current.CancellationToken, progress);

        Assert.Null(thrown);
        Assert.True(ran);
    }

    [Fact]
    public async Task Dispatch_SuccessCloseFails_DoesNotFailTheAttempt()
    {
        var serializer = new JsonJobPayloadSerializer();
        var envelope = CreateEnvelope(serializer, attempt: 1, totalAttempts: 3, completesProgressNotification: true);
        var progress = new RecordingProgress { ThrowOnComplete = outcome => outcome == JobOutcome.Succeeded };

        var (_, thrown) = await Dispatch(envelope, serializer, new HandlerBehavior(), TestContext.Current.CancellationToken, progress);

        Assert.Null(thrown);
        Assert.Single(progress.Completions);
    }

    [Fact]
    public async Task Dispatch_FailureReportFails_StillThrowsTheHandlerException()
    {
        var serializer = new JsonJobPayloadSerializer();
        var envelope = CreateEnvelope(serializer, attempt: 1, totalAttempts: 1, completesProgressNotification: true);
        var progress = new RecordingProgress { ThrowOnComplete = _ => true };
        var expected = new ArgumentException("handler failed");
        var behavior = new HandlerBehavior { Run = _ => throw expected };

        var (_, thrown) = await Dispatch(envelope, serializer, behavior, TestContext.Current.CancellationToken, progress);

        Assert.Same(expected, thrown);
    }

    // The outcome reaches the progress whoever owns the notification (a wrapper must see every outcome); only attempt reports are owner-only.
    [Fact]
    public async Task Dispatch_SharedNotification_DeliversOutcomeButNotAttemptReports()
    {
        var serializer = new JsonJobPayloadSerializer();

        var succeeded = new RecordingProgress();
        var succeededEnvelope = CreateEnvelope(serializer, attempt: 1, totalAttempts: 3, completesProgressNotification: false);
        await Dispatch(succeededEnvelope, serializer, new HandlerBehavior(), TestContext.Current.CancellationToken, succeeded);

        var failedFinal = new RecordingProgress();
        var finalEnvelope = CreateEnvelope(serializer, attempt: 1, totalAttempts: 1, completesProgressNotification: false);
        var behavior = new HandlerBehavior { Run = _ => throw new InvalidOperationException("boom") };
        var (_, thrown) = await Dispatch(finalEnvelope, serializer, behavior, TestContext.Current.CancellationToken, failedFinal);

        var failedRetry = new RecordingProgress();
        var retryEnvelope = CreateEnvelope(serializer, attempt: 1, totalAttempts: 3, completesProgressNotification: false);
        await Dispatch(retryEnvelope, serializer, behavior, TestContext.Current.CancellationToken, failedRetry);

        Assert.NotNull(thrown);
        Assert.Equal(JobOutcome.Succeeded, Assert.Single(succeeded.Completions).Outcome);
        var failure = Assert.Single(failedFinal.Completions);
        Assert.Equal(JobOutcome.Failed, failure.Outcome);
        Assert.Equal("boom", failure.Error);
        Assert.Empty(failedRetry.Completions);
        Assert.Empty(succeeded.Reports);
        Assert.Empty(failedFinal.Reports);
        Assert.Empty(failedRetry.Reports);
    }

    [Fact]
    public async Task Dispatch_ResolvedFromDi_LogsFailedSendWithJobId()
    {
        var logs = new CapturingLoggerProvider();
        var serializer = new JsonJobPayloadSerializer();
        var services = new ServiceCollection();
        services.AddLogging(x => x.AddProvider(logs));
        services.AddSingleton<IJobPayloadSerializer>(serializer);
        services.AddSingleton<JobTelemetry>();
        services.AddOptions<BackgroundJobsOptions>();
        services.AddSingleton(new HandlerBehavior());
        services.AddTransient<IBackgroundJobHandler<UserPayload>, ConfigurableHandler>();
        services.AddSingleton<IJobDispatcher, DefaultJobDispatcher>();
        using var provider = services.BuildServiceProvider();

        var envelope = CreateEnvelope(serializer, attempt: 1, totalAttempts: 1, completesProgressNotification: true);
        var progress = new RecordingProgress { ThrowOnComplete = _ => true };
        var context = new JobExecutionContext("job-42", progress, new Dictionary<string, string>());

        await provider.GetRequiredService<IJobDispatcher>().Dispatch(envelope, context, TestContext.Current.CancellationToken);

        var entry = Assert.Single(logs.Entries);
        Assert.Equal(LogLevel.Error, entry.Level);
        Assert.Contains("job-42", entry.Message);
        Assert.Contains("UserPayload", entry.Message);
    }

    private sealed class CapturingLoggerProvider : ILoggerProvider
    {
        public List<(LogLevel Level, string Message)> Entries { get; } = [];

        public ILogger CreateLogger(string categoryName) => new CapturingLogger(Entries);

        public void Dispose()
        {
        }

        private sealed class CapturingLogger(List<(LogLevel Level, string Message)> entries) : ILogger
        {
            public IDisposable BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
                => entries.Add((logLevel, formatter(state, exception)));
        }
    }
}
