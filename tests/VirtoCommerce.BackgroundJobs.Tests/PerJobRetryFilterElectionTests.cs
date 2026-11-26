#nullable enable
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Threading;
using Hangfire;
using Hangfire.Common;
using Hangfire.Logging;
using Hangfire.States;
using Hangfire.Storage;
using Moq;
using Newtonsoft.Json;
using VirtoCommerce.BackgroundJobs.Core.Models;
using VirtoCommerce.BackgroundJobs.Hangfire;
using Xunit;

namespace VirtoCommerce.BackgroundJobs.Tests;

/// <summary>Tests that replace the process-wide <see cref="GlobalJobFilters"/>; run alone, since the Hangfire conformance fixture rebuilds the same static.</summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class GlobalJobFiltersCollection
{
    public const string Name = "GlobalJobFilters";
}

/// <summary>Runs <see cref="PerJobRetryFilterAttribute"/> on a real <see cref="ElectStateContext"/> after the real <see cref="AutomaticRetryAttribute"/> elected, as Hangfire's state machine does.</summary>
[Collection(GlobalJobFiltersCollection.Name)]
public sealed class PerJobRetryFilterElectionTests : IDisposable
{
    private const string _hangfireJobId = "job-1";

    private const int _globalAttempts = 3;
    private const int _jobLimit = 5;

    private readonly List<object> _originalFilters = GlobalJobFilters.Filters.Select(x => x.Instance).ToList();
    private readonly ILogProvider? _originalLogProvider = ReadLogProvider();

    public PerJobRetryFilterElectionTests()
    {
        // An earlier conformance fixture can leave Hangfire's static log provider bound to a disposed host, and AutomaticRetryAttribute takes its logger from it.
        LogProvider.SetCurrentLogProvider(new SilentLogProvider());

        foreach (var filter in _originalFilters)
        {
            GlobalJobFilters.Filters.Remove(filter);
        }
    }

    public void Dispose()
    {
        LogProvider.SetCurrentLogProvider(_originalLogProvider!);

        foreach (var filter in GlobalJobFilters.Filters.Select(x => x.Instance).ToList())
        {
            GlobalJobFilters.Filters.Remove(filter);
        }

        foreach (var filter in _originalFilters)
        {
            GlobalJobFilters.Filters.Add(filter);
        }
    }

    [Fact]
    public void Failed_ThenDeletedByGlobal_BelowJobLimit_SchedulesRetry()
    {
        var (context, connection) = Elect(retryCountStored: _globalAttempts, AttemptsExceededAction.Delete, new FailedState(new InvalidOperationException("boom")));

        Assert.IsType<ScheduledState>(context.CandidateState);
        connection.Verify(x => x.SetJobParameter(_hangfireJobId, "RetryCount", "4"), Times.Once);
    }

    [Fact]
    public void Deleted_WithoutTraversedFailure_IsNotTouched()
    {
        // Global retry registered and the count below the limit, so only the traversed-failure check keeps this untouched.
        RegisterGlobalRetry(AttemptsExceededAction.Delete);
        var deleted = new DeletedState();
        var context = CreateContext(deleted, retryCountStored: _globalAttempts, out var connection);

        new PerJobRetryFilterAttribute().OnStateElection(context);

        Assert.Same(deleted, context.CandidateState);
        Assert.Empty(context.TraversedStates);
        connection.Verify(x => x.SetJobParameter(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>()), Times.Never);
    }

    [Fact]
    public void Failed_ByGlobal_BelowJobLimit_StillSchedulesRetry()
    {
        var (context, connection) = Elect(retryCountStored: _globalAttempts, AttemptsExceededAction.Fail, new FailedState(new InvalidOperationException("boom")));

        Assert.IsType<ScheduledState>(context.CandidateState);
        connection.Verify(x => x.SetJobParameter(_hangfireJobId, "RetryCount", "4"), Times.Once);
    }

    [Fact]
    public void Scheduled_ByGlobal_PastLowerJobLimit_FailsWithTheOriginalException()
    {
        // The global filter elects a delayed retry for the second failure; the job asked for 1 retry.
        var failure = new FailedState(new InvalidOperationException("boom"));

        var (context, _) = Elect(retryCountStored: 1, AttemptsExceededAction.Fail, failure, jobLimit: 1);

        Assert.Same(failure, context.CandidateState);
    }

    // Hangfire exposes no getter for its current log provider, and the field is read only so that it can be put back.
    private static ILogProvider? ReadLogProvider()
    {
        var field = typeof(LogProvider).GetField("_currentLogProvider", BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(field);

        return (ILogProvider?)field!.GetValue(null);
    }

    private sealed class SilentLogProvider : ILogProvider
    {
        public ILog GetLogger(string name) => new Mock<ILog>().Object;
    }

    // Elects through the global filter first, so a Delete is reached as in Hangfire (Failed, then Deleted with the failure traversed), then through the per-job filter.
    private (ElectStateContext Context, Mock<IStorageConnection> Connection) Elect(int retryCountStored, AttemptsExceededAction onExceeded, FailedState failure, int jobLimit = _jobLimit)
    {
        var global = RegisterGlobalRetry(onExceeded);

        var context = CreateContext(failure, retryCountStored, out var connection, jobLimit);

        global.OnStateElection(context);
        new PerJobRetryFilterAttribute().OnStateElection(context);

        return (context, connection);
    }

    private static AutomaticRetryAttribute RegisterGlobalRetry(AttemptsExceededAction onExceeded)
    {
        var global = new AutomaticRetryAttribute
        {
            Attempts = _globalAttempts,
            DelaysInSeconds = [60],
            OnAttemptsExceeded = onExceeded,
            LogEvents = false,
        };
        GlobalJobFilters.Filters.Add(global);

        return global;
    }

    // Hangfire's public constructors build the context; only the storage collaborators are mocked.
    private static ElectStateContext CreateContext(IState candidate, int retryCountStored, out Mock<IStorageConnection> connection, int jobLimit = _jobLimit)
    {
        var envelopeJson = JsonConvert.SerializeObject(new JobEnvelope
        {
            JobType = "t",
            PayloadType = "t",
            PayloadJson = "{}",
            MaxRetryAttempts = jobLimit,
        });
        var job = Job.FromExpression<HangfireJobExecutor>(x => x.Execute(envelopeJson, null!, CancellationToken.None));

        connection = new Mock<IStorageConnection>();

        // The count is stateful, as in Hangfire: what the global filter writes for the failure it reschedules is what
        // the per-job filter then reads back.
        var stored = retryCountStored.ToString(CultureInfo.InvariantCulture);
        connection.Setup(x => x.GetJobParameter(_hangfireJobId, "RetryCount")).Returns(() => stored);
        connection.Setup(x => x.SetJobParameter(_hangfireJobId, "RetryCount", It.IsAny<string>())).Callback<string, string, string>((_, _, value) => stored = value);

        var applyContext = new ApplyStateContext(
            new Mock<JobStorage>().Object,
            connection.Object,
            new Mock<IWriteOnlyTransaction>().Object,
            new BackgroundJob(_hangfireJobId, job, DateTime.UtcNow),
            candidate,
            ProcessingState.StateName);

        return new ElectStateContext(applyContext);
    }
}
