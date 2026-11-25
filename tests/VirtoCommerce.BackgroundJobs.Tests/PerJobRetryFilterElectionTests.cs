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

/// <summary>
/// Defines the xUnit collection of the tests that replace the process-wide <see cref="GlobalJobFilters"/>. The
/// Hangfire conformance fixture rebuilds that same static, and test classes run in parallel with each other, so such
/// tests must not overlap with it. <c>DisableParallelization</c> makes the runner execute this collection alone,
/// after the parallel ones have finished.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class GlobalJobFiltersCollection
{
    public const string Name = "GlobalJobFilters";
}

/// <summary>
/// Runs <see cref="PerJobRetryFilterAttribute"/> against a real <see cref="ElectStateContext"/> after the real
/// <see cref="AutomaticRetryAttribute"/> has elected, the way Hangfire's state machine does — the one place the
/// election itself can be asserted without a worker. Each case fails <b>silently</b> on a regression: a job that a
/// per-job limit still allows to retry would be deleted early, and its progress notification would hang at "retrying".
/// </summary>
[Collection(GlobalJobFiltersCollection.Name)]
public sealed class PerJobRetryFilterElectionTests : IDisposable
{
    private const string _hangfireJobId = "job-1";

    // The attempts the global AutomaticRetryAttribute gives every job; the per-job limit goes above it.
    private const int _globalAttempts = 3;
    private const int _jobLimit = 5;

    private readonly List<object> _originalFilters = GlobalJobFilters.Filters.Select(x => x.Instance).ToList();
    private readonly ILogProvider? _originalLogProvider = ReadLogProvider();

    public PerJobRetryFilterElectionTests()
    {
        // AutomaticRetryAttribute takes its logger in its constructor from Hangfire's static log provider. A conformance
        // fixture that ran earlier in this process leaves the provider of its disposed host there, and any logger taken
        // from it throws. This class neither asserts nor needs logging, so it runs with a silent provider.
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
    public void Failed_ThenDeletedByGlobal_AtJobLimit_StaysDeleted()
    {
        var (context, connection) = Elect(retryCountStored: _jobLimit, AttemptsExceededAction.Delete, new FailedState(new InvalidOperationException("boom")));

        Assert.IsType<DeletedState>(context.CandidateState);
        connection.Verify(x => x.SetJobParameter(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>()), Times.Never);
    }

    [Fact]
    public void Deleted_WithoutTraversedFailure_IsNotTouched()
    {
        // A dashboard delete or a cancel arrives as a plain DeletedState: nothing failed, so there is nothing to retry.
        // The global retry filter is registered, as it always is in production, and the stored count is below the job
        // limit: without the traversed-failure check the filter would reach for a failure that was never traversed.
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
        // The existing raise from FailedState, guarded alongside the Delete variant.
        var (context, connection) = Elect(retryCountStored: _globalAttempts, AttemptsExceededAction.Fail, new FailedState(new InvalidOperationException("boom")));

        Assert.IsType<ScheduledState>(context.CandidateState);
        connection.Verify(x => x.SetJobParameter(_hangfireJobId, "RetryCount", "4"), Times.Once);
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

    // Registers the global retry filter, elects the failure through it first (so a Delete is reached the way Hangfire
    // reaches it: Failed candidate, then Deleted with the failure traversed) and then through the per-job filter.
    private (ElectStateContext Context, Mock<IStorageConnection> Connection) Elect(int retryCountStored, AttemptsExceededAction onExceeded, FailedState failure)
    {
        var global = RegisterGlobalRetry(onExceeded);

        var context = CreateContext(failure, retryCountStored, out var connection);

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

    // The state change of a job the engine enqueued: Execute(envelopeJson, context, token), as HangfireJobEngine builds it.
    // Hangfire's public constructors build the context; only the storage collaborators are mocked.
    private static ElectStateContext CreateContext(IState candidate, int retryCountStored, out Mock<IStorageConnection> connection)
    {
        var envelopeJson = JsonConvert.SerializeObject(new JobEnvelope
        {
            JobType = "t",
            PayloadType = "t",
            PayloadJson = "{}",
            MaxRetryAttempts = _jobLimit,
        });
        var job = Job.FromExpression<HangfireJobExecutor>(x => x.Execute(envelopeJson, null!, CancellationToken.None));

        connection = new Mock<IStorageConnection>();
        connection.Setup(x => x.GetJobParameter(_hangfireJobId, "RetryCount")).Returns(retryCountStored.ToString(CultureInfo.InvariantCulture));

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
