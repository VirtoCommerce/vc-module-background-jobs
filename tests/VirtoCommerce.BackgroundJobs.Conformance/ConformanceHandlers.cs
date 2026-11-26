using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.Security;

namespace VirtoCommerce.BackgroundJobs.Conformance;

/// <summary>Message-job payload used by the core conformance scenarios.</summary>
public class ConformancePayload
{
    /// <summary>Unique per scenario; keys the <see cref="ConformanceProbe"/> signals and observations.</summary>
    public string CorrelationId { get; set; } = string.Empty;

    public string Value { get; set; } = string.Empty;

    /// <summary>Throw on the first N executions (then succeed) — drives the retry scenario. 0 = never throw.</summary>
    public int FailAttempts { get; set; }

    public bool FailAlways { get; set; }

    /// <summary>Report progress from the handler — drives the progress scenario.</summary>
    public bool ReportProgress { get; set; }

    /// <summary>Block in the handler until the <c>CancellationToken</c> trips — drives the cancellation scenario.</summary>
    public bool BlockUntilCancelled { get; set; }

    /// <summary>
    /// With <see cref="BlockUntilCancelled"/>: once the token trips, throw <see cref="InvalidOperationException"/>
    /// instead of rethrowing the cancellation.
    /// </summary>
    public bool FailOnCancel { get; set; }
}

/// <summary>An <c>AbstractTypeFactory</c>-derived payload, used to prove the engine round-trips the
/// concrete (extended) type, not the base.</summary>
public class ExtendedConformancePayload : ConformancePayload
{
    public string ExtraValue { get; set; } = string.Empty;
}

/// <summary>The single message-job handler the core conformance scenarios enqueue against.</summary>
public sealed class RecordingConformanceHandler(ConformanceProbe probe, IUserNameResolver userNameResolver)
    : IBackgroundJobHandler<ConformancePayload>
{
    public async Task Execute(ConformancePayload payload, IJobExecutionContext context, CancellationToken cancellationToken = default)
    {
        var executions = probe.RecordExecution(
            payload.CorrelationId, payload, userNameResolver.GetCurrentUserName(), context.Headers, context.JobId);

        if (payload.FailAlways)
        {
            throw new InvalidOperationException($"Conformance-induced failure (execution {executions}, always failing).");
        }

        // Fail the first N executions so the engine's retry brings it back; succeed afterwards.
        if (payload.FailAttempts > 0 && executions <= payload.FailAttempts)
        {
            throw new InvalidOperationException($"Conformance-induced failure (execution {executions} of {payload.FailAttempts}).");
        }

        // Long-running, cancellable job: signal started, then wait for the token to trip. When it does, record that the
        // handler observed cancellation and rethrow so the engine sees the job as cancelled.
        if (payload.BlockUntilCancelled)
        {
            // A blocked job that reports does so before it signals, so a scenario that saw the start also saw the update.
            if (payload.ReportProgress)
            {
                await context.Progress.Report(
                    new JobProgressInfo { Message = ConformanceConstants.ProgressMessage, ProcessedCount = 1, TotalCount = 1 },
                    cancellationToken);
            }

            probe.SignalStarted(payload.CorrelationId);
            try
            {
                await Task.Delay(Timeout.Infinite, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                probe.SignalCancelled(payload.CorrelationId);

                if (payload.FailOnCancel)
                {
                    throw new InvalidOperationException("Conformance-induced failure after the cancellation was observed.");
                }

                throw;
            }
        }

        if (payload.ReportProgress)
        {
            await context.Progress.Report(
                new JobProgressInfo { Message = ConformanceConstants.ProgressMessage, ProcessedCount = 1, TotalCount = 1 },
                cancellationToken);
        }

        probe.SignalCompleted(payload.CorrelationId);
    }
}

/// <summary>Map item for the map/reduce conformance scenario; a negative value makes the map handler throw.</summary>
public sealed record ConformanceMapItem(int Value);

public sealed record ConformanceMapResult(int Square);

/// <summary>Reduce state — carries the correlation id so the reducer can signal the probe.</summary>
public sealed class ConformanceReduceState
{
    public string CorrelationId { get; set; } = string.Empty;

    public bool FailAlways { get; set; }
}

public sealed class ConformanceMapHandler : IMapJobHandler<ConformanceMapItem, ConformanceMapResult>
{
    public Task<ConformanceMapResult> Map(ConformanceMapItem item, IJobExecutionContext context, CancellationToken cancellationToken = default)
        => item.Value < 0
            ? throw new InvalidOperationException("Conformance map failure (negative item).")
            : Task.FromResult(new ConformanceMapResult(item.Value * item.Value));
}

public sealed class ConformanceReduceHandler(ConformanceProbe probe) : IReduceJobHandler<ConformanceReduceState, ConformanceMapResult>
{
    public Task Reduce(ConformanceReduceState state, IReadOnlyCollection<MapResult<ConformanceMapResult>> results, IJobExecutionContext context, CancellationToken cancellationToken = default)
    {
        if (state.FailAlways)
        {
            var executions = probe.RecordExecution(state.CorrelationId, state, user: null, context.Headers, context.JobId);

            throw new InvalidOperationException($"Conformance-induced reduce failure (execution {executions}).");
        }

        var total = results.Where(r => r.Succeeded).Sum(r => r.Value!.Square);
        probe.RecordReduce(state.CorrelationId, total, results.Count, results.Count(r => !r.Succeeded));
        probe.SignalCompleted(state.CorrelationId);
        return Task.CompletedTask;
    }
}

/// <summary>
/// A reduce handler the fixture never registers: a batch naming it fails in the reduce step before any handler code
/// runs.
/// </summary>
public sealed class ConformanceUnregisteredReduceHandler : IReduceJobHandler<ConformanceReduceState, ConformanceMapResult>
{
    public Task Reduce(ConformanceReduceState state, IReadOnlyCollection<MapResult<ConformanceMapResult>> results, IJobExecutionContext context, CancellationToken cancellationToken = default)
        => throw new NotSupportedException("Never registered, so never resolved.");
}
