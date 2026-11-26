using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace VirtoCommerce.BackgroundJobs.Conformance;

/// <summary>
/// The deterministic observation point shared by the conformance handlers and the test. Each scenario uses a unique
/// correlation id; the handler records what it saw and signals completion for that id, and the test awaits that
/// signal (with a timeout) before asserting — so the suite never assumes how the engine schedules work, and never
/// depends on <c>Task.Delay</c> guesses. Registered as a singleton in the shared container.
/// </summary>
public sealed class ConformanceProbe
{
    private readonly ConcurrentDictionary<string, TaskCompletionSource> _completions = new();
    private readonly ConcurrentDictionary<string, TaskCompletionSource> _started = new();
    private readonly ConcurrentDictionary<string, TaskCompletionSource> _cancelled = new();
    private readonly ConcurrentDictionary<string, JobObservation> _jobs = new();
    private readonly ConcurrentDictionary<string, int> _executions = new();
    private readonly ConcurrentDictionary<string, List<string>> _executionJobIds = new();
    private readonly ConcurrentDictionary<string, ReduceObservation> _reduces = new();
    private readonly ConcurrentDictionary<string, MapHold> _mapHolds = new();

    /// <summary>Records one handler execution for the correlation id and returns the running execution count
    /// (1 on the first run, 2 on the first retry, …) so retry scenarios can assert how many attempts ran.</summary>
    public int RecordExecution(string correlationId, object payload, string? user, IReadOnlyDictionary<string, string> headers, string jobId)
    {
        var count = _executions.AddOrUpdate(correlationId, 1, (_, current) => current + 1);
        var jobIds = _executionJobIds.GetOrAdd(correlationId, _ => []);
        lock (jobIds)
        {
            jobIds.Add(jobId);
        }

        _jobs[correlationId] = new JobObservation
        {
            Payload = payload,
            RuntimePayloadType = payload.GetType(),
            User = user,
            Headers = headers,
            JobId = jobId,
            Executions = count,
        };
        return count;
    }

    /// <summary>Records the reduce step's aggregate for a map/reduce batch.</summary>
    public void RecordReduce(string correlationId, long total, int resultCount, int failureCount)
        => _reduces[correlationId] = new ReduceObservation { Total = total, ResultCount = resultCount, FailureCount = failureCount };

    /// <summary>Signals that the correlation id reached its terminal success state — releases <see cref="WaitAsync"/>.</summary>
    public void SignalCompleted(string correlationId) => Completion(correlationId).TrySetResult();

    /// <summary>Signals that the handler has begun executing for the correlation id — releases <see cref="WaitStartedAsync"/>.</summary>
    public void SignalStarted(string correlationId) => Started(correlationId).TrySetResult();

    /// <summary>Signals that the handler observed cancellation for the correlation id — releases <see cref="WaitCancelledAsync"/>.</summary>
    public void SignalCancelled(string correlationId) => Cancelled(correlationId).TrySetResult();

    /// <summary>Waits until the correlation id signals completion, or throws <see cref="TimeoutException"/>.</summary>
    public Task WaitAsync(string correlationId, TimeSpan timeout, CancellationToken cancellationToken)
        => Completion(correlationId).Task.WaitAsync(timeout, cancellationToken);

    /// <summary>Waits until the handler has started for the correlation id, or throws <see cref="TimeoutException"/>.</summary>
    public Task WaitStartedAsync(string correlationId, TimeSpan timeout, CancellationToken cancellationToken)
        => Started(correlationId).Task.WaitAsync(timeout, cancellationToken);

    /// <summary>Waits until the handler observed cancellation for the correlation id, or throws <see cref="TimeoutException"/>.</summary>
    public Task WaitCancelledAsync(string correlationId, TimeSpan timeout, CancellationToken cancellationToken)
        => Cancelled(correlationId).Task.WaitAsync(timeout, cancellationToken);

    /// <summary>
    /// Holds the first map task that arrives for <paramref name="gateId"/> until <see cref="ReleaseHeldMapTask"/>; every
    /// later one passes at once, so a batch blocks one worker, not the pool its retries need.
    /// </summary>
    public async Task HoldFirstMapTaskAsync(string gateId, CancellationToken cancellationToken)
    {
        var hold = MapHoldFor(gateId);
        if (Interlocked.Exchange(ref hold.Claimed, 1) == 1)
        {
            return;
        }

        await hold.Released.Task.WaitAsync(cancellationToken);
        Interlocked.Increment(ref hold.Resumed);
        hold.ResumedSignal.TrySetResult();
    }

    /// <summary>Lets the map task held for <paramref name="gateId"/> run; also covers one that arrives later.</summary>
    public void ReleaseHeldMapTask(string gateId) => MapHoldFor(gateId).Released.TrySetResult();

    /// <summary>Waits until the held map task of <paramref name="gateId"/> ran on past its hold, or throws <see cref="TimeoutException"/>.</summary>
    public Task WaitHeldMapTaskResumedAsync(string gateId, TimeSpan timeout, CancellationToken cancellationToken)
        => MapHoldFor(gateId).ResumedSignal.Task.WaitAsync(timeout, cancellationToken);

    /// <summary>How many map tasks of <paramref name="gateId"/> ran on past their hold.</summary>
    public int ResumedMapTasks(string gateId) => Volatile.Read(ref MapHoldFor(gateId).Resumed);

    public JobObservation? Job(string correlationId) => _jobs.TryGetValue(correlationId, out var value) ? value : null;

    public int ExecutionCount(string correlationId) => _executions.TryGetValue(correlationId, out var value) ? value : 0;

    /// <summary>
    /// The job id the context carried on each execution of <paramref name="correlationId"/>, in execution order.
    /// </summary>
    public IReadOnlyList<string> ExecutionJobIds(string correlationId)
    {
        if (!_executionJobIds.TryGetValue(correlationId, out var jobIds))
        {
            return [];
        }

        lock (jobIds)
        {
            return jobIds.ToList();
        }
    }

    public ReduceObservation? Reduce(string correlationId) => _reduces.TryGetValue(correlationId, out var value) ? value : null;

    private MapHold MapHoldFor(string gateId) => _mapHolds.GetOrAdd(gateId, _ => new MapHold());

    private TaskCompletionSource Completion(string correlationId)
        => _completions.GetOrAdd(correlationId, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));

    private TaskCompletionSource Started(string correlationId)
        => _started.GetOrAdd(correlationId, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));

    private TaskCompletionSource Cancelled(string correlationId)
        => _cancelled.GetOrAdd(correlationId, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));

    private sealed class MapHold
    {
        public int Claimed;
        public int Resumed;
        public TaskCompletionSource Released { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource ResumedSignal { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    public sealed record JobObservation
    {
        public required object Payload { get; init; }
        public required Type RuntimePayloadType { get; init; }
        public string? User { get; init; }
        public required IReadOnlyDictionary<string, string> Headers { get; init; }
        public required string JobId { get; init; }
        public int Executions { get; init; }
    }

    public sealed record ReduceObservation
    {
        public long Total { get; init; }
        public int ResultCount { get; init; }
        public int FailureCount { get; init; }
    }
}
