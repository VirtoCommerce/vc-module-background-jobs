using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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
    private readonly ConcurrentDictionary<string, JobObservation> _jobs = new();
    private readonly ConcurrentDictionary<string, int> _executions = new();
    private readonly ConcurrentDictionary<string, ReduceObservation> _reduces = new();

    /// <summary>Records one handler execution for the correlation id and returns the running execution count
    /// (1 on the first run, 2 on the first retry, …) so retry scenarios can assert how many attempts ran.</summary>
    public int RecordExecution(string correlationId, object payload, string? user, IReadOnlyDictionary<string, string> headers, string jobId)
    {
        var count = _executions.AddOrUpdate(correlationId, 1, (_, current) => current + 1);
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

    /// <summary>Waits until the correlation id signals completion, or throws <see cref="TimeoutException"/>.</summary>
    public Task WaitAsync(string correlationId, TimeSpan timeout, CancellationToken cancellationToken)
        => Completion(correlationId).Task.WaitAsync(timeout, cancellationToken);

    public JobObservation? Job(string correlationId) => _jobs.TryGetValue(correlationId, out var value) ? value : null;

    public int ExecutionCount(string correlationId) => _executions.TryGetValue(correlationId, out var value) ? value : 0;

    public ReduceObservation? Reduce(string correlationId) => _reduces.TryGetValue(correlationId, out var value) ? value : null;

    private TaskCompletionSource Completion(string correlationId)
        => _completions.GetOrAdd(correlationId, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));

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
