using System;
using System.Threading;

namespace VirtoCommerce.BackgroundJobs;

/// <summary>
/// Ambient, async-flowed correlation id for a batch of enqueue calls (for example a load-test run). A producer wraps
/// its <c>Enqueue</c> calls in <see cref="BeginRun"/>; the enqueue facade then stamps the id onto every
/// <see cref="Core.Models.JobEnvelope"/> as the <see cref="JobHeaders.RunId"/> header, so all jobs from one run can be
/// filtered together in telemetry. A no-op when unset — nothing is stamped and there is no production impact.
/// </summary>
/// <remarks>
/// <see cref="BeginBatch"/> works the same way for the start time of a map/reduce batch: the fan-out wraps its map task
/// enqueues in it, and the facade stamps the time as the <see cref="JobHeaders.BatchStartedAt"/> header on every
/// enqueue that reports into the batch's shared progress notification.
/// </remarks>
public static class JobEnqueueContext
{
    private static readonly AsyncLocal<string?> _runId = new();
    private static readonly AsyncLocal<DateTime?> _batchStartedAt = new();

    /// <summary>The current ambient run id, or null when none is active.</summary>
    public static string? RunId => _runId.Value;

    /// <summary>
    /// Sets the ambient run id for the current async scope; dispose the returned handle to restore the previous value.
    /// </summary>
    public static IDisposable BeginRun(string runId)
    {
        ArgumentException.ThrowIfNullOrEmpty(runId);
        var previous = _runId.Value;
        _runId.Value = runId;
        return new Scope<string?>(_runId, previous);
    }

    /// <summary>The start time of the map/reduce batch being fanned out, or null when none is active.</summary>
    public static DateTime? BatchStartedAt => _batchStartedAt.Value;

    /// <summary>
    /// Sets the ambient batch start time for the current async scope; dispose the returned handle to restore the
    /// previous value. A map task cannot read the batch's start time from its own envelope, and the platform
    /// <c>EnqueueOptions</c> has no header slot, so the enqueue facade reads it from here and
    /// stamps it only on an enqueue that carries a progress notification id. A no-op when unset.
    /// </summary>
    public static IDisposable BeginBatch(DateTime started)
    {
        var previous = _batchStartedAt.Value;
        _batchStartedAt.Value = started;
        return new Scope<DateTime?>(_batchStartedAt, previous);
    }

    private sealed class Scope<T>(AsyncLocal<T> slot, T previous) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            slot.Value = previous;
        }
    }
}
