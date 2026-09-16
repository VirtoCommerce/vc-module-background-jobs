#nullable enable
using System.Threading;
using System.Threading.Tasks;

namespace VirtoCommerce.BackgroundJobs.Core.Cancellation;

/// <summary>
/// Shared, fleet-wide record of "cancellation requested" per job id, used by queue engines (RabbitMQ) that cannot
/// recall a message once published. A cancel sets a flag; the worker checks it before starting a job (discard) and
/// while running it (trip the handler's <see cref="CancellationToken"/>). The default implementation is Redis-backed
/// (durable across the fleet), with an in-memory fallback for a single process.
/// </summary>
public interface IJobCancellationStore
{
    /// <summary>Record that <paramref name="jobId"/> should be cancelled. Idempotent.</summary>
    Task RequestCancel(string jobId, CancellationToken cancellationToken = default);

    /// <summary>Returns <c>true</c> when cancellation has been requested for <paramref name="jobId"/>.</summary>
    Task<bool> IsCancelRequested(string jobId, CancellationToken cancellationToken = default);

    /// <summary>Clears the flag once the job has settled (succeeded/failed/cancelled).</summary>
    Task Clear(string jobId, CancellationToken cancellationToken = default);
}
