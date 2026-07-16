#nullable enable
using System.Threading;
using System.Threading.Tasks;
using VirtoCommerce.BackgroundJobs.Core.Models;

namespace VirtoCommerce.BackgroundJobs.Core.Services;

/// <summary>
/// Runs an already-formed <see cref="JobEnvelope"/> in-process: builds the execution context and dispatches the
/// handler via <see cref="IJobDispatcher"/>. This is the reusable "execute a pushed envelope" path shared by
/// push-based engines (e.g. Google Cloud Tasks) whose HTTP callback receives a serialized envelope.
/// </summary>
public interface IJobEnvelopeRunner
{
    /// <summary>Builds the execution context for <paramref name="jobId"/> and dispatches the handler.</summary>
    Task Run(JobEnvelope envelope, string jobId, CancellationToken cancellationToken = default);
}
