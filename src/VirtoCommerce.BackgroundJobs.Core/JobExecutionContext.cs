using System.Collections.Generic;
using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs;

/// <summary>Default <see cref="IJobExecutionContext"/> built by the dispatcher for each running job.</summary>
public sealed class JobExecutionContext(string jobId, IJobProgress progress, IReadOnlyDictionary<string, string> headers)
    : IJobExecutionContext
{
    public string JobId { get; } = jobId;

    public IJobProgress Progress { get; } = progress;

    public IReadOnlyDictionary<string, string> Headers { get; } = headers;

    /// <summary>
    /// Whether a failure of the attempt this context was built for ends the job (see <c>JobEnvelopeExtensions.IsFinalAttempt</c>).
    /// Internal on purpose: it is the module's own coordinators that need it (a map/reduce reduce step closes the shared
    /// progress bar on its final failure), and a public member would be a contract every engine's context must honor.
    /// </summary>
    internal bool IsFinalAttempt { get; init; }
}
