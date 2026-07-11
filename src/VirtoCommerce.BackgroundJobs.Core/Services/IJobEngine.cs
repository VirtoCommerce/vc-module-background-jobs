using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using VirtoCommerce.BackgroundJobs.Core.Models;
using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs.Core.Services;

/// <summary>
/// The provider port. Exactly one implementation is active per platform instance, selected by the
/// <c>VirtoCommerce:BackgroundJobs:Provider</c> configuration key.
/// </summary>
public interface IJobEngine
{
    /// <summary>Engine name, e.g. <c>Hangfire</c> or <c>RabbitMQ</c>.</summary>
    string ProviderName { get; }

    /// <summary>Submit an envelope for execution. Returns the engine-specific job id.</summary>
    Task<string> Enqueue(JobEnvelope envelope, EnqueueOptions options, CancellationToken cancellationToken = default);

    /// <summary>
    /// Submit many envelopes in one call, returning the engine-specific ids in envelope order. The default loops
    /// <see cref="Enqueue"/>; engines that can publish in bulk more cheaply (e.g. RabbitMQ pipelining publisher
    /// confirmations under a single channel lock) override this to avoid per-job round-trip and lock churn.
    /// </summary>
    async Task<IReadOnlyList<string>> EnqueueBatch(IReadOnlyCollection<JobEnvelope> envelopes, EnqueueOptions options, CancellationToken cancellationToken = default)
    {
        var ids = new List<string>(envelopes.Count);
        foreach (var envelope in envelopes)
        {
            ids.Add(await Enqueue(envelope, options, cancellationToken));
        }

        return ids;
    }

    /// <summary>Get the status of a previously enqueued job, or null if unknown.</summary>
    Task<Job?> GetStatus(string jobId, CancellationToken cancellationToken = default);

    /// <summary>Delete/cancel a job. Returns true if the job existed and was removed.</summary>
    Task<bool> Delete(string jobId, CancellationToken cancellationToken = default);
}
