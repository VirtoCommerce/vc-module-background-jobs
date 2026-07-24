using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs;

/// <summary>
/// Bulk producer facade: submit many jobs for one handler in a single call. Complements the single-job
/// <see cref="IBackgroundJob"/> for bulk producers (imports, re-index fan-out) so the active engine can publish
/// efficiently in one shot (e.g. RabbitMQ pipelines publisher confirmations under one channel lock) instead of
/// paying a round-trip per job. Bulk enqueue does not create per-job progress notifications.
/// </summary>
public interface IBulkBackgroundJob
{
    /// <summary>Enqueue one job per payload, all handled by <typeparamref name="THandler"/>. Returns the engine job
    /// ids in payload order.</summary>
    Task<IReadOnlyList<string>> EnqueueBatch<THandler>(
        IReadOnlyCollection<object> payloads,
        EnqueueOptions? options = null,
        CancellationToken cancellationToken = default)
        where THandler : class;
}
