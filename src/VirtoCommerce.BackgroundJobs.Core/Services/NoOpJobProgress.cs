using System.Threading;
using System.Threading.Tasks;
using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs.Core.Services;

/// <summary>No-op progress used for fire-and-forget jobs enqueued without progress.</summary>
public sealed class NoOpJobProgress : IJobProgress
{
    public static readonly NoOpJobProgress Instance = new();

    public Task Report(JobProgressInfo progress, CancellationToken cancellationToken = default) => Task.CompletedTask;
}
