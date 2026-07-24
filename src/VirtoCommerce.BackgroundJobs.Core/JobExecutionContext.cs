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
}
