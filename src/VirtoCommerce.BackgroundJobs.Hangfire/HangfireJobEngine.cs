#nullable enable
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Hangfire;
using Hangfire.States;
using Newtonsoft.Json;
using VirtoCommerce.BackgroundJobs.Core;
using VirtoCommerce.BackgroundJobs.Core.Models;
using VirtoCommerce.BackgroundJobs.Core.Services;
using VirtoCommerce.Platform.Core.Jobs;
using HangfireJob = Hangfire.Common.Job;

namespace VirtoCommerce.BackgroundJobs.Hangfire;

/// <summary>
/// Hangfire implementation of <see cref="IJobEngine"/>. Message jobs are enqueued as a Hangfire job whose body
/// runs <see cref="HangfireJobExecutor"/> (which dispatches via <see cref="IJobDispatcher"/>). Reuses the existing
/// Hangfire storage, dashboard, queues, retry and user-context filter — no configuration change.
/// </summary>
public sealed class HangfireJobEngine(IBackgroundJobClient client) : IJobEngine
{
    private static readonly string[] _finalStates = [DeletedState.StateName, FailedState.StateName, SucceededState.StateName];

    public string ProviderName => BackgroundJobsProviders.Hangfire;

    public Task<string> Enqueue(JobEnvelope envelope, EnqueueOptions options, CancellationToken cancellationToken = default)
    {
        var envelopeJson = JsonConvert.SerializeObject(envelope);
        // Hangfire registers worker queues lowercased, so enqueue lowercased too or no worker would drain it.
        var queue = (string.IsNullOrEmpty(envelope.Queue) ? "default" : envelope.Queue!).ToLowerInvariant();

        var job = HangfireJob.FromExpression<HangfireJobExecutor>(x => x.Execute(envelopeJson, null!, CancellationToken.None));
        var jobId = client.Create(job, new EnqueuedState(queue));

        return Task.FromResult(jobId);
    }

    public Task<Job?> GetStatus(string jobId, CancellationToken cancellationToken = default)
    {
        var state = JobStorage.Current.GetConnection().GetStateData(jobId);

        // Unknown/expired id: report as not found (null) per the IJobEngine contract, rather than "completed".
        if (state is null)
        {
            return Task.FromResult<Job?>(null);
        }

        var result = new Job
        {
            Id = jobId,
            State = state.Name,
            Completed = _finalStates.Contains(state.Name),
        };

        return Task.FromResult<Job?>(result);
    }

    public Task<bool> Delete(string jobId, CancellationToken cancellationToken = default)
        => Task.FromResult(BackgroundJob.Delete(jobId));
}
