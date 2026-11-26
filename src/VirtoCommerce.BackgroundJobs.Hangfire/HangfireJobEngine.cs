#nullable enable
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Hangfire;
using Hangfire.States;
using Newtonsoft.Json;
using VirtoCommerce.BackgroundJobs;
using VirtoCommerce.BackgroundJobs.Core;
using VirtoCommerce.BackgroundJobs.Core.Models;
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
    private const int _maxDeleteAttempts = 3;

    private static readonly string[] _finalStates = [DeletedState.StateName, FailedState.StateName, SucceededState.StateName];

    private static readonly string[] _cancellableStates = [.. CancelBeforeRunStateFilter.NotYetRunStates, ProcessingState.StateName];

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
    {
        var deleted = false;

        for (var attempt = 1; attempt <= _maxDeleteAttempts; attempt++)
        {
            using var connection = JobStorage.Current.GetConnection();
            var stateName = connection.GetStateData(jobId)?.Name;

            if (stateName is null || !_cancellableStates.Contains(stateName))
            {
                return Task.FromResult(false);
            }

            // Pass the state just read: a job that moved on since (finished, or picked up) is refused, and the loop re-reads it.
            deleted = client.ChangeState(jobId, new DeletedState(), stateName);
            if (deleted)
            {
                break;
            }
        }

        return Task.FromResult(deleted);
    }

    // Hangfire removes a not-started job from its queue and transitions a running one to Deleted, which trips the
    // CancellationToken already flowed through HangfireJobExecutor into the dispatcher/handler.
    public bool SupportsCancellation => true;
}
