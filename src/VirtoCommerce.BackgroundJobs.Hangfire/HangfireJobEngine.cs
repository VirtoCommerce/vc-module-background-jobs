#nullable enable
using System;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Hangfire;
using Hangfire.States;
using Newtonsoft.Json;
using VirtoCommerce.BackgroundJobs.Core.Models;
using VirtoCommerce.BackgroundJobs.Core.Services;
using VirtoCommerce.Platform.Core.Jobs;
using HangfireJob = Hangfire.Common.Job;

namespace VirtoCommerce.BackgroundJobs.Hangfire;

/// <summary>
/// Hangfire implementation of <see cref="IJobEngine"/>. Message jobs are enqueued as a Hangfire job whose body
/// runs <see cref="HangfireJobExecutor"/> (which dispatches via <see cref="IJobDispatcher"/>). Also implements
/// <see cref="IExpressionJobEngine"/> for the legacy expression-based enqueue sugar. Reuses the existing
/// Hangfire storage, dashboard, queues, retry and user-context filter — no configuration change.
/// </summary>
public sealed class HangfireJobEngine(IBackgroundJobClient client) : IJobEngine, IExpressionJobEngine
{
    private static readonly string[] _finalStates = [DeletedState.StateName, FailedState.StateName, SucceededState.StateName];

    public string ProviderName => "Hangfire";

    public Task<string> Enqueue(JobEnvelope envelope, EnqueueOptions options, CancellationToken cancellationToken = default)
    {
        var envelopeJson = JsonConvert.SerializeObject(envelope);
        var queue = string.IsNullOrEmpty(envelope.Queue) ? "default" : envelope.Queue!;

        var job = HangfireJob.FromExpression<HangfireJobExecutor>(x => x.Execute(envelopeJson, null!, CancellationToken.None));
        var jobId = client.Create(job, new EnqueuedState(queue));

        return Task.FromResult(jobId);
    }

    public string Enqueue(Expression<Action> methodCall) => BackgroundJob.Enqueue(methodCall);

    public string Enqueue(Expression<Func<Task>> methodCall) => BackgroundJob.Enqueue(methodCall);

    public Task<Job?> GetStatus(string jobId, CancellationToken cancellationToken = default)
    {
        var state = JobStorage.Current.GetConnection().GetStateData(jobId);

        var result = new Job
        {
            Id = jobId,
            State = state?.Name,
        };
        result.Completed = state == null || _finalStates.Contains(result.State);

        return Task.FromResult<Job?>(result);
    }

    public Task<bool> Delete(string jobId, CancellationToken cancellationToken = default)
        => Task.FromResult(BackgroundJob.Delete(jobId));
}
