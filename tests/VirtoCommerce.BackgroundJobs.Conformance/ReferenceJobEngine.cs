using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using VirtoCommerce.BackgroundJobs.Core;
using VirtoCommerce.BackgroundJobs.Core.Models;
using VirtoCommerce.BackgroundJobs;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.PushNotifications;

namespace VirtoCommerce.BackgroundJobs.Conformance;

/// <summary>
/// A minimal, infrastructure-free reference <see cref="IJobEngine"/>: on enqueue it dispatches the job on a
/// background task (a stand-in for a real worker), retrying up to <c>MaxRetryAttempts</c>, and tracks per-job state
/// for status/delete. It exists to (a) validate the conformance kit itself in CI without any external dependency,
/// and (b) serve as the copy-paste template a new engine author starts from. It is NOT a substitute for certifying
/// a real engine.
/// </summary>
public sealed class ReferenceJobEngine(
    IServiceScopeFactory scopeFactory,
    IPushNotificationManager pushNotificationManager,
    IOptions<BackgroundJobsOptions> options) : IJobEngine
{
    private readonly ConcurrentDictionary<string, string> _states = new();

    public string ProviderName => "Reference";

    public Task<string> Enqueue(JobEnvelope envelope, EnqueueOptions enqueueOptions, CancellationToken cancellationToken = default)
    {
        var jobId = Guid.NewGuid().ToString("N");
        _states[jobId] = "Enqueued";
        _ = Task.Run(() => RunAsync(jobId, envelope, enqueueOptions), CancellationToken.None);
        return Task.FromResult(jobId);
    }

    private async Task RunAsync(string jobId, JobEnvelope envelope, EnqueueOptions enqueueOptions)
    {
        var maxRetries = Math.Max(0, enqueueOptions.MaxRetryAttempts ?? options.Value.MaxRetryAttempts);

        for (var attempt = 1; attempt <= maxRetries + 1; attempt++)
        {
            try
            {
                _states[jobId] = "Processing";
                using var scope = scopeFactory.CreateScope();
                var dispatcher = scope.ServiceProvider.GetRequiredService<IJobDispatcher>();
                var context = JobExecutionContextFactory.Create(pushNotificationManager, envelope, jobId);
                await dispatcher.Dispatch(envelope with { Attempt = attempt }, context, CancellationToken.None);
                _states[jobId] = "Succeeded";
                return;
            }
            catch
            {
                _states[jobId] = "Failed"; // retry until the attempt budget is exhausted
            }
        }
    }

    public Task<Job?> GetStatus(string jobId, CancellationToken cancellationToken = default)
        => Task.FromResult(_states.TryGetValue(jobId, out var state)
            ? new Job { Id = jobId, State = state, Completed = state is "Succeeded" or "Failed" }
            : null);

    public Task<bool> Delete(string jobId, CancellationToken cancellationToken = default)
        => Task.FromResult(_states.TryRemove(jobId, out _));
}
