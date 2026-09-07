#nullable enable
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using VirtoCommerce.BackgroundJobs.Core;
using VirtoCommerce.BackgroundJobs.Core.Models;
using VirtoCommerce.BackgroundJobs;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.PushNotifications;

namespace VirtoCommerce.BackgroundJobs.InMemory;

/// <summary>
/// Infrastructure-free in-process <see cref="IJobEngine"/> for <b>local development and testing</b>
/// (<c>VirtoCommerce:BackgroundJobs:Provider = InMemory</c>). On enqueue it runs the job on a background task through
/// the shared <see cref="IJobDispatcher"/> (in its own DI scope), retrying up to <c>MaxRetryAttempts</c>, and keeps
/// per-job state for status/delete.
/// <para>
/// It needs no SQL, broker or Redis — but it is <b>single-process and non-durable</b>: jobs live only in this
/// process's memory, are lost on restart, and do not cross instances. <c>Mode</c> is not honored (there is no
/// separate worker tier to scale). Do NOT use it in production or any multi-instance topology — use Hangfire or
/// RabbitMQ there.
/// </para>
/// </summary>
public sealed class InMemoryJobEngine(
    IServiceScopeFactory scopeFactory,
    IPushNotificationManager pushNotificationManager,
    IOptions<BackgroundJobsOptions> options,
    ILogger<InMemoryJobEngine> logger) : IJobEngine
{
    private readonly ConcurrentDictionary<string, string> _states = new();
    // Per-job cancellation sources for in-flight jobs, so Delete can cancel a running job's token (parity with the
    // real engines). Entries are added on enqueue and removed when the job settles.
    private readonly ConcurrentDictionary<string, CancellationTokenSource> _running = new();

    public string ProviderName => BackgroundJobsProviders.InMemory;

    public bool SupportsCancellation => true;

    public Task<string> Enqueue(JobEnvelope envelope, EnqueueOptions enqueueOptions, CancellationToken cancellationToken = default)
    {
        var jobId = Guid.NewGuid().ToString("N");
        _states[jobId] = "Enqueued";
        var cts = new CancellationTokenSource();
        _running[jobId] = cts;

        // Fire-and-forget on the thread pool — the in-process stand-in for a worker draining a queue. The enqueue
        // CancellationToken is intentionally not forwarded (it must not cancel the already-accepted job); the job's own
        // token (cts) is used instead, tripped by Delete.
        _ = Task.Run(() => RunAsync(jobId, envelope, cts.Token), CancellationToken.None);

        return Task.FromResult(jobId);
    }

    private async Task RunAsync(string jobId, JobEnvelope envelope, CancellationToken cancellationToken)
    {
        // MaxRetryAttempts counts retries on top of the first run (default 3 → up to 4 total). Floor at 0 so 0 disables
        // retries. The envelope's per-job value (EnqueueOptions.MaxRetryAttempts) wins over the engine-wide default,
        // matching Hangfire and RabbitMQ.
        var maxRetries = Math.Max(0, envelope.MaxRetryAttempts ?? options.Value.MaxRetryAttempts);

        try
        {
            for (var attempt = 1; attempt <= maxRetries + 1; attempt++)
            {
                try
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    _states[jobId] = "Processing";

                    // Own DI scope so scoped dependencies (repositories, DbContext, …) resolve per execution — same
                    // contract as the real engines' worker path.
                    using var scope = scopeFactory.CreateScope();
                    var dispatcher = scope.ServiceProvider.GetRequiredService<IJobDispatcher>();
                    var context = JobExecutionContextFactory.Create(pushNotificationManager, envelope, jobId);

                    await dispatcher.Dispatch(envelope with { Attempt = attempt }, context, cancellationToken);

                    _states[jobId] = "Succeeded";
                    return;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    // Cancelled via Delete — a terminal outcome, not a failure; don't retry.
                    _states[jobId] = "Cancelled";
                    logger.LogInformation("In-memory job {JobId} ({JobType}) was cancelled.", jobId, envelope.JobType);
                    return;
                }
                catch (Exception ex)
                {
                    _states[jobId] = "Failed";
                    if (attempt <= maxRetries)
                    {
                        logger.LogWarning(ex, "In-memory job {JobId} ({JobType}) failed on attempt {Attempt}; retrying (max {MaxRetries}).",
                            jobId, envelope.JobType, attempt, maxRetries);
                    }
                    else
                    {
                        logger.LogError(ex, "In-memory job {JobId} ({JobType}) failed on final attempt {Attempt}.",
                            jobId, envelope.JobType, attempt);
                    }
                }
            }
        }
        finally
        {
            if (_running.TryRemove(jobId, out var cts))
            {
                cts.Dispose();
            }
        }
    }

    public Task<Job?> GetStatus(string jobId, CancellationToken cancellationToken = default)
        => Task.FromResult(_states.TryGetValue(jobId, out var state)
            ? new Job { Id = jobId, State = state, Completed = state is "Succeeded" or "Failed" }
            : null);

    public Task<bool> Delete(string jobId, CancellationToken cancellationToken = default)
    {
        // Signal a running job to stop (its handler's token trips); RunAsync removes and disposes the source when it
        // settles. A not-yet-started or already-finished job just has its state removed.
        var known = _states.ContainsKey(jobId);
        if (_running.TryGetValue(jobId, out var cts))
        {
            cts.Cancel();
        }

        _states.TryRemove(jobId, out _);
        return Task.FromResult(known);
    }
}
