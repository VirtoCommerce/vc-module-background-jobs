using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using VirtoCommerce.BackgroundJobs;
using VirtoCommerce.BackgroundJobs.Core;
using VirtoCommerce.BackgroundJobs.Core.Models;
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
    private readonly ConcurrentDictionary<string, CancellationTokenSource> _running = new();

    public string ProviderName => "Reference";

    public bool SupportsCancellation => true;

    public Task<string> Enqueue(JobEnvelope envelope, EnqueueOptions enqueueOptions, CancellationToken cancellationToken = default)
    {
        var jobId = Guid.NewGuid().ToString("N");
        _states[jobId] = "Enqueued";
        var cts = new CancellationTokenSource();
        _running[jobId] = cts;
        _ = Task.Run(() => RunAsync(jobId, envelope, enqueueOptions, cts.Token), CancellationToken.None);
        return Task.FromResult(jobId);
    }

    private async Task RunAsync(string jobId, JobEnvelope envelope, EnqueueOptions enqueueOptions, CancellationToken cancellationToken)
    {
        // The envelope's value first; the enqueue option covers a caller that bypassed the facade.
        var maxRetries = Math.Max(0, envelope.MaxRetryAttempts ?? enqueueOptions.MaxRetryAttempts ?? options.Value.MaxRetryAttempts);

        try
        {
            await RunAttemptsAsync(jobId, envelope, maxRetries, cancellationToken);
        }
        finally
        {
            if (_running.TryRemove(jobId, out var cts))
            {
                cts.Dispose();
            }
        }
    }

    private async Task RunAttemptsAsync(string jobId, JobEnvelope envelope, int maxRetries, CancellationToken cancellationToken)
    {
        for (var attempt = 1; attempt <= maxRetries + 1; attempt++)
        {
            // Built before the cancellation check, so a cancel before this attempt closes with its attempt and
            // total, not the envelope's defaults.
            var attemptEnvelope = envelope with { Attempt = attempt, TotalAttempts = maxRetries + 1 };
            var context = JobExecutionContextFactory.Create(pushNotificationManager, attemptEnvelope, jobId);

            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                _states[jobId] = "Processing";
                using var scope = scopeFactory.CreateScope();
                var dispatcher = scope.ServiceProvider.GetRequiredService<IJobDispatcher>();

                await dispatcher.Dispatch(attemptEnvelope, context, cancellationToken);
                _states[jobId] = "Succeeded";
                return;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // The job's token has no writer but Delete: a requested cancel, never retried. Close it Canceled;
                // the dispatcher leaves it open.
                _states[jobId] = "Cancelled";
                await context.Progress.TryCompleteCanceledAsync(NullLogger.Instance, jobId);
                return;
            }
            catch
            {
                // Failed is terminal: while an attempt is left to retry, the job is still Processing.
                if (attempt > maxRetries)
                {
                    _states[jobId] = "Failed";
                }
            }
        }
    }

    public Task<Job?> GetStatus(string jobId, CancellationToken cancellationToken = default)
        => Task.FromResult(_states.TryGetValue(jobId, out var state)
            ? new Job { Id = jobId, State = state, Completed = IsTerminal(state) }
            : null);

    public Task<bool> Delete(string jobId, CancellationToken cancellationToken = default)
    {
        if (!_states.TryGetValue(jobId, out var state) || IsTerminal(state))
        {
            return Task.FromResult(false);
        }

        if (_running.TryGetValue(jobId, out var cts))
        {
            try
            {
                cts.Cancel();

                return Task.FromResult(true);
            }
            catch (ObjectDisposedException)
            {
                // Falls through to the settled check below.
            }
        }

        // Settled between the check and the cancel: decide from its state, so a cancel racing a finishing job is
        // refused.
        return Task.FromResult(_states.TryGetValue(jobId, out var settled) && !IsTerminal(settled));
    }

    private static bool IsTerminal(string state) => state is "Succeeded" or "Failed" or "Cancelled";
}
