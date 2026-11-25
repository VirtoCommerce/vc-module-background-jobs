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
    // Per-job cancellation sources, so Delete can trip a running job's token. Added on enqueue, removed when the job settles.
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
        // The envelope's per-job value rides to whichever worker runs the job; the enqueue option is the same value for
        // a caller that did not go through the facade.
        var maxRetries = Math.Max(0, envelope.MaxRetryAttempts ?? enqueueOptions.MaxRetryAttempts ?? options.Value.MaxRetryAttempts);

        try
        {
            for (var attempt = 1; attempt <= maxRetries + 1; attempt++)
            {
                // Fill the attempt and the attempts the job gets in all, and build the context from that same envelope, so
                // the dispatcher can tell a failure with a retry pending from the final one. Built before the cancellation
                // check so that a cancel observed before this attempt starts closes the notification with the attempt that
                // was about to run and the job's total, not the envelope's defaults.
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
                    // The job's own token has no writer but Delete, so this is a requested cancel — terminal, never
                    // retried. The dispatcher leaves the notification open for a cancelled token: close it Canceled here.
                    _states[jobId] = "Cancelled";
                    // A failure to close the notification must not turn a requested cancel into a failure: it stays open. The shared
                    // helper does that; the kit has no logger of its own, so the warning it would write goes nowhere.
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
            ? new Job { Id = jobId, State = state, Completed = IsTerminal(state) }
            : null);

    public Task<bool> Delete(string jobId, CancellationToken cancellationToken = default)
    {
        // Unknown or finished: nothing to cancel, and a finished job keeps the state it ended in.
        if (!_states.TryGetValue(jobId, out var state) || IsTerminal(state))
        {
            return Task.FromResult(false);
        }

        // Trip the job's token; RunAsync then records Cancelled and removes and disposes the source. The entry stays.
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

        // The job settled between the state check and the cancel (no token left, or it was just disposed), so it is
        // decided from its state as for any finished job: a cancel racing a finishing job is not accepted.
        return Task.FromResult(_states.TryGetValue(jobId, out var settled) && !IsTerminal(settled));
    }

    private static bool IsTerminal(string state) => state is "Succeeded" or "Failed" or "Cancelled";
}
