using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cronos;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using VirtoCommerce.Platform.Core.DistributedLock;
using VirtoCommerce.Platform.Core.Exceptions;
using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs.Core.Recurring;

/// <summary>
/// Engine-agnostic <see cref="IRecurringJobScheduler"/> for engines without native recurring support (e.g. RabbitMQ).
/// The platform applier calls <see cref="AddOrUpdate"/>/<see cref="Remove"/> with the resolved cron; this in-process
/// cron timer fires each due occurrence and enqueues the payload via <see cref="IBackgroundJob"/> so a worker runs it.
/// <para>
/// Multi-instance safe: every instance runs the timer, but each occurrence is enqueued exactly once via a short
/// distributed lock guarding a check-and-set against the shared <see cref="IRecurringJobStateStore"/> marker.
/// </para>
/// </summary>
public sealed class GenericRecurringJobScheduler : BackgroundService, IRecurringJobScheduler
{
    private static readonly TimeSpan _maxSleep = TimeSpan.FromSeconds(60);

    private readonly IServiceProvider _serviceProvider;
    private readonly IDistributedLockService _distributedLock;
    private readonly ILogger<GenericRecurringJobScheduler> _logger;
    private readonly Dictionary<string, ScheduledJob> _jobs = new(StringComparer.OrdinalIgnoreCase);
    private readonly Lock _sync = new();
    private readonly SemaphoreSlim _wake = new(0);

    public GenericRecurringJobScheduler(
        IServiceProvider serviceProvider,
        IDistributedLockService distributedLock,
        ILogger<GenericRecurringJobScheduler> logger)
    {
        _serviceProvider = serviceProvider;
        _distributedLock = distributedLock;
        _logger = logger;
    }

    public Task AddOrUpdate(RecurringJobRegistration registration, string cronExpression, CancellationToken cancellationToken = default)
    {
        CronExpression parsed;
        try
        {
            parsed = ParseCron(cronExpression);
        }
        catch (CronFormatException ex)
        {
            // Remove any existing schedule so a previously-valid cron stops firing after settings change to a bad value.
            _logger.LogError(ex, "Recurring job '{JobId}' has an invalid cron '{Cron}'; removing any existing schedule.", registration.Id, cronExpression);
            lock (_sync)
            {
                _jobs.Remove(registration.Id);
            }
            return Task.CompletedTask;
        }

        lock (_sync)
        {
            _jobs[registration.Id] = new ScheduledJob(registration, parsed)
            {
                NextUtc = parsed.GetNextOccurrence(DateTime.UtcNow, registration.TimeZone),
            };
        }

        _logger.LogInformation("Recurring job '{JobId}' scheduled ('{Cron}').", registration.Id, cronExpression);
        SignalWake();
        return Task.CompletedTask;
    }

    public Task Remove(string recurringJobId, CancellationToken cancellationToken = default)
    {
        lock (_sync)
        {
            _jobs.Remove(recurringJobId);
        }

        SignalWake();
        return Task.CompletedTask;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var now = DateTime.UtcNow;

            foreach (var job in DueJobs(now))
            {
                var occurrence = job.NextUtc!.Value;
                var settled = false;
                try
                {
                    settled = await FireOnceAsync(job.Registration, occurrence, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    // A genuine enqueue error (not lock contention) — advance so a poison occurrence doesn't loop forever.
                    _logger.LogError(ex, "Recurring job '{JobId}' failed to enqueue", job.Registration.Id);
                    settled = true;
                }

                // Only advance past the occurrence once it is settled (fired here, or confirmed handled by another
                // instance). On lock contention we leave NextUtc so the next tick retries — recovering the occurrence
                // if the lock holder failed before enqueuing.
                if (settled)
                {
                    lock (_sync)
                    {
                        job.NextUtc = job.Parsed.GetNextOccurrence(occurrence, job.Registration.TimeZone);
                    }
                }
            }

            try
            {
                await _wake.WaitAsync(ComputeSleep(DateTime.UtcNow), stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
        }
    }

    private List<ScheduledJob> DueJobs(DateTime now)
    {
        lock (_sync)
        {
            return _jobs.Values.Where(x => x.NextUtc is not null && x.NextUtc <= now).ToList();
        }
    }

    private TimeSpan ComputeSleep(DateTime now)
    {
        lock (_sync)
        {
            var next = _jobs.Values.Where(x => x.NextUtc is not null).Select(x => x.NextUtc!.Value).DefaultIfEmpty(now + _maxSleep).Min();
            var delay = next - now;
            return delay < TimeSpan.Zero ? TimeSpan.Zero : delay > _maxSleep ? _maxSleep : delay;
        }
    }

    // Returns true when the occurrence is settled (we fired it, or confirmed another instance already did); false on
    // lock contention, so the caller retries the occurrence instead of skipping it.
    private async Task<bool> FireOnceAsync(RecurringJobRegistration registration, DateTime occurrenceUtc, CancellationToken cancellationToken)
    {
        try
        {
            await _distributedLock.ExecuteAsync(
                $"recurring-job:{registration.Id}",
                async () =>
                {
                    using var scope = _serviceProvider.CreateScope();
                    var store = scope.ServiceProvider.GetRequiredService<IRecurringJobStateStore>();

                    var last = await store.GetLastOccurrence(registration.Id, cancellationToken);
                    if (last is not null && last >= occurrenceUtc)
                    {
                        return false; // another instance already enqueued this occurrence
                    }

                    var backgroundJob = scope.ServiceProvider.GetRequiredService<IBackgroundJob>();
                    await registration.Trigger(backgroundJob, cancellationToken);
                    await store.SetLastOccurrence(registration.Id, occurrenceUtc, cancellationToken);

                    _logger.LogInformation("Recurring job '{JobId}' enqueued for occurrence {Occurrence:o}", registration.Id, occurrenceUtc);
                    return true;
                },
                lockTimeout: TimeSpan.FromSeconds(30),
                tryLockTimeout: TimeSpan.FromSeconds(5),
                retryInterval: TimeSpan.FromMilliseconds(250),
                cancellationToken: cancellationToken);

            return true;
        }
        catch (PlatformException)
        {
            _logger.LogDebug("Recurring job '{JobId}' could not acquire the lock; will retry the occurrence.", registration.Id);
            return false;
        }
    }

    private static CronExpression ParseCron(string cron)
    {
        var fields = cron.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        return CronExpression.Parse(cron, fields.Length >= 6 ? CronFormat.IncludeSeconds : CronFormat.Standard);
    }

    private void SignalWake()
    {
        if (_wake.CurrentCount == 0)
        {
            _wake.Release();
        }
    }

    public override void Dispose()
    {
        _wake.Dispose();
        base.Dispose();
    }

    private sealed class ScheduledJob(RecurringJobRegistration registration, CronExpression parsed)
    {
        public RecurringJobRegistration Registration { get; } = registration;
        public CronExpression Parsed { get; } = parsed;
        public DateTime? NextUtc { get; set; }
    }
}
