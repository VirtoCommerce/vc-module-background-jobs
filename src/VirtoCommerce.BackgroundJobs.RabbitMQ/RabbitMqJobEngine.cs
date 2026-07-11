using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using RabbitMQ.Client;
using VirtoCommerce.BackgroundJobs.Core;
using VirtoCommerce.BackgroundJobs.Core.Models;
using VirtoCommerce.BackgroundJobs.Core.Services;
using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs.RabbitMQ;

/// <summary>
/// RabbitMQ implementation of <see cref="IJobEngine"/>. Publishes the serialized <see cref="JobEnvelope"/> to a
/// durable queue via the default exchange; the in-process <see cref="RabbitMqJobConsumer"/> drains it and runs the
/// handler through <see cref="IJobDispatcher"/>.
/// <para>
/// Publishing reuses a single long-lived channel with publisher confirmations enabled (so each publish completes
/// only after the broker accepts it). The channel is not thread-safe, so publishes are serialized by a semaphore;
/// it is re-created transparently if it drops.
/// </para>
/// <para>
/// RabbitMQ has no native job store, so <see cref="GetStatus"/> and <see cref="Delete"/> are best-effort:
/// <see cref="GetStatus"/> returns <c>null</c> (unknown) and delete is unsupported (progress is observed over SignalR instead).
/// </para>
/// </summary>
public sealed class RabbitMqJobEngine : IJobEngine, IAsyncDisposable
{
    public const string ProviderNameValue = BackgroundJobsProviders.RabbitMq;

    private static readonly CreateChannelOptions _publishChannelOptions = new(
        publisherConfirmationsEnabled: true,
        publisherConfirmationTrackingEnabled: true);

    private readonly IRabbitMqConnectionProvider _connectionProvider;
    private readonly ILogger<RabbitMqJobEngine> _logger;
    private readonly SemaphoreSlim _publishLock = new(1, 1);
    // Upper bound for a single publish+confirm, used INSTEAD of the caller's token so a client disconnect can't tear
    // an in-flight publish, while a genuinely unresponsive broker still fails instead of hanging the publish lock.
    private static readonly TimeSpan _publishTimeout = TimeSpan.FromSeconds(30);
    private readonly HashSet<string> _declaredQueues = new(StringComparer.Ordinal);
    private IChannel? _channel;

    public RabbitMqJobEngine(IRabbitMqConnectionProvider connectionProvider, ILogger<RabbitMqJobEngine> logger)
    {
        _connectionProvider = connectionProvider;
        _logger = logger;
    }

    public string ProviderName => ProviderNameValue;

    public async Task<string> Enqueue(JobEnvelope envelope, EnqueueOptions options, CancellationToken cancellationToken = default)
    {
        // Enqueue is atomic: cancellation is honored only BEFORE the publish starts (nothing is submitted yet). Once
        // publishing begins we do NOT pass the caller's token to the broker — a client disconnect / request abort must
        // not tear an in-flight confirmed publish (that would log noisy errors and leave the job ambiguously enqueued).
        // A publish is sub-millisecond; a stuck broker is bounded by _publishTimeout below.
        cancellationToken.ThrowIfCancellationRequested();

        var queue = string.IsNullOrEmpty(envelope.Queue) ? "default" : envelope.Queue!;
        var jobId = Guid.NewGuid().ToString("N");
        var body = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(envelope));

        var properties = new BasicProperties
        {
            MessageId = jobId,
            Persistent = true,
            ContentType = "application/json",
        };

        // The channel isn't thread-safe; serialize publishes (and the lazy declare) on the shared channel. Use a local
        // timeout token (NOT the caller's) so the publish runs to completion regardless of the request's lifetime.
        using var publishCts = new CancellationTokenSource(_publishTimeout);
        var publishToken = publishCts.Token;

        await _publishLock.WaitAsync(publishToken);
        try
        {
            var channel = await EnsureChannelAsync(publishToken);

            // Declare each queue once per channel lifetime (idempotent, but avoids a round-trip per publish).
            if (_declaredQueues.Add(queue))
            {
                await channel.QueueDeclareAsync(
                    queue: queue,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null,
                    cancellationToken: publishToken);
            }

            await channel.BasicPublishAsync(
                exchange: string.Empty,
                routingKey: queue,
                mandatory: false,
                basicProperties: properties,
                body: body,
                cancellationToken: publishToken);
        }
        finally
        {
            _publishLock.Release();
        }

        _logger.LogInformation("Published job {JobId} ({JobType}) to RabbitMQ queue '{Queue}' (broker confirmed)", jobId, envelope.JobType, queue);

        return jobId;
    }

    /// <summary>
    /// Bulk publish under a single channel-lock acquisition: issue every publish first, then await all publisher
    /// confirmations together, so the confirms pipeline instead of paying one full round-trip per message (the
    /// dominant cost of looping <see cref="Enqueue"/>). Atomic w.r.t. the caller's token, like <see cref="Enqueue"/>.
    /// </summary>
    public async Task<IReadOnlyList<string>> EnqueueBatch(IReadOnlyCollection<JobEnvelope> envelopes, EnqueueOptions options, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (envelopes.Count == 0)
        {
            return [];
        }

        using var publishCts = new CancellationTokenSource(_publishTimeout);
        var publishToken = publishCts.Token;

        var ids = new List<string>(envelopes.Count);
        var confirmations = new List<Task>(envelopes.Count);

        await _publishLock.WaitAsync(publishToken);
        try
        {
            var channel = await EnsureChannelAsync(publishToken);

            foreach (var envelope in envelopes)
            {
                var queue = string.IsNullOrEmpty(envelope.Queue) ? "default" : envelope.Queue!;
                if (_declaredQueues.Add(queue))
                {
                    await channel.QueueDeclareAsync(queue, durable: true, exclusive: false, autoDelete: false, arguments: null, cancellationToken: publishToken);
                }

                var jobId = Guid.NewGuid().ToString("N");
                ids.Add(jobId);
                var body = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(envelope));
                var properties = new BasicProperties { MessageId = jobId, Persistent = true, ContentType = "application/json" };

                // Issue the publish but defer awaiting its confirmation — collect them so confirms pipeline.
                confirmations.Add(channel.BasicPublishAsync(
                    exchange: string.Empty, routingKey: queue, mandatory: false,
                    basicProperties: properties, body: body, cancellationToken: publishToken).AsTask());
            }

            await Task.WhenAll(confirmations);
        }
        finally
        {
            _publishLock.Release();
        }

        _logger.LogInformation("Batch-published {Count} jobs to RabbitMQ (broker confirmed)", ids.Count);
        return ids;
    }

    // Caller holds _publishLock.
    private async Task<IChannel> EnsureChannelAsync(CancellationToken cancellationToken)
    {
        if (_channel is { IsOpen: true })
        {
            return _channel;
        }

        if (_channel is not null)
        {
            await _channel.DisposeAsync();
            _channel = null;
            _declaredQueues.Clear(); // re-declare queues on the fresh channel
        }

        _channel = await _connectionProvider.CreateChannelAsync(_publishChannelOptions, cancellationToken);
        return _channel;
    }

    /// <summary>
    /// RabbitMQ keeps no job ledger, so once a message is published its lifecycle is not queryable by id — every job
    /// is effectively unknown. Returns <c>null</c> (the <see cref="IJobEngine"/> "unknown/expired" signal) so callers
    /// treat it like any unknown job; the monitoring API maps that to a completed result so pollers stop rather than
    /// waiting forever on a status this engine can never report. Observe a job's real progress over SignalR instead.
    /// </summary>
    public Task<Job?> GetStatus(string jobId, CancellationToken cancellationToken = default)
    {
        return Task.FromResult<Job?>(null);
    }

    /// <summary>Not supported on RabbitMQ — a published message cannot be recalled by id. Always returns false.</summary>
    public Task<bool> Delete(string jobId, CancellationToken cancellationToken = default) => Task.FromResult(false);

    public async ValueTask DisposeAsync()
    {
        if (_channel is not null)
        {
            await _channel.DisposeAsync();
            _channel = null;
        }

        _publishLock.Dispose();
    }
}
