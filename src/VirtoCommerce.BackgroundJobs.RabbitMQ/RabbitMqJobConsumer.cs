using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using VirtoCommerce.BackgroundJobs.Core;
using VirtoCommerce.BackgroundJobs.Core.Models;
using VirtoCommerce.BackgroundJobs.Core.Services;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.PushNotifications;

namespace VirtoCommerce.BackgroundJobs.RabbitMQ;

/// <summary>
/// In-process RabbitMQ consumer. Runs only when the active provider is RabbitMQ and the instance <c>Mode</c> is
/// Worker/Both. Drains the configured queues, rebuilds the execution context (progress) from each
/// <see cref="JobEnvelope"/> and runs the shared <see cref="IJobDispatcher"/>. Messages are acknowledged on success;
/// on failure they are retried by re-publishing with an incremented attempt up to
/// <see cref="BackgroundJobsOptions.MaxRetryAttempts"/>, then dropped (the original is always acked to avoid a
/// poison-message redelivery loop).
/// </summary>
public sealed class RabbitMqJobConsumer : BackgroundService
{
    private static readonly TimeSpan _reconnectDelay = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan _healthCheckInterval = TimeSpan.FromSeconds(5);

    private readonly IRabbitMqConnectionProvider _connectionProvider;
    private readonly IJobDispatcher _dispatcher;
    private readonly IPushNotificationManager _pushNotificationManager;
    private readonly RabbitMqOptions _rabbitMqOptions;
    private readonly BackgroundJobsOptions _jobsOptions;
    private readonly ILogger<RabbitMqJobConsumer> _logger;

    // IChannel is not thread-safe; serialize channel writes (ack/publish/declare) so deliveries dispatched
    // concurrently (PrefetchCount > 1) don't use the channel at the same time.
    private readonly SemaphoreSlim _channelLock = new(1, 1);

    // The consumer channel also publishes the retry / dead-letter copies. Enable publisher confirms (matching the
    // producer's publish channel) so BasicPublishAsync awaits broker durability before we ack the original delivery —
    // otherwise a connection/channel drop between the republish and the ack silently loses the retry/DLQ copy.
    private static readonly CreateChannelOptions _channelOptions = new(
        publisherConfirmationsEnabled: true,
        publisherConfirmationTrackingEnabled: true);

    private IChannel? _channel;

    public RabbitMqJobConsumer(
        IRabbitMqConnectionProvider connectionProvider,
        IJobDispatcher dispatcher,
        IPushNotificationManager pushNotificationManager,
        IOptions<RabbitMqOptions> rabbitMqOptions,
        IOptions<BackgroundJobsOptions> jobsOptions,
        ILogger<RabbitMqJobConsumer> logger)
    {
        _connectionProvider = connectionProvider;
        _dispatcher = dispatcher;
        _pushNotificationManager = pushNotificationManager;
        _rabbitMqOptions = rabbitMqOptions.Value;
        _jobsOptions = jobsOptions.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Connect with retry so a temporarily-unavailable broker doesn't crash platform startup, and stay alive —
        // reconnecting if the channel/connection drops — until the host shuts down.
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await StartConsumingAsync(stoppingToken);
                _logger.LogInformation("RabbitMQ job consumer started for queues: {Queues}", string.Join(", ", GetQueues()));

                // Hold the hosted service open while the channel is healthy; loop to reconnect when it closes.
                while (!stoppingToken.IsCancellationRequested && _channel is { IsOpen: true })
                {
                    await Task.Delay(_healthCheckInterval, stoppingToken);
                }

                if (!stoppingToken.IsCancellationRequested)
                {
                    _logger.LogWarning("RabbitMQ channel closed; reconnecting in {Delay}s.", _reconnectDelay.TotalSeconds);
                    await Task.Delay(_reconnectDelay, stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "RabbitMQ job consumer error; retrying in {Delay}s", _reconnectDelay.TotalSeconds);
                await Task.Delay(_reconnectDelay, stoppingToken);
            }
        }
    }

    private async Task StartConsumingAsync(CancellationToken cancellationToken)
    {
        // Dispose any channel left over from a previous failed attempt so retries don't leak channels.
        if (_channel is not null)
        {
            await _channel.DisposeAsync();
            _channel = null;
        }

        // Build the channel locally and publish it to the field only after full setup succeeds; on partial failure
        // dispose it so we never leak a half-initialized channel.
        var channel = await _connectionProvider.CreateChannelAsync(_channelOptions, cancellationToken);
        try
        {
            await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: _rabbitMqOptions.EffectivePrefetchCount(), global: false, cancellationToken);

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += OnReceivedAsync;

            foreach (var queue in GetQueues())
            {
                await channel.QueueDeclareAsync(
                    queue: queue,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null,
                    cancellationToken: cancellationToken);

                var consumerTag = await channel.BasicConsumeAsync(queue, autoAck: false, consumer: consumer, cancellationToken: cancellationToken);
                _logger.LogInformation("RabbitMQ consumer '{ConsumerTag}' subscribed to queue '{Queue}'", consumerTag, queue);
            }

            _channel = channel;
        }
        catch
        {
            await channel.DisposeAsync();
            throw;
        }
    }

    private async Task OnReceivedAsync(object sender, BasicDeliverEventArgs eventArgs)
    {
        // Use the channel the delivery arrived on (avoids racing the _channel field during startup).
        var channel = (sender as AsyncEventingBasicConsumer)?.Channel ?? _channel;
        if (channel is null)
        {
            _logger.LogWarning("RabbitMQ delivery {DeliveryTag} received but the channel is null; skipping", eventArgs.DeliveryTag);
            return;
        }

        _logger.LogDebug("RabbitMQ delivery received: tag {DeliveryTag}, routingKey '{RoutingKey}', messageId {MessageId}, {Bytes} bytes",
            eventArgs.DeliveryTag, eventArgs.RoutingKey, eventArgs.BasicProperties.MessageId, eventArgs.Body.Length);

        var json = Encoding.UTF8.GetString(eventArgs.Body.Span);
        JobEnvelope? envelope;
        try
        {
            envelope = JsonConvert.DeserializeObject<JobEnvelope>(json, JobJsonSettings.Default);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Discarding malformed RabbitMQ job message (delivery tag {DeliveryTag})", eventArgs.DeliveryTag);
            await TryAckAsync(channel, eventArgs.DeliveryTag);
            return;
        }

        if (envelope is null)
        {
            await TryAckAsync(channel, eventArgs.DeliveryTag);
            return;
        }

        var jobId = eventArgs.BasicProperties.MessageId ?? string.Empty;
        var dispatched = false;

        try
        {
            var context = JobExecutionContextFactory.Create(_pushNotificationManager, envelope, jobId);

            _logger.LogDebug("Dispatching job {JobId} ({JobType})", jobId, envelope.JobType);

            await _dispatcher.Dispatch(envelope, context, CancellationToken.None);
            dispatched = true;

            await RunChannelOpAsync(async () => await channel.BasicAckAsync(eventArgs.DeliveryTag, multiple: false));
            _logger.LogDebug("Completed and acked job {JobId} ({JobType})", jobId, envelope.JobType);
        }
        catch (Exception ex) when (!dispatched)
        {
            // The handler itself failed: re-route to retry/DLQ, then ack the original — but only if re-routing
            // succeeds, otherwise leave it unacked so the broker redelivers it (no silent loss).
            try
            {
                await HandleFailureAsync(channel, envelope, jobId, ex);
                await RunChannelOpAsync(async () => await channel.BasicAckAsync(eventArgs.DeliveryTag, multiple: false));
            }
            catch (Exception failureEx)
            {
                _logger.LogError(failureEx, "Failed to route failed job {JobId} to retry/dead-letter; leaving it unacked for redelivery", jobId);
            }
        }
        catch (Exception ackEx)
        {
            // The handler ran but the ack failed: do NOT re-route (that would double-execute). The broker may
            // redeliver, so handlers should be idempotent.
            _logger.LogError(ackEx, "Job {JobId} executed but acknowledgement failed; it may be redelivered", jobId);
        }
    }

    // Serializes a write on the (non-thread-safe) channel against concurrent deliveries.
    private async Task RunChannelOpAsync(Func<Task> operation)
    {
        await _channelLock.WaitAsync();
        try
        {
            await operation();
        }
        finally
        {
            _channelLock.Release();
        }
    }

    private async Task TryAckAsync(IChannel channel, ulong deliveryTag)
    {
        try
        {
            await RunChannelOpAsync(async () => await channel.BasicAckAsync(deliveryTag, multiple: false));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to acknowledge RabbitMQ delivery {DeliveryTag}", deliveryTag);
        }
    }

    /// <summary>
    /// Re-routes a failed job: requeue (incremented attempt) until <see cref="BackgroundJobsOptions.MaxRetryAttempts"/>,
    /// then dead-letter (or drop). Does NOT ack — the caller acks the original only after this succeeds, so a failure
    /// here leaves the original unacked for redelivery rather than losing it.
    /// </summary>
    private async Task HandleFailureAsync(IChannel channel, JobEnvelope envelope, string jobId, Exception ex)
    {
        // MaxRetryAttempts counts retries: the first run is Attempt 1, so requeue while Attempt <= MaxRetryAttempts
        // (e.g. default 3 → up to 3 re-publications), then dead-letter. Floor at 0 (not 1) so MaxRetryAttempts = 0
        // disables retries entirely — dead-letter on the first failure — matching Hangfire's AutomaticRetry { Attempts = 0 }.
        var maxAttempts = Math.Max(0, _jobsOptions.MaxRetryAttempts);

        if (envelope.Attempt <= maxAttempts)
        {
            _logger.LogWarning(ex, "Job {JobId} ({JobType}) failed on attempt {Attempt}; requeuing (max {MaxAttempts} retries)",
                jobId, envelope.JobType, envelope.Attempt, maxAttempts);

            var retry = envelope with { Attempt = envelope.Attempt + 1 };
            var workQueue = string.IsNullOrEmpty(retry.Queue) ? _jobsOptions.DefaultQueue : retry.Queue!;
            var body = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(retry, JobJsonSettings.Default));
            var properties = new BasicProperties
            {
                MessageId = jobId,
                Persistent = true,
                ContentType = "application/json",
            };

            var delaySeconds = Math.Max(0, _rabbitMqOptions.RetryDelaySeconds);
            if (delaySeconds == 0)
            {
                // Immediate retry: re-publish straight to the work queue.
                await RunChannelOpAsync(async () => await channel.BasicPublishAsync(exchange: string.Empty, routingKey: workQueue,
                    mandatory: false, basicProperties: properties, body: body));
            }
            else
            {
                // Delayed retry (backoff): park the message in a per-queue TTL "delay" queue that dead-letters back to
                // the work queue when the TTL expires — a fixed delay without the consumer sleeping, so a burst of
                // poison messages can't hot-loop. The delay is encoded in the queue name so changing RetryDelaySeconds
                // creates a new delay queue instead of failing an incompatible redeclare of the existing one.
                var delayQueue = $"{workQueue}.retry.{delaySeconds}s";
                await RunChannelOpAsync(async () =>
                {
                    await channel.QueueDeclareAsync(
                        queue: delayQueue,
                        durable: true,
                        exclusive: false,
                        autoDelete: false,
                        arguments: new Dictionary<string, object?>
                        {
                            ["x-message-ttl"] = delaySeconds * 1000,
                            ["x-dead-letter-exchange"] = string.Empty,
                            ["x-dead-letter-routing-key"] = workQueue,
                        });

                    await channel.BasicPublishAsync(exchange: string.Empty, routingKey: delayQueue,
                        mandatory: false, basicProperties: properties, body: body);
                });
            }
        }
        else if (_rabbitMqOptions.UseDeadLetterQueue)
        {
            _logger.LogError(ex, "Job {JobId} ({JobType}) failed on final attempt {Attempt}/{MaxAttempts}; dead-lettering",
                jobId, envelope.JobType, envelope.Attempt, maxAttempts);

            await PublishToDeadLetterAsync(channel, envelope, jobId, ex);
        }
        else
        {
            _logger.LogError(ex, "Job {JobId} ({JobType}) failed on final attempt {Attempt}/{MaxAttempts}; dropping (dead-letter queue disabled)",
                jobId, envelope.JobType, envelope.Attempt, maxAttempts);
        }
    }

    private async Task PublishToDeadLetterAsync(IChannel channel, JobEnvelope envelope, string jobId, Exception ex)
    {
        var workQueue = string.IsNullOrEmpty(envelope.Queue) ? _jobsOptions.DefaultQueue : envelope.Queue!;
        var deadLetterQueue = workQueue + _rabbitMqOptions.DeadLetterQueueSuffix;

        // Application-level dead-lettering: declare a durable DLQ and publish the envelope to it via the default
        // exchange. (Done explicitly rather than via x-dead-letter-exchange args so we don't have to redeclare the
        // work queue with different arguments, which RabbitMQ rejects for an existing queue.)
        var properties = new BasicProperties
        {
            MessageId = jobId,
            Persistent = true,
            ContentType = "application/json",
            Headers = new Dictionary<string, object?>
            {
                ["x-original-queue"] = workQueue,
                ["x-attempts"] = envelope.Attempt,
                ["x-death-reason"] = ex.Message,
            },
        };

        var body = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(envelope, JobJsonSettings.Default));

        await RunChannelOpAsync(async () =>
        {
            await channel.QueueDeclareAsync(
                queue: deadLetterQueue,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: null);

            await channel.BasicPublishAsync(exchange: string.Empty, routingKey: deadLetterQueue, mandatory: false,
                basicProperties: properties, body: body);
        });

        _logger.LogWarning("Job {JobId} routed to dead-letter queue '{DeadLetterQueue}'", jobId, deadLetterQueue);
    }

    private List<string> GetQueues()
    {
        var queues = new List<string> { _jobsOptions.DefaultQueue };
        queues.AddRange(_rabbitMqOptions.Queues);

        return queues
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(x => x.Trim())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken);

        if (_channel is not null)
        {
            await _channel.CloseAsync(cancellationToken);
            await _channel.DisposeAsync();
            _channel = null;
        }

        _channelLock.Dispose();
    }
}
