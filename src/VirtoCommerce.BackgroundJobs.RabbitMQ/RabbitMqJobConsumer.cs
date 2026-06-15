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

    private readonly IRabbitMqConnectionProvider _connectionProvider;
    private readonly IJobDispatcher _dispatcher;
    private readonly IPushNotificationManager _pushNotificationManager;
    private readonly RabbitMqOptions _rabbitMqOptions;
    private readonly BackgroundJobsOptions _jobsOptions;
    private readonly ILogger<RabbitMqJobConsumer> _logger;

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
        // Connect with retry so a temporarily-unavailable broker doesn't crash platform startup.
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await StartConsumingAsync(stoppingToken);
                _logger.LogInformation("RabbitMQ job consumer started for queues: {Queues}", string.Join(", ", GetQueues()));
                break;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to start RabbitMQ job consumer; retrying in {Delay}s", _reconnectDelay.TotalSeconds);
                await Task.Delay(_reconnectDelay, stoppingToken);
            }
        }
    }

    private async Task StartConsumingAsync(CancellationToken cancellationToken)
    {
        _channel = await _connectionProvider.CreateChannelAsync(cancellationToken: cancellationToken);

        await _channel.BasicQosAsync(prefetchSize: 0, prefetchCount: _rabbitMqOptions.PrefetchCount, global: false, cancellationToken);

        var consumer = new AsyncEventingBasicConsumer(_channel);
        consumer.ReceivedAsync += OnReceivedAsync;

        foreach (var queue in GetQueues())
        {
            await _channel.QueueDeclareAsync(
                queue: queue,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: null,
                cancellationToken: cancellationToken);

            var consumerTag = await _channel.BasicConsumeAsync(queue, autoAck: false, consumer: consumer, cancellationToken: cancellationToken);
            _logger.LogInformation("RabbitMQ consumer '{ConsumerTag}' subscribed to queue '{Queue}'", consumerTag, queue);
        }
    }

    private async Task OnReceivedAsync(object sender, BasicDeliverEventArgs eventArgs)
    {
        var channel = _channel;
        if (channel is null)
        {
            _logger.LogWarning("RabbitMQ delivery {DeliveryTag} received but the channel is null; skipping", eventArgs.DeliveryTag);
            return;
        }

        _logger.LogInformation("RabbitMQ delivery received: tag {DeliveryTag}, routingKey '{RoutingKey}', messageId {MessageId}, {Bytes} bytes",
            eventArgs.DeliveryTag, eventArgs.RoutingKey, eventArgs.BasicProperties.MessageId, eventArgs.Body.Length);

        var json = Encoding.UTF8.GetString(eventArgs.Body.Span);
        JobEnvelope? envelope;
        try
        {
            envelope = JsonConvert.DeserializeObject<JobEnvelope>(json);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Discarding malformed RabbitMQ job message (delivery tag {DeliveryTag})", eventArgs.DeliveryTag);
            await channel.BasicAckAsync(eventArgs.DeliveryTag, multiple: false);
            return;
        }

        if (envelope is null)
        {
            await channel.BasicAckAsync(eventArgs.DeliveryTag, multiple: false);
            return;
        }

        var jobId = eventArgs.BasicProperties.MessageId ?? string.Empty;

        try
        {
            IJobProgress progress = string.IsNullOrEmpty(envelope.ProgressNotificationId)
                ? NoOpJobProgress.Instance
                : new PushNotificationJobProgress(_pushNotificationManager, envelope.ProgressNotificationId!, envelope.UserName);

            var context = new JobExecutionContext(jobId, progress, envelope.Headers);

            _logger.LogInformation("Dispatching job {JobId} ({JobType})", jobId, envelope.JobType);

            await _dispatcher.Dispatch(envelope, context, CancellationToken.None);

            await channel.BasicAckAsync(eventArgs.DeliveryTag, multiple: false);

            _logger.LogInformation("Completed and acked job {JobId} ({JobType})", jobId, envelope.JobType);
        }
        catch (Exception ex)
        {
            await HandleFailureAsync(channel, eventArgs, envelope, jobId, ex);
        }
    }

    private async Task HandleFailureAsync(IChannel channel, BasicDeliverEventArgs eventArgs, JobEnvelope envelope, string jobId, Exception ex)
    {
        var maxAttempts = Math.Max(1, _jobsOptions.MaxRetryAttempts);

        if (envelope.Attempt < maxAttempts)
        {
            _logger.LogWarning(ex, "Job {JobId} ({JobType}) failed on attempt {Attempt}/{MaxAttempts}; requeuing",
                jobId, envelope.JobType, envelope.Attempt, maxAttempts);

            var retry = envelope with { Attempt = envelope.Attempt + 1 };
            var queue = string.IsNullOrEmpty(retry.Queue) ? "default" : retry.Queue!;
            var body = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(retry));
            var properties = new BasicProperties
            {
                MessageId = jobId,
                Persistent = true,
                ContentType = "application/json",
            };

            await channel.BasicPublishAsync(exchange: string.Empty, routingKey: queue, mandatory: false,
                basicProperties: properties, body: body);
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

        // Always ack the original delivery: retry/dead-letter (if any) was re-published as a fresh message above, so
        // requeuing the original would double-deliver.
        await channel.BasicAckAsync(eventArgs.DeliveryTag, multiple: false);
    }

    private async Task PublishToDeadLetterAsync(IChannel channel, JobEnvelope envelope, string jobId, Exception ex)
    {
        var workQueue = string.IsNullOrEmpty(envelope.Queue) ? "default" : envelope.Queue!;
        var deadLetterQueue = workQueue + _rabbitMqOptions.DeadLetterQueueSuffix;

        // Application-level dead-lettering: declare a durable DLQ and publish the envelope to it via the default
        // exchange. (Done explicitly rather than via x-dead-letter-exchange args so we don't have to redeclare the
        // work queue with different arguments, which RabbitMQ rejects for an existing queue.)
        await channel.QueueDeclareAsync(
            queue: deadLetterQueue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null);

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

        var body = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(envelope));

        await channel.BasicPublishAsync(exchange: string.Empty, routingKey: deadLetterQueue, mandatory: false,
            basicProperties: properties, body: body);

        _logger.LogWarning("Job {JobId} routed to dead-letter queue '{DeadLetterQueue}'", jobId, deadLetterQueue);
    }

    private IEnumerable<string> GetQueues()
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
    }
}
