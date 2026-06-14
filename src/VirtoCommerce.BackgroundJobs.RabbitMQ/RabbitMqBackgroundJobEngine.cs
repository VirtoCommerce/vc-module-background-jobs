using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace VirtoCommerce.BackgroundJobs.RabbitMQ;

/// <summary>
/// Foundation for the RabbitMQ background-job engine.
/// <para>
/// This is intentionally a placeholder: the real engine will implement the message-based job-engine port
/// (<c>IJobEngine</c>) once that contract is introduced in the platform, publishing serialized job envelopes
/// and consuming them on workers. For now it only proves the connection plumbing by publishing a raw message
/// to a durable queue.
/// </para>
/// </summary>
public class RabbitMqBackgroundJobEngine
{
    private readonly IRabbitMqConnectionProvider _connectionProvider;
    private readonly ILogger<RabbitMqBackgroundJobEngine> _logger;

    public RabbitMqBackgroundJobEngine(
        IRabbitMqConnectionProvider connectionProvider,
        ILogger<RabbitMqBackgroundJobEngine> logger)
    {
        _connectionProvider = connectionProvider;
        _logger = logger;
    }

    /// <summary>
    /// Declares a durable queue (if needed) and publishes a message body to it via the default exchange.
    /// </summary>
    public async Task PublishAsync(string queue, ReadOnlyMemory<byte> body, CancellationToken cancellationToken = default)
    {
        await using var channel = await _connectionProvider.CreateChannelAsync(cancellationToken);

        await channel.QueueDeclareAsync(
            queue: queue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null,
            cancellationToken: cancellationToken);

        await channel.BasicPublishAsync(
            exchange: string.Empty,
            routingKey: queue,
            body: body,
            cancellationToken: cancellationToken);

        _logger.LogDebug("Published {Bytes} bytes to RabbitMQ queue '{Queue}'", body.Length, queue);
    }
}
