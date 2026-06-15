using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using RabbitMQ.Client;
using VirtoCommerce.BackgroundJobs.Core.Models;
using VirtoCommerce.BackgroundJobs.Core.Services;
using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs.RabbitMQ;

/// <summary>
/// RabbitMQ implementation of <see cref="IJobEngine"/>. Publishes the serialized <see cref="JobEnvelope"/> to a
/// durable queue via the default exchange; the in-process <see cref="RabbitMqJobConsumer"/> drains it and runs the
/// handler through <see cref="IJobDispatcher"/>.
/// <para>
/// RabbitMQ has no native job store, so <see cref="GetStatus"/> and <see cref="Delete"/> are best-effort: status is
/// reported as <c>Unknown</c> and delete is unsupported (progress is observed over SignalR instead). It does NOT
/// implement <see cref="IExpressionJobEngine"/> — delegates cannot be serialized onto a queue.
/// </para>
/// </summary>
public sealed class RabbitMqJobEngine(IRabbitMqConnectionProvider connectionProvider, ILogger<RabbitMqJobEngine> logger)
    : IJobEngine
{
    public const string ProviderNameValue = "RabbitMQ";

    public string ProviderName => ProviderNameValue;

    public async Task<string> Enqueue(JobEnvelope envelope, EnqueueOptions options, CancellationToken cancellationToken = default)
    {
        var queue = string.IsNullOrEmpty(envelope.Queue) ? "default" : envelope.Queue!;
        var jobId = Guid.NewGuid().ToString("N");

        var body = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(envelope));

        // Publisher confirmations make BasicPublishAsync complete only after the broker has accepted the message.
        // Without them the publish is fire-and-forget into the client's write pipe, and disposing this short-lived
        // channel right after would race the unflushed write — silently dropping the job.
        var channelOptions = new CreateChannelOptions(
            publisherConfirmationsEnabled: true,
            publisherConfirmationTrackingEnabled: true);

        await using var channel = await connectionProvider.CreateChannelAsync(channelOptions, cancellationToken);

        await channel.QueueDeclareAsync(
            queue: queue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null,
            cancellationToken: cancellationToken);

        var properties = new BasicProperties
        {
            MessageId = jobId,
            Persistent = true,
            ContentType = "application/json",
        };

        await channel.BasicPublishAsync(
            exchange: string.Empty,
            routingKey: queue,
            mandatory: false,
            basicProperties: properties,
            body: body,
            cancellationToken: cancellationToken);

        logger.LogInformation("Published job {JobId} ({JobType}) to RabbitMQ queue '{Queue}' (broker confirmed)", jobId, envelope.JobType, queue);

        return jobId;
    }

    /// <summary>
    /// Best-effort status: RabbitMQ keeps no job ledger, so once a message is published its lifecycle is not
    /// queryable by id. Returns <c>Unknown</c>/not-completed. Use progress push-notifications to observe a job.
    /// </summary>
    public Task<Job?> GetStatus(string jobId, CancellationToken cancellationToken = default)
    {
        var result = new Job
        {
            Id = jobId,
            State = "Unknown",
            Completed = false,
        };

        return Task.FromResult<Job?>(result);
    }

    /// <summary>Not supported on RabbitMQ — a published message cannot be recalled by id. Always returns false.</summary>
    public Task<bool> Delete(string jobId, CancellationToken cancellationToken = default) => Task.FromResult(false);
}
