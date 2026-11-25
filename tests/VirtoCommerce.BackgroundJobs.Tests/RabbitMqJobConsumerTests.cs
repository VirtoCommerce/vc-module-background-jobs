#nullable enable
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Newtonsoft.Json;
using RabbitMQ.Client;
using VirtoCommerce.BackgroundJobs.Core;
using VirtoCommerce.BackgroundJobs.Core.Cancellation;
using VirtoCommerce.BackgroundJobs.Core.Models;
using VirtoCommerce.BackgroundJobs.Data.Cancellation;
using VirtoCommerce.BackgroundJobs.RabbitMQ;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.PushNotifications;
using Xunit;

namespace VirtoCommerce.BackgroundJobs.Tests;

/// <summary>
/// Drives <see cref="RabbitMqJobConsumer"/> through a fake channel to see what it does to the job's cancellation flag.
/// The flag belongs to the job: a delivery that cannot know no other delivery follows (a retry was published, or its
/// own ack failed so the broker redelivers it) must leave it, or the next delivery runs a job that was canceled.
/// </summary>
public sealed class RabbitMqJobConsumerTests
{
    private const string _jobId = "job-1";

    [Fact]
    public async Task Delivery_FailedAttemptWithRetryPublished_AckFails_KeepsCancelFlagAndDiscardsRedelivery()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = new InMemoryJobCancellationStore();
        var dispatcher = new FailingDispatcher(onDispatch: () => store.RequestCancel(_jobId));
        var channel = new FakeChannel { AckThrows = true };
        await using var host = await ConsumerHost.StartAsync(channel, dispatcher, store, ct);

        // Attempt 1 of 4 fails after a cancel was requested: the retry is published, then the ack of the original fails.
        await host.DeliverAsync(deliveryTag: 1, attempt: 1, ct);

        Assert.Equal(1, dispatcher.Runs);
        Assert.Equal(1, channel.Published);
        Assert.True(await store.IsCancelRequested(_jobId, ct), "the retry and the redelivery of the original still have to find the flag.");

        // The broker redelivers the original, and the retry arrives: both are discarded, neither runs the handler.
        channel.AckThrows = false;
        await host.DeliverAsync(deliveryTag: 2, attempt: 1, ct);
        await host.DeliverAsync(deliveryTag: 3, attempt: 2, ct);

        Assert.Equal(1, dispatcher.Runs);
        Assert.Equal(2, channel.Acked);
        Assert.True(await store.IsCancelRequested(_jobId, ct), "a discard leaves the flag: more deliveries of the job may follow.");
    }

    // The same rule from the other side: a delivery that settled the job (acked, nothing follows) is what clears the flag.
    [Fact]
    public async Task Delivery_FinalFailure_Acked_ClearsCancelFlag()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = new InMemoryJobCancellationStore();
        var dispatcher = new FailingDispatcher(onDispatch: () => store.RequestCancel(_jobId));
        var channel = new FakeChannel();
        await using var host = await ConsumerHost.StartAsync(channel, dispatcher, store, ct);

        // Attempt 4 is the last of 4 (3 retries): dead-lettered, original acked, no follow-up the flag could be needed by.
        await host.DeliverAsync(deliveryTag: 1, attempt: 4, ct);

        Assert.Equal(1, channel.Acked);
        Assert.False(await store.IsCancelRequested(_jobId, ct));
    }

    private sealed class FailingDispatcher(Func<Task> onDispatch) : IJobDispatcher
    {
        public int Runs { get; private set; }

        public async Task Dispatch(JobEnvelope envelope, IJobExecutionContext context, CancellationToken cancellationToken = default)
        {
            Runs++;
            await onDispatch();

            throw new InvalidOperationException("handler failed");
        }
    }

    // The channel surface the consumer uses, over a Moq IChannel: setup, consume (captures the consumer), ack, publish.
    private sealed class FakeChannel
    {
        public Mock<IChannel> Mock { get; } = new();
        public IAsyncBasicConsumer? Consumer { get; private set; }
        public bool AckThrows { get; set; }
        public int Acked { get; private set; }
        public int Published { get; private set; }

        public FakeChannel()
        {
            Mock.SetupGet(x => x.IsOpen).Returns(true);

            Mock.Setup(x => x.BasicConsumeAsync(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<bool>(),
                    It.IsAny<IDictionary<string, object?>?>(), It.IsAny<IAsyncBasicConsumer>(), It.IsAny<CancellationToken>()))
                .Callback<string, bool, string, bool, bool, IDictionary<string, object?>?, IAsyncBasicConsumer, CancellationToken>(
                    (_, _, _, _, _, _, consumer, _) => Consumer = consumer)
                .ReturnsAsync("tag");

            Mock.Setup(x => x.BasicAckAsync(It.IsAny<ulong>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
                .Returns(() =>
                {
                    if (AckThrows)
                    {
                        throw new InvalidOperationException("ack failed: channel closed");
                    }

                    Acked++;

                    return ValueTask.CompletedTask;
                });

            Mock.Setup(x => x.BasicPublishAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<BasicProperties>(),
                    It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
                .Returns(() =>
                {
                    Published++;

                    return ValueTask.CompletedTask;
                });
        }
    }

    private sealed class ConsumerHost : IAsyncDisposable
    {
        private readonly RabbitMqJobConsumer _consumer;
        private readonly FakeChannel _channel;

        private ConsumerHost(RabbitMqJobConsumer consumer, FakeChannel channel)
        {
            _consumer = consumer;
            _channel = channel;
        }

        public static async Task<ConsumerHost> StartAsync(FakeChannel channel, IJobDispatcher dispatcher, IJobCancellationStore store, CancellationToken ct)
        {
            var connection = new Mock<IRabbitMqConnectionProvider>();
            connection.Setup(x => x.CreateChannelAsync(It.IsAny<CreateChannelOptions?>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(channel.Mock.Object);

            var consumer = new RabbitMqJobConsumer(
                connection.Object,
                dispatcher,
                new Mock<IPushNotificationManager>().Object,
                store,
                Options.Create(new RabbitMqOptions { RetryDelaySeconds = 0 }),
                Options.Create(new BackgroundJobsOptions { DefaultQueue = "default", MaxRetryAttempts = 3 }),
                NullLogger<RabbitMqJobConsumer>.Instance);

            await consumer.StartAsync(ct);

            // The consumer subscribes from its background loop: wait for it to hand its consumer to the channel.
            for (var i = 0; i < 400 && channel.Consumer is null; i++)
            {
                await Task.Delay(25, ct);
            }

            Assert.NotNull(channel.Consumer);

            return new ConsumerHost(consumer, channel);
        }

        public Task DeliverAsync(ulong deliveryTag, int attempt, CancellationToken ct)
        {
            var envelope = new JobEnvelope { JobType = "t", PayloadType = "t", PayloadJson = "{}", Attempt = attempt, Queue = "default" };
            var body = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(envelope, JobJsonSettings.Default));

            return _channel.Consumer!.HandleBasicDeliverAsync(
                "tag", deliveryTag, redelivered: false, exchange: string.Empty, routingKey: "default",
                new BasicProperties { MessageId = _jobId }, body, ct);
        }

        public async ValueTask DisposeAsync()
        {
            await _consumer.StopAsync(CancellationToken.None);
            _consumer.Dispose();
        }
    }
}
