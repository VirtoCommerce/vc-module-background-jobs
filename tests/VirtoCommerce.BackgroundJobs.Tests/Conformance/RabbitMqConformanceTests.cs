#nullable enable
using System;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using VirtoCommerce.BackgroundJobs.Conformance;
using VirtoCommerce.BackgroundJobs.Data.Recurring;
using VirtoCommerce.BackgroundJobs.RabbitMQ.Extensions;
using VirtoCommerce.Platform.Core.DistributedLock;

namespace VirtoCommerce.BackgroundJobs.Tests.Conformance;

/// <summary>
/// Certifies the real RabbitMQ engine + in-process consumer against a real broker. Set the broker connection via the
/// <c>VC_CONFORMANCE_RABBITMQ</c> environment variable (an AMQP uri, e.g.
/// <c>amqp://guest:guest@localhost:5672/</c>); when it is unset the suite skips with that message. RabbitMQ keeps no
/// job ledger, so status/delete/expression are unsupported (the suite asserts the documented fallbacks).
/// </summary>
public sealed class RabbitMqConformanceFixture : JobEngineConformanceFixture
{
    public const string ConnectionEnvironmentVariable = "VC_CONFORMANCE_RABBITMQ";

    public override EngineCapabilities Capabilities => new()
    {
        SupportsStatusQuery = false,
        SupportsDelete = false,
        SupportsUniqueKeyDedup = false,
        SupportsQueueRouting = true,
        SupportsRetry = true,
        SupportsRecurringScheduler = true,
    };

    protected override bool TryConfigureEngine(IServiceCollection services, out string? unavailableReason)
    {
        var connection = Environment.GetEnvironmentVariable(ConnectionEnvironmentVariable);
        if (string.IsNullOrWhiteSpace(connection))
        {
            unavailableReason = $"set {ConnectionEnvironmentVariable} to an AMQP uri (e.g. amqp://guest:guest@localhost:5672/) to run the RabbitMQ conformance suite.";
            return false;
        }

        unavailableReason = null;

        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["VirtoCommerce:BackgroundJobs:Provider"] = "RabbitMQ",
                ["VirtoCommerce:BackgroundJobs:MaxRetryAttempts"] = "3",
                ["VirtoCommerce:RabbitMQ:Uri"] = connection,
                ["VirtoCommerce:RabbitMQ:Queues:0"] = ConformanceConstants.CustomQueue,
                // Pin both knobs so the run is deterministic (PrefetchCount defaults to 0 = auto-scale to CPU count).
                ["VirtoCommerce:RabbitMQ:PrefetchCount"] = "4",
                ["VirtoCommerce:RabbitMQ:ConsumerDispatchConcurrency"] = "4",
            })
            .Build();

        services.AddRabbitMqJobEngine(config); // producer side: IJobEngine + connection provider + options
        services.AddRabbitMqJobConsumer();      // worker side: the in-process consumer (a hosted service)

        // RabbitMQ has no native recurring scheduler — reuse the in-process one + a no-op lock for the run.
        services.AddSingleton<IDistributedLockService, ConformanceNoLockService>();
        services.AddInProcessRecurringScheduler();
        return true;
    }
}

public class RabbitMqConformanceTests(RabbitMqConformanceFixture fixture)
    : JobEngineConformanceTests<RabbitMqConformanceFixture>(fixture);
