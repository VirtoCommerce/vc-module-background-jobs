#nullable enable
using Microsoft.Extensions.DependencyInjection;
using VirtoCommerce.BackgroundJobs.Conformance;
using VirtoCommerce.BackgroundJobs.Data.Recurring;
using VirtoCommerce.BackgroundJobs.InMemory.Extensions;
using VirtoCommerce.Platform.Core.DistributedLock;

namespace VirtoCommerce.BackgroundJobs.Tests.Conformance;

/// <summary>
/// Certifies the shippable <see cref="InMemoryJobEngine"/> (the <c>Provider=InMemory</c> local/testing engine)
/// against the full engine conformance suite. It needs no external infrastructure, so — unlike the Hangfire/RabbitMQ
/// fixtures — it always runs in CI.
/// </summary>
public sealed class InMemoryEngineConformanceFixture : JobEngineConformanceFixture
{
    public override EngineCapabilities Capabilities => new()
    {
        SupportsStatusQuery = true,
        SupportsDelete = true,
        SupportsUniqueKeyDedup = false,
        SupportsQueueRouting = true,
        SupportsRetry = true,
        SupportsRecurringScheduler = true,
    };

    protected override bool TryConfigureEngine(IServiceCollection services, out string? unavailableReason)
    {
        unavailableReason = null;
        services.AddInMemoryJobEngine();

        // Recurring via the engine-agnostic in-process scheduler + a no-op lock (single-process conformance run).
        services.AddSingleton<IDistributedLockService, ConformanceNoLockService>();
        services.AddInProcessRecurringScheduler();
        return true;
    }
}

public class InMemoryEngineConformanceTests(InMemoryEngineConformanceFixture fixture)
    : JobEngineConformanceTests<InMemoryEngineConformanceFixture>(fixture);
