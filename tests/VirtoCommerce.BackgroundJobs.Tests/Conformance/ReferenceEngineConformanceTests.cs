#nullable enable
using Microsoft.Extensions.DependencyInjection;
using VirtoCommerce.BackgroundJobs.Conformance;
using VirtoCommerce.BackgroundJobs.Core.Services;
using VirtoCommerce.BackgroundJobs.Data.Recurring;
using VirtoCommerce.Platform.Core.DistributedLock;

namespace VirtoCommerce.BackgroundJobs.Tests.Conformance;

/// <summary>
/// Certifies the in-process <see cref="ReferenceJobEngine"/>. This needs no external infrastructure, so it always
/// runs — it is the kit's own smoke test (proving the abstract suite is correct) and the copy-paste template a new
/// engine author starts from: implement <c>IJobEngine</c>, then a fixture like this declaring your capabilities and
/// registering your engine + worker.
/// </summary>
public sealed class ReferenceEngineConformanceFixture : JobEngineConformanceFixture
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
        services.AddSingleton<IJobEngine, ReferenceJobEngine>();

        // Recurring via the engine-agnostic in-process scheduler + a no-op lock (single-process conformance run).
        services.AddSingleton<IDistributedLockService, ConformanceNoLockService>();
        services.AddInProcessRecurringScheduler();
        return true;
    }
}

public class ReferenceEngineConformanceTests(ReferenceEngineConformanceFixture fixture)
    : JobEngineConformanceTests<ReferenceEngineConformanceFixture>(fixture);
