#nullable enable
using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using VirtoCommerce.BackgroundJobs.Core;
using VirtoCommerce.BackgroundJobs.Core.Services;
using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs.Web.Infrastructure.HealthChecks;

/// <summary>
/// Reports background-job readiness on the platform's <c>/health</c> endpoint. Unhealthy when no
/// <see cref="IJobEngine"/> is registered for the configured provider (enqueue would fail); Degraded when the active
/// engine's provider name doesn't match the configured one; otherwise Healthy.
/// </summary>
public sealed class BackgroundJobsHealthCheck : IHealthCheck
{
    private readonly IConfiguration _configuration;

    // Optional: the active engine module registers it. Null when no engine is installed — that's the unhealthy case.
    private readonly IJobEngine? _engine;

    public BackgroundJobsHealthCheck(IConfiguration configuration, IJobEngine? engine = null)
    {
        _configuration = configuration;
        _engine = engine;
    }

    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var provider = _configuration.GetBackgroundJobsProvider();

        var result = BackgroundJobsEngineStatusEvaluator.Evaluate(_engine, provider) switch
        {
            BackgroundJobsEngineStatus.NoEngine => new HealthCheckResult(
                context.Registration.FailureStatus,
                $"No background-job engine is registered for provider '{provider}'. {BackgroundJobEngineNotInstalledException.DefaultMessage}"),

            BackgroundJobsEngineStatus.ProviderMismatch => HealthCheckResult.Degraded(
                $"Configured provider '{provider}' does not match the active engine '{_engine!.ProviderName}'."),

            _ => HealthCheckResult.Healthy($"Active background-job engine '{_engine!.ProviderName}'."),
        };

        return Task.FromResult(result);
    }
}
