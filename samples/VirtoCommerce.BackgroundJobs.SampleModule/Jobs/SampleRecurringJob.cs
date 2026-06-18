using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs.SampleModule.Jobs;

/// <summary>
/// Sample recurring job — an ordinary <see cref="IBackgroundJobHandler{TPayload}"/>. It is scheduled by
/// <c>AddRecurringJob</c> in <see cref="Module.Initialize"/>; the active engine (Hangfire or RabbitMQ) fires it on
/// the configured cron and runs this handler on a worker. There is no recurring-specific handler contract.
/// </summary>
public class SampleRecurringJob(ILogger<SampleRecurringJob> logger) : IBackgroundJobHandler<SampleRecurringJobPayload>
{
    public Task Execute(SampleRecurringJobPayload payload, IJobExecutionContext context, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("SampleRecurringJob fired at {Utc:o} (job id {JobId}).", DateTime.UtcNow, context.JobId);
        return Task.CompletedTask;
    }
}
