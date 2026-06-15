using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs.SampleModule.Jobs;

/// <summary>
/// Sample background-job handler. Reports progress step-by-step so you can watch it in the admin
/// notification UI when enqueued with <c>ReportProgress = true</c>.
/// </summary>
public class SampleJob(ILogger<SampleJob> logger) : IBackgroundJobHandler<SampleJobPayload>
{
    public async Task Execute(SampleJobPayload payload, IJobExecutionContext context, CancellationToken cancellationToken = default)
    {
        var steps = payload.StepCount <= 0 ? 1 : payload.StepCount;

        logger.LogInformation("SampleJob {JobId} started: {Message}", context.JobId, payload.Message);

        for (var i = 1; i <= steps; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            await context.Progress.Report(
                new JobProgressInfo
                {
                    Message = $"Step {i} of {steps}: {payload.Message}",
                    ProcessedCount = i,
                    TotalCount = steps,
                },
                cancellationToken);

            await Task.Delay(1000, cancellationToken);
        }

        logger.LogInformation("SampleJob {JobId} finished: {Message}", context.JobId, payload.Message);
    }
}
