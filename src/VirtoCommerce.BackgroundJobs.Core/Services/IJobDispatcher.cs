using System.Threading;
using System.Threading.Tasks;
using VirtoCommerce.BackgroundJobs.Core.Models;
using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs.Core.Services;

/// <summary>
/// Shared execution path used by every engine (the Hangfire job body, the RabbitMQ consumer, a future push
/// callback): deserialize the payload, resolve the <c>IBackgroundJob&lt;T&gt;</c> handler from DI, and run it.
/// </summary>
public interface IJobDispatcher
{
    Task Dispatch(JobEnvelope envelope, IJobExecutionContext context, CancellationToken cancellationToken = default);
}
