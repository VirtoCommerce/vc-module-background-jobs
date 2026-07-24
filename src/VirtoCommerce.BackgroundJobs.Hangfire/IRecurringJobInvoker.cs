#nullable enable
using System.Threading.Tasks;

namespace VirtoCommerce.BackgroundJobs.Hangfire;

/// <summary>
/// Bridges a native Hangfire recurring job to a declared <c>RecurringJobRegistration</c>. Hangfire schedules a
/// recurring call to <see cref="Run"/>; on each occurrence it looks up the registration by id and enqueues its
/// payload through the engine-agnostic facade.
/// </summary>
public interface IRecurringJobInvoker
{
    Task Run(string recurringJobId);
}
