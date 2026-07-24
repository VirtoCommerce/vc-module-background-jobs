#nullable enable
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs.Core.Admin;

/// <summary>
/// Read model for the admin troubleshooting API: enumerates the registered background-job handlers and the recurring
/// schedules with their effective cron and last/next run. Engine-agnostic — composes the DI registries
/// (<c>IEnumerable&lt;BackgroundJobDescriptor&gt;</c>, <c>IEnumerable&lt;RecurringJobRegistration&gt;</c>) with settings
/// and the recurring state store.
/// </summary>
public interface IBackgroundJobsAdminQuery
{
    /// <summary>All registered handlers, de-duplicated by name.</summary>
    IReadOnlyList<RegisteredJobInfo> GetRegisteredJobs();

    /// <summary>
    /// Finds a registered handler by its friendly name (case-insensitive), returning the raw descriptor (with the
    /// assembly-qualified handler/payload types needed to resolve and enqueue it), or null when unknown.
    /// </summary>
    BackgroundJobDescriptor? FindRegisteredJob(string name);

    /// <summary>All declared recurring jobs with their resolved schedule, de-duplicated by id.</summary>
    Task<IReadOnlyList<RecurringJobInfo>> GetRecurringJobsAsync(CancellationToken cancellationToken = default);
}
