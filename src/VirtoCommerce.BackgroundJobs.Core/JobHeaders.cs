namespace VirtoCommerce.BackgroundJobs;

/// <summary>
/// Well-known <see cref="Core.Models.JobEnvelope.Headers"/> keys the engine and dispatcher understand. Kept small and
/// engine-agnostic: producers stamp them, the shared dispatcher reads them for telemetry.
/// </summary>
public static class JobHeaders
{
    /// <summary>UTC ticks (<see cref="System.DateTime.Ticks"/>) captured when the job was enqueued; the dispatcher
    /// derives queue latency (enqueue → dispatch) from it.</summary>
    public const string EnqueuedAt = "vc-enqueued-at";

    /// <summary>Optional correlation id for a batch of enqueues (e.g. a load-test run), so all jobs from one run can
    /// be filtered together in telemetry. Set via <see cref="JobEnqueueContext"/>.</summary>
    public const string RunId = "vc-run-id";
}
