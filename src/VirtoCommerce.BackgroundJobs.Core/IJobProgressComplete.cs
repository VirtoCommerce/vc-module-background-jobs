using System.Threading.Tasks;

namespace VirtoCommerce.BackgroundJobs;

/// <summary>
/// The completion seam of a job's progress channel: the dispatcher closes the job's notification through it,
/// stating how the job ended.
/// </summary>
public interface IJobProgressComplete
{
    /// <param name="outcome">A <see cref="JobOutcome"/> constant.</param>
    /// <param name="error">The failure message, or null when the job did not fail.</param>
    Task Complete(string outcome, string? error);
}
