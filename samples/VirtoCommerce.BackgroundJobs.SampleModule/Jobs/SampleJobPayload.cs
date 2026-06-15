using VirtoCommerce.Platform.Core.Common;

namespace VirtoCommerce.BackgroundJobs.SampleModule.Jobs;

/// <summary>
/// Sample job payload. A <see cref="ValueObject"/> created via <c>AbstractTypeFactory</c> so a partner module
/// could extend it (add fields) without forking.
/// </summary>
public class SampleJobPayload : ValueObject
{
    public string? Message { get; set; }

    /// <summary>Number of progress steps the job will report.</summary>
    public int StepCount { get; set; } = 3;
}
