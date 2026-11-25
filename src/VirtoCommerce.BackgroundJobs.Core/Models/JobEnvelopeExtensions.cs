namespace VirtoCommerce.BackgroundJobs.Core.Models;

public static class JobEnvelopeExtensions
{
    /// <summary>
    /// Whether a failure of this attempt ends the job. An unknown <see cref="JobEnvelope.TotalAttempts"/> counts as
    /// final: with no budget to compare against, no retry can be promised.
    /// </summary>
    public static bool IsFinalAttempt(this JobEnvelope envelope)
        => envelope.TotalAttempts is null || envelope.Attempt >= envelope.TotalAttempts;
}
