namespace VirtoCommerce.BackgroundJobs.Core.Models;

public static class JobEnvelopeExtensions
{
    /// <summary>Whether a failure of this attempt ends the job; an unknown <see cref="JobEnvelope.TotalAttempts"/> counts as final.</summary>
    public static bool IsFinalAttempt(this JobEnvelope envelope)
        => envelope.TotalAttempts is null || envelope.Attempt >= envelope.TotalAttempts;
}
