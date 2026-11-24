namespace VirtoCommerce.BackgroundJobs;

/// <summary>
/// How a job ended, as written to <c>JobProgressPushNotification.Outcome</c>. String constants rather than an enum:
/// SignalR serializes enums as numbers, and the admin UI reads the name.
/// </summary>
public static class JobOutcome
{
    public const string Succeeded = "Succeeded";
    public const string Failed = "Failed";
    public const string Canceled = "Canceled";
}
