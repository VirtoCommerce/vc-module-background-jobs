namespace VirtoCommerce.BackgroundJobs.GoogleCloudTasks;

/// <summary>
/// Settings for the Google Cloud Tasks background-job engine. Bound from the top-level configuration section
/// <c>VirtoCommerce:GoogleCloudTasks</c> (provider-specific, kept separate from the engine-agnostic
/// <c>VirtoCommerce:BackgroundJobs</c> selector).
/// </summary>
public class GoogleCloudTasksOptions
{
    /// <summary>GCP project id that owns the Cloud Tasks queue.</summary>
    public string ProjectId { get; set; } = string.Empty;

    /// <summary>Queue location/region, e.g. <c>us-central1</c>.</summary>
    public string LocationId { get; set; } = string.Empty;

    /// <summary>Cloud Tasks queue id that tasks are created in.</summary>
    public string QueueId { get; set; } = string.Empty;

    // Credentials are resolved via Application Default Credentials (ADC): on GCP use Workload Identity or the
    // instance's attached service account; locally point the standard GOOGLE_APPLICATION_CREDENTIALS environment
    // variable at a service-account JSON key file. No credential path option is exposed here on purpose — ADC is the
    // recommended, secret-rotation-friendly mechanism.

    /// <summary>
    /// Publicly reachable base URL of this platform that Cloud Tasks POSTs back to, e.g.
    /// <c>https://admin.example.com</c>. The engine appends the callback path
    /// (<see cref="GoogleCloudTasksConstants.CallbackPath"/>). Must terminate at an instance able to run handlers.
    /// </summary>
    public string CallbackBaseUrl { get; set; } = string.Empty;

    /// <summary>
    /// Service-account email Cloud Tasks uses to sign the OIDC token on the push request. The callback verifies the
    /// token was issued for this account.
    /// </summary>
    public string OidcServiceAccountEmail { get; set; } = string.Empty;

    /// <summary>
    /// OIDC audience the push token is minted for and validated against. Defaults to <see cref="CallbackBaseUrl"/>
    /// when empty.
    /// </summary>
    public string? OidcAudience { get; set; }

    /// <summary>
    /// URL of the Cloud Tasks queue in the GCP console, surfaced as a developer tool in the platform admin. When not
    /// set it is composed from <see cref="ProjectId"/>/<see cref="LocationId"/>/<see cref="QueueId"/>.
    /// </summary>
    public string? ConsoleUri { get; set; }
}
