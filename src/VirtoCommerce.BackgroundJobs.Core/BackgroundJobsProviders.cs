namespace VirtoCommerce.BackgroundJobs.Core;

/// <summary>
/// Canonical names of the built-in background-job providers. Shared here (in Core) so the engine modules, the host
/// selector, settings, and health checks all compare against the same literal instead of re-typing the string.
/// Custom engine modules define their own provider name (e.g. <c>"GoogleCloudTasks"</c>).
/// </summary>
public static class BackgroundJobsProviders
{
    public const string Hangfire = "Hangfire";

    public const string RabbitMq = "RabbitMQ";

    /// <summary>Infrastructure-free in-process engine for local development / testing (non-durable, single-process).</summary>
    public const string InMemory = "InMemory";
}
