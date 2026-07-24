using System.Threading;
using System.Threading.Tasks;

namespace VirtoCommerce.BackgroundJobs.GoogleCloudTasks;

/// <summary>
/// Validates the OIDC bearer token Cloud Tasks signs onto each push request, so only the configured service account
/// can invoke the callback. Swappable via DI (e.g. to plug in a different verification policy or a test double).
/// </summary>
public interface IGoogleCloudTasksTokenValidator
{
    /// <summary>Returns true when the request's <c>Authorization</c> header carries a valid push token.</summary>
    Task<bool> ValidateAsync(string? authorizationHeader, CancellationToken cancellationToken = default);
}
