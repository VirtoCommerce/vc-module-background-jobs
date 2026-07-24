using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Apis.Auth;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace VirtoCommerce.BackgroundJobs.GoogleCloudTasks;

/// <summary>
/// Default validator: verifies the Google-signed OIDC token (signature, expiry, issuer) against the configured
/// audience and confirms it was minted for the expected service account. Google's public certs are fetched and
/// cached by <see cref="GoogleJsonWebSignature"/>.
/// </summary>
public sealed class GoogleCloudTasksTokenValidator : IGoogleCloudTasksTokenValidator
{
    private const string BearerPrefix = "Bearer ";

    private readonly GoogleCloudTasksOptions _options;
    private readonly ILogger<GoogleCloudTasksTokenValidator> _logger;

    public GoogleCloudTasksTokenValidator(IOptions<GoogleCloudTasksOptions> options, ILogger<GoogleCloudTasksTokenValidator> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public async Task<bool> ValidateAsync(string? authorizationHeader, CancellationToken cancellationToken = default)
    {
        // Fail closed: without a configured service account we cannot prove the caller is *our* Cloud Tasks queue.
        // (Validating only the audience would accept any Google-signed token whose aud matches the public callback
        // URL, including one minted by an unrelated GCP project.) Refuse rather than run anonymous jobs.
        if (string.IsNullOrEmpty(_options.OidcServiceAccountEmail))
        {
            _logger.LogError("Cloud Tasks callback rejected: OidcServiceAccountEmail is not configured; refusing to accept push requests.");
            return false;
        }

        if (string.IsNullOrWhiteSpace(authorizationHeader))
        {
            _logger.LogWarning("Cloud Tasks callback rejected: missing Authorization header.");
            return false;
        }

        var token = authorizationHeader.StartsWith(BearerPrefix, StringComparison.OrdinalIgnoreCase)
            ? authorizationHeader[BearerPrefix.Length..].Trim()
            : authorizationHeader.Trim();

        try
        {
            var audience = string.IsNullOrEmpty(_options.OidcAudience)
                ? GoogleCloudTasksRequestBuilder.BuildCallbackUrl(_options)
                : _options.OidcAudience!;

            var settings = new GoogleJsonWebSignature.ValidationSettings { Audience = [audience] };
            var payload = await GoogleJsonWebSignature.ValidateAsync(token, settings);

            if (!string.Equals(payload.Email, _options.OidcServiceAccountEmail, StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogWarning("Cloud Tasks callback rejected: token subject '{Email}' does not match the configured service account.", payload.Email);
                return false;
            }

            return true;
        }
        // A bad/expired/wrong-audience token (InvalidJwtException) or a malformed bearer value (ArgumentException) is a
        // definitive rejection -> return false so the controller answers 401 (non-retryable). Any OTHER exception
        // (e.g. a transient failure fetching Google's signing certs) is deliberately NOT caught here: it propagates so
        // the controller returns 5xx and Cloud Tasks retries, rather than dropping the job as unauthenticated.
        catch (Exception ex) when (ex is InvalidJwtException or ArgumentException)
        {
            _logger.LogWarning(ex, "Cloud Tasks callback rejected: OIDC token validation failed.");
            return false;
        }
    }
}
