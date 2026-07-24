using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using VirtoCommerce.BackgroundJobs.Core.Models;
using VirtoCommerce.BackgroundJobs;

namespace VirtoCommerce.BackgroundJobs.GoogleCloudTasks.Controllers;

/// <summary>
/// Push endpoint Cloud Tasks POSTs to. This is the GCT engine's processing host (a push engine has no in-process
/// consumer). It is anonymous to the platform — the request carries no admin session — and is instead authenticated
/// by the Cloud Tasks OIDC token. On success it runs the shared <see cref="IJobEnvelopeRunner"/> and returns 200 so
/// Cloud Tasks acks; on handler failure it returns 5xx so Cloud Tasks retries per the queue's retry config.
/// Handlers must be idempotent (at-least-once delivery).
/// </summary>
[ApiController]
[Route("api/background-jobs/google-cloud-tasks")]
public sealed class GoogleCloudTasksCallbackController : ControllerBase
{
    // Header Cloud Tasks sets with the full task resource name; used as the job id for the execution context.
    private const string TaskNameHeader = "X-CloudTasks-TaskName";

    private readonly IJobEnvelopeRunner _runner;
    private readonly IGoogleCloudTasksTokenValidator _tokenValidator;
    private readonly ILogger<GoogleCloudTasksCallbackController> _logger;

    public GoogleCloudTasksCallbackController(
        IJobEnvelopeRunner runner,
        IGoogleCloudTasksTokenValidator tokenValidator,
        ILogger<GoogleCloudTasksCallbackController> logger)
    {
        _runner = runner;
        _tokenValidator = tokenValidator;
        _logger = logger;
    }

    [HttpPost("callback")]
    [AllowAnonymous]
    public async Task<IActionResult> Callback(CancellationToken cancellationToken)
    {
        if (!await _tokenValidator.ValidateAsync(Request.Headers.Authorization.ToString(), cancellationToken))
        {
            return Unauthorized();
        }

        string json;
        using (var reader = new StreamReader(Request.Body, Encoding.UTF8))
        {
            json = await reader.ReadToEndAsync(cancellationToken);
        }

        JobEnvelope? envelope;
        try
        {
            envelope = JsonConvert.DeserializeObject<JobEnvelope>(json);
        }
        catch (Exception ex)
        {
            // Malformed body is a poison message: 400 is non-retryable, so Cloud Tasks drops it instead of looping.
            _logger.LogError(ex, "Discarding malformed Cloud Tasks callback body.");
            return BadRequest();
        }

        if (envelope is null)
        {
            return BadRequest();
        }

        var jobId = Request.Headers.TryGetValue(TaskNameHeader, out var taskName)
            ? taskName.ToString()
            : string.Empty;

        try
        {
            _logger.LogInformation("Dispatching Cloud Tasks job {JobId} ({JobType})", jobId, envelope.JobType);

            await _runner.Run(envelope, jobId, cancellationToken);

            return Ok();
        }
        catch (Exception ex)
        {
            // 5xx tells Cloud Tasks to retry per the queue's retry configuration (backoff, max attempts).
            _logger.LogError(ex, "Cloud Tasks job {JobId} ({JobType}) failed; returning 500 for retry.", jobId, envelope.JobType);
            return StatusCode(500);
        }
    }
}
