#nullable enable
using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using VirtoCommerce.BackgroundJobs.Core;
using VirtoCommerce.BackgroundJobs.Core.Admin;
using VirtoCommerce.BackgroundJobs;
using VirtoCommerce.BackgroundJobs.Web.Models;
using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs.Web.Controllers.Api;

/// <summary>
/// Admin / integration API for background jobs (engine-agnostic): list the registered handlers and recurring
/// schedules for troubleshooting, and trigger a registered job by name. Read endpoints require the module's
/// <c>background-jobs:read</c> permission; triggering requires the separate <c>background-jobs:execute</c> permission.
/// </summary>
[Produces("application/json")]
[Route("api/background-jobs")]
public class BackgroundJobsAdminController(
    IBackgroundJobsAdminQuery query,
    IBackgroundJob backgroundJob,
    IJobPayloadSerializer serializer) : Controller
{
    /// <summary>Lists every registered background-job handler (name, handler type, payload type).</summary>
    [HttpGet("registered")]
    [Authorize(ModuleConstants.Security.Permissions.Read)]
    public ActionResult<IReadOnlyList<RegisteredJobInfo>> GetRegistered()
    {
        return Ok(query.GetRegisteredJobs());
    }

    /// <summary>Lists every declared recurring job with its effective schedule and last/next run.</summary>
    [HttpGet("recurring")]
    [Authorize(ModuleConstants.Security.Permissions.Read)]
    public async Task<ActionResult<IReadOnlyList<RecurringJobInfo>>> GetRecurring(CancellationToken cancellationToken)
    {
        return Ok(await query.GetRecurringJobsAsync(cancellationToken));
    }

    /// <summary>
    /// Triggers a registered background job by name (for integration middleware or manual runs). Only registered
    /// handlers are triggerable — addressed by their friendly name, never a raw type — and the request payload JSON is
    /// bound to the handler's payload type. Enqueues on the active engine and returns the engine job id (poll via
    /// <c>GET api/platform/jobs/{id}</c>).
    /// </summary>
    [HttpPost("enqueue")]
    [Authorize(ModuleConstants.Security.Permissions.Execute)]
    [ProducesResponseType(typeof(string), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<ActionResult<string>> Enqueue([FromBody] EnqueueJobRequest request, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(request?.Name))
        {
            return BadRequest("A registered job 'name' is required.");
        }

        var descriptor = query.FindRegisteredJob(request.Name);
        if (descriptor is null)
        {
            return NotFound($"No registered background job named '{request.Name}'.");
        }

        // Internal plumbing / system handlers are listed for troubleshooting but must not be runnable on demand with a
        // caller-crafted payload (e.g. map/reduce coordinators, module management).
        if (!descriptor.Triggerable)
        {
            return BadRequest($"Background job '{descriptor.Name}' is internal and cannot be triggered on demand.");
        }

        // Bind the request payload JSON to the handler's payload type. Missing/empty payload ⇒ default instance.
        var payloadJson = request.Payload.ValueKind is JsonValueKind.Undefined or JsonValueKind.Null
            ? "{}"
            : request.Payload.GetRawText();

        object payload;
        try
        {
            payload = serializer.Deserialize(descriptor.PayloadType, payloadJson);
        }
        catch (Exception ex)
        {
            return BadRequest($"Payload does not match job '{descriptor.Name}': {ex.Message}");
        }

        var handlerType = Type.GetType(descriptor.HandlerType);
        if (handlerType is null)
        {
            return Problem($"Handler type '{descriptor.HandlerType}' could not be loaded.");
        }

        try
        {
            var jobId = await backgroundJob.Enqueue(handlerType, payload, request.Options, cancellationToken);
            return Ok(jobId);
        }
        catch (BackgroundJobEngineNotInstalledException ex)
        {
            // No active engine is installed for the configured provider — a deployment/config problem, not a bad request.
            return StatusCode(StatusCodes.Status503ServiceUnavailable, ex.Message);
        }
        catch (ArgumentException ex)
        {
            // Handler/payload mismatch (should be rare — the payload was already bound to the descriptor's type).
            return BadRequest(ex.Message);
        }
    }
}
