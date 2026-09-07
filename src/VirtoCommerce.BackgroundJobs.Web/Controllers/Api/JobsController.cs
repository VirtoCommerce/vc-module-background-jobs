using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using VirtoCommerce.BackgroundJobs;
using VirtoCommerce.Platform.Core;
using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs.Web.Controllers.Api
{
    /// <summary>
    /// Engine-agnostic background-job monitoring API. Reads status from the active <see cref="IJobEngine"/>
    /// (Hangfire, RabbitMQ, …). The route is preserved so existing admin UI clients keep working.
    /// </summary>
    [Produces("application/json")]
    [Route("api/platform/jobs")]
    [Authorize(PlatformConstants.Security.Permissions.BackgroundJobsManage)]
    public class JobsController : Controller
    {
        private readonly IJobEngine _jobEngine;

        // IJobEngine is optional (like the IBackgroundJob facade): with no engine module installed it is not
        // registered. Requiring it here would make DI fail to construct the controller and 500 the monitoring
        // endpoint; accepting null lets the API stay up and degrade gracefully (see GetStatus).
        public JobsController(IJobEngine jobEngine = null)
        {
            _jobEngine = jobEngine;
        }

        /// <summary>
        /// Get background job status.
        /// </summary>
        /// <param name="id">Job ID.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        [HttpGet]
        [Route("{id}")]
        public async Task<ActionResult<Job>> GetStatus(string id, CancellationToken cancellationToken)
        {
            // IJobEngine.GetStatus returns null for an unknown/expired job (the port contract). The monitoring API,
            // however, never returns null: clients (the admin UI poller, integration tests) expect a Job, treating an
            // unknown id as completed so they stop polling — preserving the platform's original endpoint behavior.
            // With no engine installed (_jobEngine == null) we return the same "completed" shape for the same reason.
            var result = (_jobEngine is null ? null : await _jobEngine.GetStatus(id, cancellationToken))
                ?? new Job { Id = id, Completed = true };

            return Ok(result);
        }

        /// <summary>
        /// Request cancellation of a background job. Best-effort and engine-dependent (see
        /// <see cref="IBackgroundJob.Cancel"/>): a not-started job is prevented from running, a running job is asked to
        /// stop via its cancellation token. Returns 501 when the active engine (or absence of one) cannot cancel.
        /// </summary>
        /// <param name="id">Job ID.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        [HttpPost]
        [Route("{id}/cancel")]
        public async Task<ActionResult> Cancel(string id, CancellationToken cancellationToken)
        {
            if (_jobEngine is null || !_jobEngine.SupportsCancellation)
            {
                return StatusCode(StatusCodes.Status501NotImplemented, "Cancellation is not supported by the active background-job engine.");
            }

            var canceled = await _jobEngine.Delete(id, cancellationToken);
            return canceled ? Ok() : NotFound();
        }
    }
}
