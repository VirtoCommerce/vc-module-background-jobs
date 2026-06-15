using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using VirtoCommerce.BackgroundJobs.Core.Services;
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

        public JobsController(IJobEngine jobEngine)
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
            var result = await _jobEngine.GetStatus(id, cancellationToken);
            return Ok(result);
        }
    }
}
