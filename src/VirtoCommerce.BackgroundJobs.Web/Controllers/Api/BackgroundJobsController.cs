using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Permissions = VirtoCommerce.BackgroundJobs.Core.ModuleConstants.Security.Permissions;

namespace VirtoCommerce.BackgroundJobs.Web.Controllers.Api;

[Authorize]
[Route("api/background-jobs")]
public class BackgroundJobsController : Controller
{
    // GET: api/background-jobs
    /// <summary>
    /// Get message
    /// </summary>
    /// <remarks>Return "Hello world!" message</remarks>
    [HttpGet]
    [Route("")]
    [Authorize(Permissions.Read)]
    public ActionResult<string> Get()
    {
        return Ok(new { result = "Hello world!" });
    }
}
