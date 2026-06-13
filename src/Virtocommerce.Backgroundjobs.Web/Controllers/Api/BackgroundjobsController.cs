using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Permissions = Virtocommerce.Backgroundjobs.Core.ModuleConstants.Security.Permissions;

namespace Virtocommerce.Backgroundjobs.Web.Controllers.Api;

[Authorize]
[Route("api/background-jobs")]
public class BackgroundjobsController : Controller
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
