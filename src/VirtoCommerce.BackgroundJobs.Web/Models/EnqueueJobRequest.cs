#nullable enable
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs.Web.Models;

/// <summary>Request body for triggering a registered background job by name over REST.</summary>
public sealed class EnqueueJobRequest
{
    /// <summary>Registered job name (see <c>GET api/background-jobs/registered</c>).</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>The job payload as JSON; bound to the handler's payload type. Omit for a parameterless payload.</summary>
    [JsonConverter(typeof(RawJsonPayloadConverter))]
    public JToken? Payload { get; set; }

    /// <summary>Optional enqueue options (queue, title, progress, retries, unique key).</summary>
    public EnqueueOptions? Options { get; set; }
}
