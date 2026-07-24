using System;
using System.Security.Cryptography;
using System.Text;
using Google.Cloud.Tasks.V2;
using Google.Protobuf;
using Newtonsoft.Json;
using VirtoCommerce.BackgroundJobs.Core.Models;

namespace VirtoCommerce.BackgroundJobs.GoogleCloudTasks;

/// <summary>
/// Pure mapping from a <see cref="JobEnvelope"/> to the Cloud Tasks <see cref="CreateTaskRequest"/> the engine
/// submits: an HTTP-target task that POSTs the serialized envelope to the in-platform callback, authenticated with
/// an OIDC token. Kept free of the GCP client so it is unit-testable without credentials or a live queue.
/// </summary>
public static class GoogleCloudTasksRequestBuilder
{
    public static QueueName BuildQueueName(GoogleCloudTasksOptions options) =>
        QueueName.FromProjectLocationQueue(options.ProjectId, options.LocationId, options.QueueId);

    public static string BuildCallbackUrl(GoogleCloudTasksOptions options) =>
        options.CallbackBaseUrl.TrimEnd('/') + GoogleCloudTasksConstants.CallbackPath;

    public static CreateTaskRequest Build(JobEnvelope envelope, GoogleCloudTasksOptions options)
    {
        var callbackUrl = BuildCallbackUrl(options);
        var audience = string.IsNullOrEmpty(options.OidcAudience) ? callbackUrl : options.OidcAudience!;
        var body = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(envelope));

        var task = new Google.Cloud.Tasks.V2.Task
        {
            HttpRequest = new HttpRequest
            {
                HttpMethod = HttpMethod.Post,
                Url = callbackUrl,
                Headers = { ["Content-Type"] = "application/json" },
                Body = ByteString.CopyFrom(body),
                OidcToken = new OidcToken
                {
                    ServiceAccountEmail = options.OidcServiceAccountEmail,
                    Audience = audience,
                },
            },
        };

        // Honor EnqueueOptions.UniqueKey via Cloud Tasks' built-in de-duplication: naming a task makes the broker
        // reject a second create with the same name for ~1h after the first runs (CreateTask -> AlreadyExists), so
        // re-enqueuing the same key collapses to one task. The key is hashed because task ids allow only
        // [A-Za-z0-9_-]. (Note: enabling name-based dedup adds latency on Google's side per their docs.)
        if (!string.IsNullOrEmpty(envelope.UniqueKey))
        {
            var taskId = DeriveTaskId(envelope.UniqueKey!);
            task.Name = TaskName.FromProjectLocationQueueTask(options.ProjectId, options.LocationId, options.QueueId, taskId).ToString();
        }

        return new CreateTaskRequest
        {
            Parent = BuildQueueName(options).ToString(),
            Task = task,
        };
    }

    private static string DeriveTaskId(string uniqueKey)
    {
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(uniqueKey));
        return Convert.ToHexString(hash).ToLowerInvariant();
    }
}
