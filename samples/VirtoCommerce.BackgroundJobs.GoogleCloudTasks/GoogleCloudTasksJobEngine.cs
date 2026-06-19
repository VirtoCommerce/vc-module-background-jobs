using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Cloud.Tasks.V2;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using VirtoCommerce.BackgroundJobs.Core.Models;
using VirtoCommerce.BackgroundJobs.Core.Services;
using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs.GoogleCloudTasks;

/// <summary>
/// Google Cloud Tasks implementation of <see cref="IJobEngine"/> — a <b>push</b> engine. Enqueue creates an
/// HTTP-target Cloud Task that POSTs the serialized <see cref="JobEnvelope"/> (with an OIDC token) to the in-platform
/// callback; Cloud Tasks delivers and retries it per the queue's configuration. There is no in-process consumer:
/// <c>GoogleCloudTasksCallbackController</c> receives the push and runs the shared <see cref="IJobDispatcher"/>.
/// <para>
/// Unlike RabbitMQ, Cloud Tasks keeps a queryable task ledger, so <see cref="GetStatus"/> and <see cref="Delete"/>
/// are real (a task that no longer exists has been delivered/acked). It does NOT implement
/// <see cref="IExpressionJobEngine"/> — delegates can't be marshaled to an HTTP push.
/// </para>
/// </summary>
public sealed class GoogleCloudTasksJobEngine : IJobEngine
{
    public const string ProviderNameValue = GoogleCloudTasksConstants.ProviderName;

    private readonly GoogleCloudTasksOptions _options;
    private readonly ILogger<GoogleCloudTasksJobEngine> _logger;

    // Built lazily so platform startup (which resolves IJobEngine to validate the provider) never blocks on
    // credential resolution / network — the client is created on first enqueue or status call. Credentials come from
    // Application Default Credentials (Workload Identity / the attached service account on GCP, or the
    // GOOGLE_APPLICATION_CREDENTIALS key-file path locally) — see GoogleCloudTasksOptions.
    private readonly Lazy<CloudTasksClient> _client;

    public GoogleCloudTasksJobEngine(IOptions<GoogleCloudTasksOptions> options, ILogger<GoogleCloudTasksJobEngine> logger)
    {
        _options = options.Value;
        _logger = logger;
        _client = new Lazy<CloudTasksClient>(() => new CloudTasksClientBuilder().Build());
    }

    public string ProviderName => ProviderNameValue;

    public async Task<string> Enqueue(JobEnvelope envelope, EnqueueOptions options, CancellationToken cancellationToken = default)
    {
        // Only EnqueueOptions.UniqueKey is honored (mapped to Cloud Tasks name-based de-duplication in the builder).
        // The target queue is fixed by GoogleCloudTasksOptions, so options.Queue / envelope.Queue are NOT used for
        // routing — this engine is intentionally single-queue. EnqueueOptions carries no delay, so no ScheduleTime.
        var request = GoogleCloudTasksRequestBuilder.Build(envelope, _options);

        try
        {
            var created = await _client.Value.CreateTaskAsync(request, cancellationToken);

            // The Cloud Tasks resource name is stable and uniquely identifies the task — use it as the job id.
            _logger.LogInformation("Created Cloud Task {TaskName} for job {JobType}", created.Name, envelope.JobType);

            return created.Name;
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.AlreadyExists)
        {
            // Name-based dedup: a task with this UniqueKey already exists (within the dedup window). The job is
            // already enqueued, so treat this as success and return the deterministic task name.
            _logger.LogInformation("Cloud Task {TaskName} already exists (unique key dedup); skipping duplicate enqueue.", request.Task.Name);

            return request.Task.Name;
        }
    }

    /// <summary>
    /// Cloud Tasks deletes a task once it is successfully delivered, so an existing task is still scheduled/in-flight
    /// and a NotFound means it already ran (or never existed). Progress is observed over SignalR like other engines.
    /// </summary>
    public async Task<Job?> GetStatus(string jobId, CancellationToken cancellationToken = default)
    {
        try
        {
            var task = await _client.Value.GetTaskAsync(TaskName.Parse(jobId), cancellationToken);
            return new Job { Id = task.Name, State = "Scheduled", Completed = false };
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.NotFound)
        {
            return new Job { Id = jobId, State = "Completed", Completed = true };
        }
    }

    public async Task<bool> Delete(string jobId, CancellationToken cancellationToken = default)
    {
        try
        {
            await _client.Value.DeleteTaskAsync(TaskName.Parse(jobId), cancellationToken);
            return true;
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.NotFound)
        {
            return false;
        }
    }
}
