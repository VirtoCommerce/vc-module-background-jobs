using System.Text;
using System.Threading.Tasks;
using Google.Cloud.Tasks.V2;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using VirtoCommerce.BackgroundJobs.Core.Models;
using VirtoCommerce.BackgroundJobs.GoogleCloudTasks;
using Xunit;

namespace VirtoCommerce.BackgroundJobs.Tests;

public class GoogleCloudTasksTests
{
    private static JobEnvelope BuildEnvelope() => new()
    {
        JobType = "Some.Payload, Some.Assembly",
        PayloadType = "Some.Payload, Some.Assembly",
        PayloadJson = "{\"value\":\"hi\"}",
        Queue = "default",
        UserName = "tester",
    };

    private static GoogleCloudTasksOptions BuildOptions() => new()
    {
        ProjectId = "my-project",
        LocationId = "us-central1",
        QueueId = "vc-jobs",
        CallbackBaseUrl = "https://admin.example.com/",
        OidcServiceAccountEmail = "tasks@my-project.iam.gserviceaccount.com",
        OidcAudience = "https://admin.example.com",
    };

    [Fact]
    public void Build_Maps_Envelope_To_HttpTargetTask()
    {
        // Arrange
        var envelope = BuildEnvelope();
        var options = BuildOptions();

        // Act
        var request = GoogleCloudTasksRequestBuilder.Build(envelope, options);

        // Assert — queue parent + HTTP target
        Assert.Equal("projects/my-project/locations/us-central1/queues/vc-jobs", request.Parent);

        var http = request.Task.HttpRequest;
        Assert.Equal(HttpMethod.Post, http.HttpMethod);
        Assert.Equal("https://admin.example.com/api/background-jobs/google-cloud-tasks/callback", http.Url);
        Assert.Equal("application/json", http.Headers["Content-Type"]);

        // OIDC token is minted for the configured service account + audience.
        Assert.Equal("tasks@my-project.iam.gserviceaccount.com", http.OidcToken.ServiceAccountEmail);
        Assert.Equal("https://admin.example.com", http.OidcToken.Audience);

        // Body is the round-trippable envelope.
        var roundTripped = JsonConvert.DeserializeObject<JobEnvelope>(http.Body.ToStringUtf8());
        Assert.NotNull(roundTripped);
        Assert.Equal(envelope.JobType, roundTripped!.JobType);
        Assert.Equal(envelope.PayloadJson, roundTripped.PayloadJson);
    }

    [Fact]
    public void Build_Defaults_Audience_To_Callback_Url_When_Not_Set()
    {
        // Arrange
        var options = BuildOptions();
        options.OidcAudience = null;

        // Act
        var request = GoogleCloudTasksRequestBuilder.Build(BuildEnvelope(), options);

        // Assert — audience falls back to the full callback URL.
        Assert.Equal(
            "https://admin.example.com/api/background-jobs/google-cloud-tasks/callback",
            request.Task.HttpRequest.OidcToken.Audience);
    }

    [Fact]
    public void Build_Sets_Deterministic_TaskName_For_UniqueKey()
    {
        var options = BuildOptions();

        var r1 = GoogleCloudTasksRequestBuilder.Build(BuildEnvelope() with { UniqueKey = "order-42" }, options);
        var r2 = GoogleCloudTasksRequestBuilder.Build(BuildEnvelope() with { UniqueKey = "order-42" }, options);

        // Named task in the configured queue -> Cloud Tasks dedups; the same key always yields the same name.
        Assert.StartsWith("projects/my-project/locations/us-central1/queues/vc-jobs/tasks/", r1.Task.Name);
        Assert.Equal(r1.Task.Name, r2.Task.Name);
    }

    [Fact]
    public void Build_Leaves_TaskName_Empty_Without_UniqueKey()
    {
        var request = GoogleCloudTasksRequestBuilder.Build(BuildEnvelope(), BuildOptions());

        // No unique key -> Cloud Tasks auto-generates the task id.
        Assert.Equal(string.Empty, request.Task.Name);
    }

    [Fact]
    public async System.Threading.Tasks.Task TokenValidator_Fails_Closed_When_ServiceAccount_Not_Configured()
    {
        var options = BuildOptions();
        options.OidcServiceAccountEmail = string.Empty;
        var validator = new GoogleCloudTasksTokenValidator(
            Options.Create(options), NullLogger<GoogleCloudTasksTokenValidator>.Instance);

        // Even with a bearer value present, validation must refuse when no service account is configured.
        var ok = await validator.ValidateAsync("Bearer anything", TestContext.Current.CancellationToken);

        Assert.False(ok);
    }
}
