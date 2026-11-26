#nullable enable
using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Moq;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using VirtoCommerce.BackgroundJobs.Core;
using VirtoCommerce.BackgroundJobs.Core.Admin;
using VirtoCommerce.BackgroundJobs.Core.Models;
using VirtoCommerce.BackgroundJobs.Data.Admin;
using VirtoCommerce.BackgroundJobs.Data.Services;
using VirtoCommerce.BackgroundJobs.Web.Controllers.Api;
using VirtoCommerce.Platform.Core.Common;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.JsonConverters;
using VirtoCommerce.Platform.Core.PushNotifications;
using VirtoCommerce.Platform.Core.Security;
using VirtoCommerce.Platform.Core.Settings;
using Xunit;

namespace VirtoCommerce.BackgroundJobs.Tests;

/// <summary><c>POST api/background-jobs/enqueue</c> through the real MVC pipeline, facade and dispatcher, with and without a stand-in for the platform's global <c>JsonElement</c> converter.</summary>
public class EnqueueEndpointTests
{
    public class ProbeItem
    {
        public ProbeItem()
        {
            Type = GetType().Name;
        }

        public string Type { get; set; }
    }

    public class ProbeSubItem : ProbeItem
    {
        public decimal Price { get; set; }

        public string? When { get; set; }

        public string? Extra { get; set; }
    }

    public class ProbePayload
    {
        public decimal Amount { get; set; }

        public string? Stamp { get; set; }

        public ProbeItem? Item { get; set; }
    }

    public sealed class EnqueueRecorder
    {
        public int Enqueued;

        public List<ProbePayload> Received { get; } = [];
    }

    public sealed class ProbeHandler(EnqueueRecorder recorder) : IBackgroundJobHandler<ProbePayload>
    {
        public Task Execute(ProbePayload payload, IJobExecutionContext context, CancellationToken cancellationToken = default)
        {
            recorder.Received.Add(payload);
            return Task.CompletedTask;
        }
    }

    // Runs the job inline through the real dispatcher, so the handler has run when Enqueue returns.
    private sealed class DispatchingTestEngine(IServiceProvider serviceProvider, IPushNotificationManager pushNotificationManager, EnqueueRecorder recorder) : IJobEngine
    {
        public string ProviderName => "Test";

        public async Task<string> Enqueue(JobEnvelope envelope, EnqueueOptions options, CancellationToken cancellationToken = default)
        {
            recorder.Enqueued++;
            var jobId = Guid.NewGuid().ToString("N");
            var context = JobExecutionContextFactory.Create(pushNotificationManager, envelope, jobId);
            await serviceProvider.GetRequiredService<IJobDispatcher>().Dispatch(envelope, context, cancellationToken);

            return jobId;
        }

        public Task<Job?> GetStatus(string jobId, CancellationToken cancellationToken = default) => Task.FromResult<Job?>(null);

        public Task<bool> Delete(string jobId, CancellationToken cancellationToken = default) => Task.FromResult(false);
    }

    // Stands in for the platform's global JsonElement converter.
    private sealed class StandInJsonElementConverter : JsonConverter<JsonElement>
    {
        public override void WriteJson(JsonWriter writer, JsonElement value, Newtonsoft.Json.JsonSerializer serializer)
        {
            writer.WriteRawValue(value.GetRawText());
        }

        public override JsonElement ReadJson(JsonReader reader, Type objectType, JsonElement existingValue, bool hasExistingValue, Newtonsoft.Json.JsonSerializer serializer)
        {
            var dateParseHandling = reader.DateParseHandling;
            reader.DateParseHandling = DateParseHandling.None;
            Newtonsoft.Json.Linq.JToken token;
            try
            {
                token = Newtonsoft.Json.Linq.JToken.Load(reader);
            }
            finally
            {
                reader.DateParseHandling = dateParseHandling;
            }

            using var document = JsonDocument.Parse(token.ToString(Formatting.None));

            return document.RootElement.Clone();
        }
    }

    static EnqueueEndpointTests()
    {
        AbstractTypeFactory<ProbeItem>.RegisterType<ProbeSubItem>();
        PolymorphJsonConverter.RegisterTypeForDiscriminator(typeof(ProbeItem), nameof(ProbeItem.Type));
    }

    private static async Task<(WebApplication App, HttpClient Client, EnqueueRecorder Recorder)> StartAsync(bool withJsonElementConverter)
    {
        var recorder = new EnqueueRecorder();
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();

        var services = builder.Services;
        services.AddSingleton(recorder);
        services.AddControllers()
            .AddApplicationPart(typeof(BackgroundJobsAdminController).Assembly)
            .AddNewtonsoftJson(options =>
            {
                // As the platform's Startup configures MVC.
                options.SerializerSettings.ContractResolver = new PolymorphJsonContractResolver();
                options.SerializerSettings.Converters.Add(new StringEnumConverter());
                if (withJsonElementConverter)
                {
                    options.SerializerSettings.Converters.Add(new StandInJsonElementConverter());
                }

                options.SerializerSettings.PreserveReferencesHandling = PreserveReferencesHandling.None;
                options.SerializerSettings.ReferenceLoopHandling = ReferenceLoopHandling.Ignore;
                options.SerializerSettings.DateTimeZoneHandling = DateTimeZoneHandling.Utc;
                options.SerializerSettings.NullValueHandling = NullValueHandling.Ignore;
                options.SerializerSettings.Formatting = Formatting.None;
                options.SerializerSettings.Converters.Add(new PolymorphJsonConverter());
            });

        services.AddAuthorization(options =>
        {
            foreach (var permission in ModuleConstants.Security.Permissions.AllPermissions)
            {
                options.AddPolicy(permission, policy => policy.RequireAssertion(_ => true));
            }
        });

        services.AddBackgroundJob<ProbeHandler, ProbePayload>();
        services.AddSingleton(Mock.Of<ISettingsManager>());
        services.AddScoped<IBackgroundJobsAdminQuery, BackgroundJobsAdminQuery>();
        services.Configure<BackgroundJobsOptions>(_ => { });
        services.AddSingleton(Mock.Of<IPushNotificationManager>());
        services.AddSingleton(Mock.Of<IUserNameResolver>());
        services.AddSingleton<IJobPayloadSerializer>(sp => new JsonJobPayloadSerializer(
            sp.GetService<IOptions<MvcNewtonsoftJsonOptions>>()?.Value.SerializerSettings.Converters,
            sp.GetService<IHostApplicationLifetime>()));
        services.AddSingleton<IJobDispatcher, DefaultJobDispatcher>();
        services.AddSingleton<IJobEngine, DispatchingTestEngine>();
        services.AddScoped<IBackgroundJob, JobEngineBackgroundJob>();

        var app = builder.Build();
        app.UseRouting();
        app.UseAuthorization();
        app.MapControllers();
        await app.StartAsync();

        return (app, app.GetTestClient(), recorder);
    }

    private static Task<HttpResponseMessage> PostEnqueueAsync(HttpClient client, string json)
    {
        return client.PostAsync("api/background-jobs/enqueue", new StringContent(json, Encoding.UTF8, "application/json"), TestContext.Current.CancellationToken);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Enqueue_ObjectPayload_ReachesHandlerUnchanged(bool withJsonElementConverter)
    {
        var (app, client, recorder) = await StartAsync(withJsonElementConverter);
        await using var _ = app;
        using var __ = client;

        var body = """
            {
              "name": "ProbeHandler",
              "payload": {
                "amount": 79228162514264337593543950335.0,
                "stamp": "2026-11-20T10:00:00.000Z",
                "item": { "type": "ProbeSubItem", "price": 1234567890.123456789, "when": "2026-11-20T10:00:00.000Z", "extra": "x" }
              }
            }
            """;

        var response = await PostEnqueueAsync(client, body);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var payload = Assert.Single(recorder.Received);
        Assert.Equal(79228162514264337593543950335m, payload.Amount);
        Assert.Equal("2026-11-20T10:00:00.000Z", payload.Stamp);
        var item = Assert.IsType<ProbeSubItem>(payload.Item);
        Assert.Equal(1234567890.123456789m, item.Price);
        Assert.Equal("2026-11-20T10:00:00.000Z", item.When);
        Assert.Equal("x", item.Extra);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Enqueue_WithProgressNotificationId_IsRefused(bool withJsonElementConverter)
    {
        var (app, client, recorder) = await StartAsync(withJsonElementConverter);
        await using var _ = app;
        using var __ = client;

        var response = await PostEnqueueAsync(client, """{ "name": "ProbeHandler", "options": { "progressNotificationId": "x" } }""");

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Contains("progressNotificationId", await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken));
        Assert.Equal(0, recorder.Enqueued);
    }

    // The binder hands a JSON null over as no payload, so the controller needs no separate case.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Enqueue_ExplicitNullPayload_RunsHandlerWithDefaultPayload(bool withJsonElementConverter)
    {
        var (app, client, recorder) = await StartAsync(withJsonElementConverter);
        await using var _ = app;
        using var __ = client;

        var response = await PostEnqueueAsync(client, """{ "name": "ProbeHandler", "payload": null }""");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.NotNull(Assert.Single(recorder.Received));
    }
}
