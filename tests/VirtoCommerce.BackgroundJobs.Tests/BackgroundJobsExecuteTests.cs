#nullable enable
using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Moq;
using VirtoCommerce.BackgroundJobs.Core.Admin;
using VirtoCommerce.BackgroundJobs;
using VirtoCommerce.BackgroundJobs.Data.Admin;
using VirtoCommerce.BackgroundJobs.Data.Services;
using VirtoCommerce.BackgroundJobs.Web.Controllers.Api;
using VirtoCommerce.BackgroundJobs.Web.Models;
using VirtoCommerce.Platform.Core.Jobs;
using VirtoCommerce.Platform.Core.Settings;
using Xunit;

namespace VirtoCommerce.BackgroundJobs.Tests;

public class BackgroundJobsExecuteTests
{
    public sealed class AdminSamplePayload { public string? Value { get; set; } }

    private sealed class AdminSampleHandler : IBackgroundJobHandler<AdminSamplePayload>
    {
        public Task Execute(AdminSamplePayload payload, IJobExecutionContext context, CancellationToken cancellationToken = default)
            => Task.CompletedTask;
    }

    // Records the (handlerType, payload) the controller enqueues, so we can assert name→type resolution + binding.
    private sealed class CapturingBackgroundJob : IBackgroundJob
    {
        public Type? LastHandlerType { get; private set; }
        public object? LastPayload { get; private set; }

        public Task<string> Enqueue<THandler>(object payload, EnqueueOptions? options = null, CancellationToken ct = default)
            where THandler : class
            => Enqueue(typeof(THandler), payload, options, ct);

        public Task<string> Enqueue(Type handlerType, object payload, EnqueueOptions? options = null, CancellationToken ct = default)
        {
            LastHandlerType = handlerType;
            LastPayload = payload;
            return Task.FromResult("job-1");
        }
    }

    private static BackgroundJobsAdminController CreateController(CapturingBackgroundJob capturing, params BackgroundJobDescriptor[] descriptors)
        => new(new BackgroundJobsAdminQuery(descriptors, [], Mock.Of<ISettingsManager>()), capturing, new JsonJobPayloadSerializer());

    [Fact]
    public async Task Enqueue_KnownName_ResolvesHandler_BindsPayload_AndEnqueues()
    {
        var descriptor = new BackgroundJobDescriptor
        {
            Name = "AdminSampleHandler",
            HandlerType = typeof(AdminSampleHandler).AssemblyQualifiedName!,
            PayloadType = typeof(AdminSamplePayload).AssemblyQualifiedName!,
        };
        var capturing = new CapturingBackgroundJob();
        var controller = CreateController(capturing, descriptor);

        var request = new EnqueueJobRequest
        {
            Name = "AdminSampleHandler",
            Payload = JsonDocument.Parse("""{"value":"hello"}""").RootElement,
        };

        var result = await controller.Enqueue(request, TestContext.Current.CancellationToken);

        var ok = Assert.IsType<OkObjectResult>(result.Result);
        Assert.Equal("job-1", ok.Value);
        Assert.Equal(typeof(AdminSampleHandler), capturing.LastHandlerType);
        var payload = Assert.IsType<AdminSamplePayload>(capturing.LastPayload);
        Assert.Equal("hello", payload.Value);
    }

    [Fact]
    public async Task Enqueue_InternalHandler_IsRejected_AndDoesNotEnqueue()
    {
        var descriptor = new BackgroundJobDescriptor
        {
            Name = "InternalCoordinator",
            HandlerType = typeof(AdminSampleHandler).AssemblyQualifiedName!,
            PayloadType = typeof(AdminSamplePayload).AssemblyQualifiedName!,
            Triggerable = false,   // internal plumbing — listed but not runnable by name
        };
        var capturing = new CapturingBackgroundJob();
        var controller = CreateController(capturing, descriptor);

        var request = new EnqueueJobRequest { Name = "InternalCoordinator" };

        var result = await controller.Enqueue(request, TestContext.Current.CancellationToken);

        Assert.IsType<BadRequestObjectResult>(result.Result);
        Assert.Null(capturing.LastHandlerType);
    }

    [Fact]
    public async Task Enqueue_UnknownName_ReturnsNotFound_AndDoesNotEnqueue()
    {
        var capturing = new CapturingBackgroundJob();
        var controller = CreateController(capturing);   // empty registry

        var request = new EnqueueJobRequest { Name = "does-not-exist" };

        var result = await controller.Enqueue(request, TestContext.Current.CancellationToken);

        Assert.IsType<NotFoundObjectResult>(result.Result);
        Assert.Null(capturing.LastHandlerType);
    }
}
