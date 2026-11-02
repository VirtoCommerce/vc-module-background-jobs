#nullable enable
using System.Threading;
using Hangfire;
using Hangfire.Common;
using Newtonsoft.Json;
using VirtoCommerce.BackgroundJobs.Core.Models;
using VirtoCommerce.BackgroundJobs.Hangfire;
using Xunit;

namespace VirtoCommerce.BackgroundJobs.Tests;

// The Hangfire dashboard lists every job as HangfireJobExecutor.Execute / IRecurringJobInvoker.Run unless the methods
// carry a display name; these assert the names it renders.
public class HandlerJobDisplayNameTests
{
    private static string Display(JobEnvelope? envelope)
    {
        var json = envelope is null ? "not json" : JsonConvert.SerializeObject(envelope);
        var job = Job.FromExpression<HangfireJobExecutor>(x => x.Execute(json, null, CancellationToken.None));
        return new HandlerJobDisplayNameAttribute().Format(null!, job);
    }

    private static JobEnvelope Envelope(string? handlerType, string payloadType) => new()
    {
        JobType = payloadType,
        HandlerType = handlerType,
        PayloadType = payloadType,
        PayloadJson = "{}",
    };

    [Fact]
    public void ShowsHandlerTypeName()
        => Assert.Equal("LogEntityChangesJobHandler", Display(Envelope(
            "VirtoCommerce.CatalogModule.Data.Jobs.LogEntityChangesJobHandler, VirtoCommerce.CatalogModule.Data, Version=3.0.0.0, Culture=neutral, PublicKeyToken=null",
            "VirtoCommerce.CatalogModule.Data.Jobs.LogEntityChangesJobPayload, VirtoCommerce.CatalogModule.Data")));

    [Fact]
    public void PayloadTypedEnqueue_ShowsPayloadTypeName()
        => Assert.Equal("ExportPayload", Display(Envelope(null, "My.Ns.Outer+ExportPayload, My.Asm")));

    [Fact]
    public void UnreadableEnvelope_FallsBackToMethodName()
        => Assert.Equal("HangfireJobExecutor.Execute", Display(null));

    [Fact]
    public void RecurringInvoker_ShowsRecurringJobId()
    {
        var job = Job.FromExpression<IRecurringJobInvoker>(x => x.Run("catalog-cleanup"));
        var attribute = Assert.Single(job.Method.GetCustomAttributes(typeof(JobDisplayNameAttribute), true));
        Assert.Equal("Recurring: catalog-cleanup", ((JobDisplayNameAttribute)attribute).Format(null!, job));
    }
}
