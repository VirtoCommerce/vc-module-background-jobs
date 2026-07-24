#nullable enable
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using VirtoCommerce.BackgroundJobs.Core.Models;
using VirtoCommerce.BackgroundJobs;
using VirtoCommerce.BackgroundJobs.RabbitMQ.Extensions;
using VirtoCommerce.Platform.Core.Jobs;
using Xunit;

namespace VirtoCommerce.BackgroundJobs.Tests.Conformance;

/// <summary>
/// Micro-benchmark for the RabbitMQ batch-enqueue optimization: publishes N envelopes to a throwaway queue
/// sequentially (looping <see cref="IJobEngine.Enqueue"/>) vs in one <see cref="IJobEngine.EnqueueBatch"/> call
/// (pipelined publisher confirmations), and reports both rates. Producer-only (no consumer) so it measures pure
/// enqueue cost. Gated on the same <c>VC_CONFORMANCE_RABBITMQ</c> broker uri as the conformance suite.
/// </summary>
public sealed class RabbitMqBatchEnqueueBenchmarkTests(ITestOutputHelper output)
{
    [Fact]
    public async Task BatchEnqueue_Pipelines_Confirms_And_Is_Not_Slower_Than_Sequential()
    {
        var connection = Environment.GetEnvironmentVariable(RabbitMqConformanceFixture.ConnectionEnvironmentVariable);
        if (string.IsNullOrWhiteSpace(connection))
        {
            Assert.Skip($"set {RabbitMqConformanceFixture.ConnectionEnvironmentVariable} to an AMQP uri to run.");
            return;
        }

        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?> { ["VirtoCommerce:RabbitMQ:Uri"] = connection })
            .Build();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRabbitMqJobEngine(config);
        await using var provider = services.BuildServiceProvider();
        var engine = provider.GetRequiredService<IJobEngine>();

        var cancellationToken = TestContext.Current.CancellationToken;
        const int n = 1000;
        var queue = "bench-batch-" + Guid.NewGuid().ToString("N");
        var options = new EnqueueOptions { Queue = queue };
        JobEnvelope Make() => new() { JobType = "bench", PayloadType = "bench", PayloadJson = "{}", Queue = queue };

        // Warm up the connection/channel so neither run pays the first-connect cost.
        await engine.Enqueue(Make(), options, cancellationToken);

        var sequential = Stopwatch.StartNew();
        for (var i = 0; i < n; i++)
        {
            await engine.Enqueue(Make(), options, cancellationToken);
        }

        sequential.Stop();

        var envelopes = Enumerable.Range(0, n).Select(_ => Make()).ToList();
        var batch = Stopwatch.StartNew();
        var ids = await engine.EnqueueBatch(envelopes, options, cancellationToken);
        batch.Stop();

        output.WriteLine($"sequential {n}: {sequential.ElapsedMilliseconds} ms ({n * 1000.0 / Math.Max(1, sequential.ElapsedMilliseconds):F0}/s)");
        output.WriteLine($"batch      {n}: {batch.ElapsedMilliseconds} ms ({n * 1000.0 / Math.Max(1, batch.ElapsedMilliseconds):F0}/s)");
        output.WriteLine($"speedup: {(double)sequential.ElapsedMilliseconds / Math.Max(1, batch.ElapsedMilliseconds):F1}x");

        Assert.Equal(n, ids.Count);
        Assert.Distinct(ids);
        Assert.True(batch.Elapsed <= sequential.Elapsed, "batch enqueue should not be slower than sequential");
    }
}
