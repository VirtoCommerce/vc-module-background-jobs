using System.Diagnostics;
using System.Net.Http.Json;
using NBomber.Contracts;
using NBomber.CSharp;

// Load-test harness for the background-job engine comparison. Drives the sample module's benchmark endpoints on a
// running Producer; canonical CPU/memory/throughput come from Application Insights, this drives the load and reports
// client-side rate/latency. Usage:  dotnet run -- <profile>   where <profile> is one of the keys below.
//
//   Env:  VC_BENCH_BASEURL   e.g. https://localhost:5001   (required)
//         VC_BENCH_TOKEN     bearer token for the [Authorize] endpoints (required unless the API is open)
//         VC_BENCH_ENGINE    label for the report name, e.g. "hangfire" | "rabbitmq" (optional)

var profile = args.Length > 0 ? args[0].ToLowerInvariant() : "smoke";
var baseUrl = (Environment.GetEnvironmentVariable("VC_BENCH_BASEURL") ?? "https://localhost:5001").TrimEnd('/');
var token = Environment.GetEnvironmentVariable("VC_BENCH_TOKEN");
var engine = Environment.GetEnvironmentVariable("VC_BENCH_ENGINE") ?? "engine";

using var http = Bench.CreateClient(baseUrl, token);

Console.WriteLine($"Profile '{profile}' against {baseUrl} (engine label: {engine})");

switch (profile)
{
    // Rate-based NBomber profiles — steady enqueue rate for a duration; the system drains asynchronously.
    case "sustained":
        Bench.RunInjected($"{engine}_sustained", http, baseUrl, "Io", delayMs: 50, cpu: 0, rate: 200, duration: TimeSpan.FromMinutes(2));
        break;
    case "io":
        Bench.RunInjected($"{engine}_io", http, baseUrl, "Io", delayMs: 200, cpu: 0, rate: 300, duration: TimeSpan.FromMinutes(2));
        break;
    case "cpu":
        Bench.RunInjected($"{engine}_cpu", http, baseUrl, "Cpu", delayMs: 0, cpu: 2_000_000, rate: 100, duration: TimeSpan.FromMinutes(2));
        break;

    // Burst + drain profiles — enqueue a fixed batch once, then poll the run to completion and report drain throughput.
    case "burst":
        await Bench.RunBurstDrain(http, baseUrl, count: 10_000, kind: "Io", delayMs: 50, cpu: 0, payloadBytes: 0);
        break;
    case "mapreduce":
        await Bench.RunMapReduceDrain(http, baseUrl, width: 2_000, kind: "Io", delayMs: 50, cpu: 0, resultBytes: 0);
        break;
    case "largepayload":
        await Bench.RunBurstDrain(http, baseUrl, count: 2_000, kind: "Io", delayMs: 10, cpu: 0, payloadBytes: 64 * 1024);
        break;

    // Tiny, dependency-light run for CI / connectivity check (in-memory Hangfire is fine).
    case "smoke":
    default:
        await Bench.RunBurstDrain(http, baseUrl, count: 50, kind: "Io", delayMs: 10, cpu: 0, payloadBytes: 0, timeout: TimeSpan.FromSeconds(60));
        break;
}

internal static class Bench
{
    public static HttpClient CreateClient(string baseUrl, string? token)
    {
        // Dev-friendly: accept the platform's self-signed cert on localhost.
        var handler = new HttpClientHandler
        {
            ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator,
        };
        var client = new HttpClient(handler) { BaseAddress = new Uri(baseUrl + "/"), Timeout = TimeSpan.FromMinutes(5) };
        if (!string.IsNullOrEmpty(token))
        {
            client.DefaultRequestHeaders.Authorization = new("Bearer", token);
        }

        return client;
    }

    // Steady enqueue rate: each iteration posts one benchmark job; NBomber controls the injection rate and reports
    // client-side rate + latency. Combine with Application Insights for server CPU/memory/throughput.
    public static void RunInjected(string name, HttpClient http, string baseUrl, string kind, int delayMs, long cpu, int rate, TimeSpan duration)
    {
        var url = $"api/background-jobs-sample/benchmark/fire?count=1&kind={kind}&delayMs={delayMs}&cpu={cpu}";

        var scenario = Scenario.Create(name, async context =>
        {
            using var response = await http.PostAsync(url, content: null, context.ScenarioCancellationToken);
            return response.IsSuccessStatusCode ? Response.Ok() : Response.Fail(statusCode: ((int)response.StatusCode).ToString());
        })
        .WithoutWarmUp()
        .WithLoadSimulations(Simulation.Inject(rate, interval: TimeSpan.FromSeconds(1), during: duration));

        // NBomber writes HTML + CSV + MD reports to ./reports by default.
        NBomberRunner
            .RegisterScenarios(scenario)
            .WithReportFileName(name)
            .Run();
    }

    public static async Task RunBurstDrain(HttpClient http, string baseUrl, int count, string kind, int delayMs, long cpu, int payloadBytes, TimeSpan? timeout = null)
    {
        var url = $"api/background-jobs-sample/benchmark/fire?count={count}&kind={kind}&delayMs={delayMs}&cpu={cpu}&payloadBytes={payloadBytes}";
        Console.WriteLine($"Enqueuing {count} jobs ({kind})...");
        var start = await http.PostAsync(url, content: null);
        start.EnsureSuccessStatusCode();
        var run = await start.Content.ReadFromJsonAsync<RunResponse>()
                  ?? throw new InvalidOperationException("No run response.");
        Console.WriteLine($"runId={run.RunId} enqueueMs={run.EnqueueMs:F0}");
        await PollToCompletion(http, run.RunId, count, timeout ?? TimeSpan.FromMinutes(10));
    }

    public static async Task RunMapReduceDrain(HttpClient http, string baseUrl, int width, string kind, int delayMs, long cpu, int resultBytes, TimeSpan? timeout = null)
    {
        var url = $"api/background-jobs-sample/benchmark/mapreduce?width={width}&kind={kind}&delayMs={delayMs}&cpu={cpu}&resultBytes={resultBytes}";
        Console.WriteLine($"Enqueuing map/reduce batch of {width} ({kind})...");
        var start = await http.PostAsync(url, content: null);
        start.EnsureSuccessStatusCode();
        var run = await start.Content.ReadFromJsonAsync<RunResponse>()
                  ?? throw new InvalidOperationException("No run response.");
        Console.WriteLine($"runId={run.RunId} batchId={run.BatchId} enqueueMs={run.EnqueueMs:F0}");
        await PollToCompletion(http, run.RunId, width, timeout ?? TimeSpan.FromMinutes(10));
    }

    // Samples the completion count each second and reports the PEAK per-second drain rate — the honest,
    // concurrency-limited throughput, independent of how long enqueue took (enqueue and drain overlap).
    private static async Task PollToCompletion(HttpClient http, string runId, int total, TimeSpan timeout)
    {
        var sw = Stopwatch.StartNew();
        var lastDone = 0;
        var lastT = 0.0;
        var peakRate = 0.0;
        while (sw.Elapsed < timeout)
        {
            var status = await http.GetFromJsonAsync<StatusResponse>($"api/background-jobs-sample/benchmark/{runId}");
            if (status is { Tracked: true })
            {
                var done = status.Completed + status.Failed;
                var t = sw.Elapsed.TotalSeconds;
                var dt = t - lastT;
                if (dt >= 0.5)
                {
                    var rate = (done - lastDone) / dt;
                    if (rate > peakRate) { peakRate = rate; }
                    lastDone = done;
                    lastT = t;
                }

                Console.Write($"\r  {done}/{total} ({status.Failed} failed) · {t:F0}s · peak {peakRate:F0}/s   ");
                if (status.Done)
                {
                    break;
                }
            }

            await Task.Delay(1000);
        }

        sw.Stop();
        var avg = total / Math.Max(1.0, sw.Elapsed.TotalSeconds);
        Console.WriteLine($"\nDrained {total} in {sw.Elapsed.TotalSeconds:F1}s → avg {avg:F0}/s, PEAK {peakRate:F0}/s (registry-sampled, enqueue-overlap-free).");
    }

    private sealed record RunResponse(string RunId, string? BatchId, int Count, double EnqueueMs);

    private sealed record StatusResponse(string RunId, bool Tracked, int Total, int Completed, int Failed, double ElapsedMs, bool Done);
}
