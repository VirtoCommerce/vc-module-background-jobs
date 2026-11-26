#nullable enable
using System;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Console;
using Hangfire.Console.Progress;
using Hangfire.Server;
using Microsoft.Extensions.Logging;
using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs.Hangfire;

/// <summary>
/// Mirrors a job's progress into the Hangfire dashboard console and forwards every call to the wrapped progress.
/// </summary>
public sealed class ConsoleJobProgress(IJobProgress inner, PerformContext? performContext, ILogger logger) : IJobProgress
{
    private readonly object _lock = new();
    private string? _lastMessage;
    private long _processedCount;
    private long _totalCount;
    private int _lastPercent = -1;
    private IProgressBar? _progressBar;

    public Task Report(JobProgressInfo progress, CancellationToken cancellationToken = default)
    {
        lock (_lock)
        {
            // Separate guards: a failing line must not skip the progress bar.
            TryMirror(() => MirrorMessage(progress.Message), "message");
            TryMirror(() => MirrorPercent(progress), "progress bar");
        }

        return inner.Report(progress, cancellationToken);
    }

    public Task Complete(JobOutcome outcome, string? error = null, CancellationToken cancellationToken = default)
    {
        try
        {
            performContext.WriteLine(string.IsNullOrEmpty(error) ? outcome.ToString() : $"{outcome}: {error}");
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Could not write the job outcome to the Hangfire console; the job continues.");
        }

        return inner.Complete(outcome, error, cancellationToken);
    }

    private void TryMirror(Action mirror, string what)
    {
        try
        {
            mirror();
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Could not write the job {What} to the Hangfire console; the job continues.", what);
        }
    }

    private void MirrorMessage(string? message)
    {
        if (string.IsNullOrEmpty(message) || message == _lastMessage)
        {
            return;
        }

        performContext.WriteLine(message);
        _lastMessage = message;
    }

    private void MirrorPercent(JobProgressInfo progress)
    {
        _processedCount = progress.ProcessedCount ?? _processedCount;
        _totalCount = progress.TotalCount ?? _totalCount;

        if (_totalCount <= 0)
        {
            return;
        }

        var percent = (int)Math.Clamp(Math.Floor(_processedCount * 100d / _totalCount), 0, 100);

        if (percent == _lastPercent)
        {
            return;
        }

        if (_progressBar is null)
        {
            _progressBar = performContext.WriteProgressBar(percent);
        }
        else
        {
            _progressBar.SetValue(percent);
        }

        _lastPercent = percent;
    }
}
