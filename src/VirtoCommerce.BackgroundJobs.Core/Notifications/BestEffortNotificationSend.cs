using System;
using System.Threading;
using System.Threading.Tasks;

namespace VirtoCommerce.BackgroundJobs.Core.Notifications;

/// <summary>Runs a send to the job-progress notification channel without letting it throw: a failure to display never changes what the job does.</summary>
public static class BestEffortNotificationSend
{
    /// <summary>Awaits <paramref name="send"/> and passes any exception to <paramref name="onFailure"/> instead of throwing; an <see cref="OperationCanceledException"/> while <paramref name="cancellationToken"/> is cancelled goes to <paramref name="onCanceled"/> when one is given.</summary>
    public static async Task TrySendAsync(
        Func<Task> send,
        Action<Exception> onFailure,
        Action<Exception>? onCanceled = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await send();
        }
        catch (OperationCanceledException ex) when (cancellationToken.IsCancellationRequested && onCanceled is not null)
        {
            onCanceled(ex);
        }
        catch (Exception ex)
        {
            onFailure(ex);
        }
    }
}
