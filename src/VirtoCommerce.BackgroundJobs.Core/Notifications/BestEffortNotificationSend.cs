using System;
using System.Threading;
using System.Threading.Tasks;

namespace VirtoCommerce.BackgroundJobs.Core.Notifications;

/// <summary>
/// Runs a send to the notification channel that only displays a job: whatever the send throws is handed to a callback
/// and swallowed, so a failure to display never changes what the job does or replaces the error the caller is about
/// to get.
/// </summary>
public static class BestEffortNotificationSend
{
    /// <summary>
    /// Awaits <paramref name="send"/> and never lets it throw. The caller logs inside the callbacks, so each record
    /// keeps a constant message template (CA2254) and its own structured fields.
    /// </summary>
    /// <param name="send">The send to the notification channel.</param>
    /// <param name="onFailure">Called with any exception the send throws, except the cancellation <paramref name="onCanceled"/> handles.</param>
    /// <param name="onCanceled">
    /// Called instead of <paramref name="onFailure"/> when the send throws <see cref="OperationCanceledException"/> while
    /// <paramref name="cancellationToken"/> is cancelled, i.e. a shutdown or a cancel rather than a fault. When it is
    /// null, a cancellation goes to <paramref name="onFailure"/> like any other exception.
    /// </param>
    /// <param name="cancellationToken">The token whose cancellation makes an <see cref="OperationCanceledException"/> expected.</param>
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
