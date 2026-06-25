using System;
using System.Threading;
using System.Threading.Tasks;
using VirtoCommerce.Platform.Core.DistributedLock;
using VirtoCommerce.Platform.Core.Security;

namespace VirtoCommerce.BackgroundJobs.Conformance;

/// <summary>Shared constants for the conformance suite.</summary>
public static class ConformanceConstants
{
    /// <summary>The queue the queue-routing scenario enqueues onto; a conformant worker must drain it.</summary>
    public const string CustomQueue = "conformance-queue";

    /// <summary>Progress message the recording handler reports; the progress scenario asserts it surfaced.</summary>
    public const string ProgressMessage = "conformance-progress";

    /// <summary>Default time a scenario waits for the engine to process a job before failing.</summary>
    public static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(30);
}

/// <summary>
/// <see cref="IUserNameResolver"/> backed by an <see cref="AsyncLocal{T}"/> so the enqueuing user and the
/// worker-side user (set by the dispatcher per job from the envelope) stay isolated per execution flow — concurrent
/// jobs do not clobber each other's user context.
/// </summary>
public sealed class ConformanceUserNameResolver : IUserNameResolver
{
    private readonly AsyncLocal<string?> _current = new();

    public string GetCurrentUserName() => _current.Value ?? "unknown";

    public void SetCurrentUserName(string userName) => _current.Value = userName;
}

/// <summary>No-op <see cref="IDistributedLockService"/> for single-process conformance runs (mirrors the platform's
/// NoLockService) so the in-process recurring scheduler can be exercised without external lock infrastructure.</summary>
public sealed class ConformanceNoLockService : IDistributedLockService
{
    public T Execute<T>(string resourceKey, Func<T> resolver, TimeSpan? lockTimeout = null, TimeSpan? tryLockTimeout = null, TimeSpan? retryInterval = null, CancellationToken? cancellationToken = null)
        => resolver();

    public Task<T> ExecuteAsync<T>(string resourceKey, Func<Task<T>> resolver, TimeSpan? lockTimeout = null, TimeSpan? tryLockTimeout = null, TimeSpan? retryInterval = null, CancellationToken? cancellationToken = null)
        => resolver();
}
