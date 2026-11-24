#nullable enable
using System;
using System.Linq;
using Hangfire;
using Microsoft.Extensions.DependencyInjection;
using VirtoCommerce.BackgroundJobs.Hangfire;
using VirtoCommerce.Platform.Core.PushNotifications;

namespace VirtoCommerce.Platform.Hangfire.Extensions;

/// <summary>
/// Adds this module's Hangfire filters that need a service from the container to <see cref="GlobalJobFilters.Filters"/>,
/// which is process-wide: a filter holds what it was built with, so registering again replaces the earlier instance
/// instead of leaving one bound to a provider that is gone (a second host in one process, a test fixture per class).
/// </summary>
public static class HangfireJobFilterRegistration
{
    public static void RegisterServiceFilters(IServiceProvider serviceProvider)
    {
        ArgumentNullException.ThrowIfNull(serviceProvider);

        var existing = GlobalJobFilters.Filters.Where(x => x.Instance is CancelBeforeRunStateFilter).Select(x => x.Instance).ToList();
        foreach (var filter in existing)
        {
            GlobalJobFilters.Filters.Remove(filter);
        }

        GlobalJobFilters.Filters.Add(new CancelBeforeRunStateFilter(serviceProvider.GetRequiredService<IPushNotificationManager>()));
    }
}
