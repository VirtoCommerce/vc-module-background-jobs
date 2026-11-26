#nullable enable
using System;
using System.Linq;
using Hangfire;
using Microsoft.Extensions.DependencyInjection;
using VirtoCommerce.BackgroundJobs.Hangfire;
using VirtoCommerce.Platform.Core.PushNotifications;

namespace VirtoCommerce.Platform.Hangfire.Extensions;

/// <summary>
/// Adds this module's container-dependent Hangfire filters to the process-wide <see cref="GlobalJobFilters.Filters"/>,
/// replacing any instance an earlier registration left there.
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
