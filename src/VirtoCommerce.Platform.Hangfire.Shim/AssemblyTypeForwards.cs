using System.Runtime.CompilerServices;

// Type forwarders keep the original "VirtoCommerce.Platform.Hangfire" assembly identity and namespaces working
// for existing modules, while the actual implementations now live in VirtoCommerce.BackgroundJobs.Hangfire.dll.
// Every public type that the platform's old VirtoCommerce.Platform.Hangfire assembly exposed is forwarded here.

[assembly: TypeForwardedTo(typeof(VirtoCommerce.Platform.Hangfire.HangfireOptions))]
[assembly: TypeForwardedTo(typeof(VirtoCommerce.Platform.Hangfire.HangfireJobStorageType))]
[assembly: TypeForwardedTo(typeof(VirtoCommerce.Platform.Hangfire.HangfireAuthorizationHandler))]
[assembly: TypeForwardedTo(typeof(VirtoCommerce.Platform.Hangfire.IRecurringJobService))]
[assembly: TypeForwardedTo(typeof(VirtoCommerce.Platform.Hangfire.RecurringJobService))]
[assembly: TypeForwardedTo(typeof(VirtoCommerce.Platform.Hangfire.SettingCronJob))]
[assembly: TypeForwardedTo(typeof(VirtoCommerce.Platform.Hangfire.SettingCronJobBuilder))]
[assembly: TypeForwardedTo(typeof(VirtoCommerce.Platform.Hangfire.JobCancellationTokenWrapper))]
[assembly: TypeForwardedTo(typeof(VirtoCommerce.Platform.Hangfire.Extensions.ServiceCollectionExtensions))]
[assembly: TypeForwardedTo(typeof(VirtoCommerce.Platform.Hangfire.Extensions.ApplicationBuilderExtensions))]
[assembly: TypeForwardedTo(typeof(VirtoCommerce.Platform.Hangfire.Middleware.HangfireUserContextMiddleware))]
