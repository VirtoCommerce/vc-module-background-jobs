using System;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Hangfire;
using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs.Hangfire
{
    /// <summary>
    /// Hangfire implementation of the platform's engine-agnostic <see cref="IBackgroundJobProcessor"/> facade.
    /// Maps the in-process enqueue calls directly onto Hangfire's <see cref="BackgroundJob"/> client.
    /// </summary>
    public class HangfireBackgroundJobProcessor : IBackgroundJobProcessor
    {
        public string Enqueue(Expression<Action> methodCall) => BackgroundJob.Enqueue(methodCall);

        public string Enqueue(Expression<Func<Task>> methodCall) => BackgroundJob.Enqueue(methodCall);
    }
}
