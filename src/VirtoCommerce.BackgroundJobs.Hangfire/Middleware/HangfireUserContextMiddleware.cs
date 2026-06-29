using Hangfire.Client;
using Hangfire.Server;
using VirtoCommerce.Platform.Core.Common;
using VirtoCommerce.Platform.Core.Security;
using static VirtoCommerce.Platform.Core.Common.ThreadSlotNames;

namespace VirtoCommerce.Platform.Hangfire.Middleware
{
    /// <summary>
    /// This class allow to process all HangFire jobs to add user name from identity and save it to
    /// the Thread after job is performing to achieve getting access to user name in background tasks
    /// </summary>
    public class HangfireUserContextMiddleware : IClientFilter, IServerFilter
    {
        public const int UserNameLength = 64;

        private readonly IUserNameResolver _userNameResolver;

        public HangfireUserContextMiddleware(IUserNameResolver userNameResolver)
        {
            _userNameResolver = userNameResolver;
        }

        #region IClientFilter Members

        public void OnCreating(CreatingContext context)
        {
            var currentUserName = _userNameResolver.GetCurrentUserName();

            if (!string.IsNullOrEmpty(currentUserName))
            {
                context.SetJobParameter(USER_NAME, currentUserName);
            }
        }

        public void OnCreated(CreatedContext context)
        {
            // Pass
        }

        #endregion IClientFilter Members

        #region IServerFilter Members

        public void OnPerforming(PerformingContext context)
        {
            string userName;

            if (IsRecurringJob(context, out var recurringJobId))
            {
                userName = $"system:{recurringJobId}".Truncate(UserNameLength);
            }
            else
            {
                userName = context.GetJobParameter<string>(USER_NAME);
            }

            _userNameResolver.SetCurrentUserName(userName);
        }

        public void OnPerformed(PerformedContext context)
        {
            // Pass
        }

        #endregion IServerFilter Members

        private static bool IsRecurringJob(PerformingContext context, out string recurringJobId)
        {
            recurringJobId = context.GetJobParameter<string>("RecurringJobId");
            return !string.IsNullOrEmpty(recurringJobId);
        }
    }
}
