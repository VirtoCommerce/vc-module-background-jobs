using System;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace VirtoCommerce.BackgroundJobs.Core.Services;

/// <summary>
/// Optional engine capability: native expression-based enqueue (Hangfire only). The enqueue facade uses this
/// when the active engine implements it; otherwise expression enqueue throws <see cref="System.NotSupportedException"/>.
/// </summary>
public interface IExpressionJobEngine
{
    string Enqueue(Expression<Action> methodCall);

    string Enqueue(Expression<Func<Task>> methodCall);
}
