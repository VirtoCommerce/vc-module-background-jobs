#nullable enable
using System;
using System.Collections.Generic;
using System.Reflection;
using Hangfire;
using Hangfire.States;
using VirtoCommerce.BackgroundJobs.Hangfire;
using Xunit;

namespace VirtoCommerce.BackgroundJobs.Tests;

/// <summary>
/// Guards the two assumptions <see cref="PerJobRetryFilterAttribute"/> makes about Hangfire internals. Both are
/// invisible at compile time and both fail <b>silently</b> — the filter simply stops overriding and per-job
/// <c>MaxRetryAttempts</c> becomes a no-op on the Hangfire engine — so they are asserted against the shipped assembly
/// rather than trusted. A Hangfire upgrade that breaks either one turns this test red instead of the feature.
/// </summary>
public class PerJobRetryFilterTests
{
    [Fact]
    public void RetryCountParameter_Matches_The_Job_Parameter_Hangfire_Writes()
    {
        var onStateElection = typeof(AutomaticRetryAttribute).GetMethod(nameof(AutomaticRetryAttribute.OnStateElection));
        Assert.NotNull(onStateElection);

        var literals = GetStringLiterals(onStateElection!);

        Assert.Contains(PerJobRetryFilterAttribute.RetryCountParameter, literals);
    }

    [Fact]
    public void Filter_Runs_After_AutomaticRetry()
    {
        // Lower order runs first: AutomaticRetryAttribute must have elected the reschedule before this filter can
        // decide whether to undo it.
        Assert.True(new PerJobRetryFilterAttribute().Order > new AutomaticRetryAttribute().Order);
    }

    [Fact]
    public void Filter_Handles_State_Election()
    {
        // The whole mechanism hangs off this interface; losing it would silently disable the filter.
        Assert.IsAssignableFrom<IElectStateFilter>(new PerJobRetryFilterAttribute());
    }

    /// <summary>Reads the <c>ldstr</c> operands of a method body — the string literals it references.</summary>
    private static IReadOnlyCollection<string> GetStringLiterals(MethodInfo method)
    {
        const byte ldstr = 0x72;

        var il = method.GetMethodBody()?.GetILAsByteArray() ?? [];
        var module = method.Module;
        var result = new HashSet<string>(StringComparer.Ordinal);

        for (var i = 0; i < il.Length - 4; i++)
        {
            if (il[i] != ldstr)
            {
                continue;
            }

            try
            {
                result.Add(module.ResolveString(BitConverter.ToInt32(il, i + 1)));
            }
            catch (ArgumentException)
            {
                // The byte happened to be operand data rather than an opcode — not a string token, skip it.
            }
        }

        return result;
    }
}
