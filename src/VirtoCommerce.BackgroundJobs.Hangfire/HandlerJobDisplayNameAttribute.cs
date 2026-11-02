#nullable enable
using System;
using Hangfire;
using Hangfire.Common;
using Hangfire.Dashboard;
using Newtonsoft.Json;
using VirtoCommerce.BackgroundJobs.Core.Models;

namespace VirtoCommerce.BackgroundJobs.Hangfire;

/// <summary>
/// Shows the real handler in the Hangfire dashboard instead of the generic <c>HangfireJobExecutor.Execute</c>: reads
/// the handler (or, for a payload-typed enqueue, the payload) type from the job's envelope argument. Resolved when the
/// dashboard renders, so jobs already in storage are shown the same way.
/// </summary>
[AttributeUsage(AttributeTargets.Method)]
public sealed class HandlerJobDisplayNameAttribute() : JobDisplayNameAttribute(nameof(HangfireJobExecutor))
{
    public override string Format(DashboardContext context, Job job)
    {
        try
        {
            var envelope = job.Args.Count > 0 && job.Args[0] is string json ? JsonConvert.DeserializeObject<JobEnvelope>(json) : null;
            var typeName = envelope?.HandlerType ?? envelope?.PayloadType;
            if (typeName is not null)
            {
                return ShortTypeName(typeName);
            }
        }
        catch (JsonException)
        {
            // Unreadable envelope: fall through to the default name.
        }

        return $"{job.Type.Name}.{job.Method.Name}";
    }

    // "Ns.Outer+Handler, Asm, Version=…" → "Handler"; a generic keeps only its definition's name.
    private static string ShortTypeName(string assemblyQualifiedName)
    {
        var fullName = assemblyQualifiedName.Split('[', ',')[0].Trim();
        return fullName[(fullName.LastIndexOfAny(['.', '+']) + 1)..];
    }
}
