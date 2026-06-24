using VirtoCommerce.Platform.Core.Common;

namespace VirtoCommerce.BackgroundJobs.SampleModule.Jobs;

/// <summary>
/// Payload for the sample recurring job. It carries parameters supplied at registration via the
/// <c>AddRecurringJob(() =&gt; new SampleRecurringJobPayload { ... }, ...)</c> factory overload — the factory runs
/// once per scheduled occurrence.
/// </summary>
public class SampleRecurringJobPayload : ValueObject
{
    /// <summary>A label passed from the schedule registration, echoed by the handler.</summary>
    public string? Label { get; set; }
}
