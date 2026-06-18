using VirtoCommerce.Platform.Core.Common;

namespace VirtoCommerce.BackgroundJobs.SampleModule.Jobs;

/// <summary>
/// Payload for the sample recurring job. Recurring payloads are parameterless by convention — a fresh instance is
/// created (via <see cref="AbstractTypeFactory{T}"/>) and enqueued on each scheduled occurrence.
/// </summary>
public class SampleRecurringJobPayload : ValueObject
{
}
