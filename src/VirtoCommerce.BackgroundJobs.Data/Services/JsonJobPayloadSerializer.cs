#nullable enable
using System;
using Newtonsoft.Json;
using VirtoCommerce.BackgroundJobs.Core.Services;

namespace VirtoCommerce.BackgroundJobs.Data.Services;

/// <summary>
/// Newtonsoft.Json implementation of <see cref="IJobPayloadSerializer"/>, using the module's shared
/// <see cref="JobJsonSettings.Default"/> (reference-loop safe). These are the module's own settings, not the
/// platform's MVC/GraphQL JSON configuration — see <see cref="JobJsonSettings"/> for the polymorphic-payload caveat.
/// </summary>
public sealed class JsonJobPayloadSerializer : IJobPayloadSerializer
{
    public (string PayloadType, string PayloadJson) Serialize<TPayload>(TPayload payload) where TPayload : class
    {
        // Fail with a clear message rather than an "Object reference not set" NRE from GetType() below when a caller
        // (e.g. a map handler that legitimately returned null) passes null.
        ArgumentNullException.ThrowIfNull(payload);

        var concreteType = payload.GetType();
        return (concreteType.AssemblyQualifiedName!, JsonConvert.SerializeObject(payload, JobJsonSettings.Default));
    }

    public object Deserialize(string payloadType, string payloadJson)
    {
        var type = Type.GetType(payloadType)
            ?? throw new InvalidOperationException($"Cannot resolve job payload type '{payloadType}'.");

        return JsonConvert.DeserializeObject(payloadJson, type, JobJsonSettings.Default)
            ?? throw new InvalidOperationException($"Failed to deserialize job payload of type '{payloadType}'.");
    }
}
