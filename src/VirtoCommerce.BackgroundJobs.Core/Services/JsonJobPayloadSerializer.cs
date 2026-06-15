using System;
using Newtonsoft.Json;

namespace VirtoCommerce.BackgroundJobs.Core.Services;

/// <summary>Newtonsoft.Json implementation of <see cref="IJobPayloadSerializer"/> (matches platform serialization).</summary>
public sealed class JsonJobPayloadSerializer : IJobPayloadSerializer
{
    public (string PayloadType, string PayloadJson) Serialize<TPayload>(TPayload payload) where TPayload : class
    {
        var concreteType = payload.GetType();
        return (concreteType.AssemblyQualifiedName!, JsonConvert.SerializeObject(payload));
    }

    public object Deserialize(string payloadType, string payloadJson)
    {
        var type = Type.GetType(payloadType)
            ?? throw new InvalidOperationException($"Cannot resolve job payload type '{payloadType}'.");

        return JsonConvert.DeserializeObject(payloadJson, type)
            ?? throw new InvalidOperationException($"Failed to deserialize job payload of type '{payloadType}'.");
    }
}
