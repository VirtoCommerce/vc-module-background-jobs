namespace VirtoCommerce.BackgroundJobs;

/// <summary>Serializes job payloads to/from the envelope's <c>PayloadType</c> + <c>PayloadJson</c>.</summary>
public interface IJobPayloadSerializer
{
    /// <summary>Serialize a payload, returning its concrete assembly-qualified type name and JSON.</summary>
    (string PayloadType, string PayloadJson) Serialize<TPayload>(TPayload payload) where TPayload : class;

    /// <summary>Deserialize a payload from its assembly-qualified type name and JSON.</summary>
    object Deserialize(string payloadType, string payloadJson);
}
