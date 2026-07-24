using Newtonsoft.Json;

namespace VirtoCommerce.BackgroundJobs;

/// <summary>
/// Shared Newtonsoft settings for background-job serialization — used for both the job <b>payload</b>
/// (the <c>JsonJobPayloadSerializer</c> in the Data layer) and the transport <b>envelope</b> (the RabbitMQ
/// engine/consumer), so the two never diverge. Tuned for durable server-to-server messages: reference loops are ignored rather than throwing,
/// and nulls are preserved so a round-tripped value is structurally identical.
/// <para>
/// NOTE: these are the module's own settings, <b>not</b> the platform's MVC/GraphQL JSON configuration. A payload
/// that depends on platform-registered polymorphic converters should use a concrete type (or an
/// <c>AbstractTypeFactory</c>-registered type — the envelope carries the concrete payload type for the top level).
/// </para>
/// </summary>
public static class JobJsonSettings
{
    /// <summary>The shared settings instance. Reused (not re-created per call) to avoid per-message allocation.</summary>
    public static readonly JsonSerializerSettings Default = new()
    {
        ReferenceLoopHandling = ReferenceLoopHandling.Ignore,
        NullValueHandling = NullValueHandling.Include,
    };
}
