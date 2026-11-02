using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using VirtoCommerce.Platform.Core.JsonConverters;

namespace VirtoCommerce.BackgroundJobs;

/// <summary>
/// Shared Newtonsoft settings for background-job serialization — used for both the job <b>payload</b>
/// (the <c>JsonJobPayloadSerializer</c> in the Data layer) and the transport <b>envelope</b> (the RabbitMQ
/// engine/consumer), so the two never diverge. Tuned for durable server-to-server messages: reference loops are ignored rather than throwing,
/// and nulls are preserved so a round-tripped value is structurally identical.
/// <para>
/// Payloads nest the same types the API accepts (an export's abstract <c>ExportDataQuery</c>, an order's
/// <c>IOperation</c> children, dynamic property values), so the payload serializer writes and reads them with the converters the
/// platform and modules register on the host's MVC settings (<see cref="WithConverters"/>), as Hangfire did. Without a
/// host, <see cref="Default"/> carries the platform's <see cref="PolymorphJsonConverter"/> alone.
/// </para>
/// </summary>
public static class JobJsonSettings
{
    /// <summary>The shared settings instance. Reused (not re-created per call) to avoid per-message allocation.</summary>
    public static readonly JsonSerializerSettings Default = WithConverters([new PolymorphJsonConverter()]);

    /// <summary>The job settings with the given converters, the polymorphic ones guarded against null tokens.</summary>
    public static JsonSerializerSettings WithConverters(IEnumerable<JsonConverter> converters)
    {
        return new JsonSerializerSettings
        {
            ReferenceLoopHandling = ReferenceLoopHandling.Ignore,
            NullValueHandling = NullValueHandling.Include,
            Converters = converters.Select(x => x is PolymorphJsonConverter ? new NullSafeJsonConverter(x) : x).ToList(),
        };
    }

    // Nulls are included, so an unset member reaches its converter as a null token. PolymorphJsonConverter up to platform
    // 3.1079 fails on it, and so do the module converters that override ReadJson (orders' IOperation, payment methods): they
    // call JObject.Load before the base class, so a null check in the platform's own ReadJson can't cover them. Keep this
    // wrapper whatever the platform version. Other converters keep their own handling of null.
    private sealed class NullSafeJsonConverter(JsonConverter inner) : JsonConverter
    {
        public override bool CanRead => inner.CanRead;

        public override bool CanWrite => inner.CanWrite;

        public override bool CanConvert(Type objectType) => inner.CanConvert(objectType);

        public override object? ReadJson(JsonReader reader, Type objectType, object? existingValue, JsonSerializer serializer)
        {
            return reader.TokenType == JsonToken.Null ? null : inner.ReadJson(reader, objectType, existingValue, serializer);
        }

        public override void WriteJson(JsonWriter writer, object? value, JsonSerializer serializer)
        {
            inner.WriteJson(writer, value, serializer);
        }
    }
}
