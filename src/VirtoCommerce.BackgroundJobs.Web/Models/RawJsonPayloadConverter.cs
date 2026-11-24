#nullable enable
using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace VirtoCommerce.BackgroundJobs.Web.Models;

/// <summary>
/// Reads an enqueue request's <c>payload</c> member as a raw <see cref="JToken"/> that keeps date-like strings as
/// strings and numbers with a fraction as decimals, so the payload text handed to the job serializer carries what the
/// caller sent. MVC's own reader would turn a date-like string into a <c>DateTime</c> before the payload type is known.
/// Does not depend on the platform's global converters, which differ between platform versions.
/// </summary>
public sealed class RawJsonPayloadConverter : JsonConverter
{
    public override bool CanConvert(Type objectType) => typeof(JToken).IsAssignableFrom(objectType);

    public override object? ReadJson(JsonReader reader, Type objectType, object? existingValue, JsonSerializer serializer)
    {
        if (reader.TokenType == JsonToken.Null)
        {
            return null;
        }

        var dateParseHandling = reader.DateParseHandling;
        var floatParseHandling = reader.FloatParseHandling;
        reader.DateParseHandling = DateParseHandling.None;
        reader.FloatParseHandling = FloatParseHandling.Decimal;
        try
        {
            return JToken.Load(reader);
        }
        finally
        {
            reader.DateParseHandling = dateParseHandling;
            reader.FloatParseHandling = floatParseHandling;
        }
    }

    public override void WriteJson(JsonWriter writer, object? value, JsonSerializer serializer)
    {
        if (value is JToken token)
        {
            token.WriteTo(writer);
        }
        else
        {
            writer.WriteNull();
        }
    }
}
