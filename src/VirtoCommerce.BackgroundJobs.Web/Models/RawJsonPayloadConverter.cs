#nullable enable
using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace VirtoCommerce.BackgroundJobs.Web.Models;

/// <summary>
/// Reads an enqueue request's <c>payload</c> as a raw <see cref="JToken"/>, keeping date-like strings as strings and
/// fractional numbers as decimals, independent of the platform's global converters.
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
