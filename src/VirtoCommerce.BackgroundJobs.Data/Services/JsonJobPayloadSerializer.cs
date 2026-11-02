#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Hosting;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using VirtoCommerce.BackgroundJobs;

namespace VirtoCommerce.BackgroundJobs.Data.Services;

/// <summary>
/// Newtonsoft.Json implementation of <see cref="IJobPayloadSerializer"/>, using the module's
/// <see cref="JobJsonSettings"/> (reference-loop safe). Given the host's converters, payloads are written and read with
/// them, in the API's wire format they are written against — see <see cref="JobJsonSettings"/> for why payloads need them.
/// </summary>
/// <param name="hostConverters">
/// The host's MVC Newtonsoft converters. Modules add theirs while the platform starts, after this is built.
/// </param>
/// <param name="lifetime">Tells when startup has finished, so the converter list is complete.</param>
public sealed class JsonJobPayloadSerializer(IList<JsonConverter>? hostConverters = null, IHostApplicationLifetime? lifetime = null) : IJobPayloadSerializer
{
    // The host converters read the API's JSON: camelCase (ModuleIdentityJsonConverter reads "major") and enums by name
    // (the order module's IOperation converter turns an unknown PaymentStatus into Custom). Dictionary keys are kept as
    // they are. Payloads written before (PascalCase, enums as numbers) still read: names match case-insensitively and
    // StringEnumConverter accepts numbers.
    private static readonly IContractResolver WireFormat = new DefaultContractResolver { NamingStrategy = new CamelCaseNamingStrategy() };

    private JsonSerializerSettings? _settings;

    private JsonSerializerSettings Settings
    {
        get
        {
            if (hostConverters is null)
            {
                return JobJsonSettings.Default;
            }

            if (_settings is { } built)
            {
                return built;
            }

            // Until startup ends, modules may still be adding converters: build from what is there on each call
            // (without waiting; a payload that needs a missing one fails like any job error) and keep it once started.
            // A copy (not an enumeration) of a list modules may still be adding to.
            var settings = JobJsonSettings.WithConverters(new List<JsonConverter>(hostConverters).Where(x => x != null));
            settings.ContractResolver = WireFormat;
            if (lifetime is null || lifetime.ApplicationStarted.IsCancellationRequested)
            {
                _settings = settings;
            }

            return settings;
        }
    }

    public (string PayloadType, string PayloadJson) Serialize<TPayload>(TPayload payload) where TPayload : class
    {
        // Fail with a clear message rather than an "Object reference not set" NRE from GetType() below when a caller
        // (e.g. a map handler that legitimately returned null) passes null.
        ArgumentNullException.ThrowIfNull(payload);

        var concreteType = payload.GetType();
        return (concreteType.AssemblyQualifiedName!, JsonConvert.SerializeObject(payload, Settings));
    }

    public object Deserialize(string payloadType, string payloadJson)
    {
        var type = Type.GetType(payloadType)
            ?? throw new InvalidOperationException($"Cannot resolve job payload type '{payloadType}'.");

        return JsonConvert.DeserializeObject(payloadJson, type, Settings)
            ?? throw new InvalidOperationException($"Failed to deserialize job payload of type '{payloadType}'.");
    }
}
