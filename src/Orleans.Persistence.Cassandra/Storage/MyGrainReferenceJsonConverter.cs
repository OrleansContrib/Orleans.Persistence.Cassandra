using System;
using System.Net;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Orleans.Runtime;
using Orleans.GrainReferences;
using Orleans.Serialization.TypeSystem;
using Microsoft.Extensions.Options;
using System.Globalization;
using System.Diagnostics;

namespace Orleans.Serialization
{

    /// <summary>
    /// <see cref="Newtonsoft.Json.JsonConverter" /> implementation for <see cref="GrainReference"/>.
    /// </summary>
    /// <seealso cref="Newtonsoft.Json.JsonConverter" />
    public class MyGrainReferenceJsonConverter : JsonConverter
    {
        private static readonly Type AddressableType = typeof(IAddressable);
        private readonly GrainReferenceActivator referenceActivator;

        /// <summary>
        /// Initializes a new instance of the <see cref="MyGrainReferenceJsonConverter"/> class.
        /// </summary>
        /// <param name="referenceActivator">The grain reference activator.</param>
        public MyGrainReferenceJsonConverter(GrainReferenceActivator referenceActivator)
        {
            this.referenceActivator = referenceActivator;
        }

        /// <inheritdoc/>
        public override bool CanConvert(Type objectType)
        {
            return AddressableType.IsAssignableFrom(objectType);
        }

        /// <inheritdoc/>
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            var val = (GrainReference)value;
            Debug.Assert(val != null);

            writer.WriteStartObject();
            writer.WritePropertyName("Id");
            writer.WriteStartObject();
            writer.WritePropertyName("Type");
            writer.WriteValue(val.GrainId.Type.ToString());
            writer.WritePropertyName("Key");
            writer.WriteValue(val.GrainId.Key.ToString());
            writer.WriteEndObject();
            writer.WritePropertyName("Interface");
            writer.WriteValue(val.InterfaceType.ToString());
            writer.WriteEndObject();
        }

        /// <inheritdoc/>
        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            JObject jo = JObject.Load(reader);
            var id = jo["Id"] ?? throw new Exception("'Id' property missing");

            var type = id["Type"]?.ToObject<string>() ?? throw new Exception("'Type' property missing");
            var key = id["Key"]?.ToObject<string>() ?? throw new Exception("'Key' property missing");
            var intf = jo["Interface"]?.ToString() ?? throw new Exception("'Interface' property missing");
            
            GrainId grainId = GrainId.Create(type, key);
            var iface = GrainInterfaceType.Create(intf);
            return this.referenceActivator.CreateReference(grainId, iface);
        }
    }
}
