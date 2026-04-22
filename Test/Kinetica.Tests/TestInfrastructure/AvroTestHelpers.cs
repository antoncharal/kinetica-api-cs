using System;
using System.Collections.Generic;
using System.IO;
using Avro.IO;
using Avro.Specific;
using kinetica;

namespace Kinetica.Tests.TestInfrastructure
{
    /// <summary>
    /// Avro encode / decode helpers for tests that need to produce or inspect
    /// Avro-encoded bytes without a live Kinetica server.
    /// </summary>
    internal static class AvroTestHelpers
    {
        /// <summary>
        /// Encode a <see cref="KineticaData"/> / <see cref="ISpecificRecord"/>
        /// instance to raw Avro binary bytes using the schema embedded in the object.
        /// </summary>
        public static byte[] Encode(ISpecificRecord record)
        {
            using var ms = new MemoryStream();
            var writer = new SpecificDefaultWriter(record.Schema);
            writer.Write(record.Schema, record, new BinaryEncoder(ms));
            return ms.ToArray();
        }

        /// <summary>
        /// Decode raw Avro bytes into an instance of <typeparamref name="T"/>.
        /// </summary>
        public static T Decode<T>(byte[] bytes) where T : ISpecificRecord, new()
        {
            T obj = new();
            using var ms = new MemoryStream(bytes);
            var reader = new SpecificReader<T>(obj.Schema, obj.Schema);
            return reader.Read(obj, new BinaryDecoder(ms));
        }

        /// <summary>
        /// Build a minimal Avro-encoded <see cref="RawKineticaResponse"/> wrapping
        /// an already-encoded payload.  Suitable for use as fake transport response bytes.
        /// </summary>
        public static byte[] BuildOkResponse(string dataType, byte[] payload)
        {
            var raw = new RawKineticaResponse
            {
                status   = "OK",
                message  = "",
                data_type = dataType,
                data     = payload,
                data_str = ""
            };
            return Encode(raw);
        }

        /// <summary>
        /// Build a minimal Avro-encoded <see cref="RawKineticaResponse"/> wrapping
        /// an empty payload.  Suitable for endpoints whose callers ignore the response body.
        /// </summary>
        public static byte[] BuildOkResponse(string dataType)
            => BuildOkResponse(dataType, Array.Empty<byte>());

        /// <summary>
        /// Build a valid Avro-encoded <see cref="ShowSystemPropertiesResponse"/> wrapped
        /// in a <see cref="RawKineticaResponse"/> envelope.
        /// </summary>
        public static byte[] BuildShowSystemPropertiesResponse(
            IDictionary<string, string>? propertyMap = null)
        {
            var inner = new ShowSystemPropertiesResponse
            {
                property_map = propertyMap ?? new Dictionary<string, string>()
            };
            return BuildOkResponse("show_system_properties_response", Encode(inner));
        }

        /// <summary>
        /// Build an Avro-encoded <see cref="RawKineticaResponse"/> with an ERROR status
        /// and the given message — the format used by the Kinetica error envelope.
        /// </summary>
        public static byte[] BuildErrorResponse(string message)
        {
            var raw = new RawKineticaResponse
            {
                status   = "ERROR",
                message  = message,
                data_type = "",
                data     = Array.Empty<byte>(),
                data_str = ""
            };
            return Encode(raw);
        }
    }
}
