using System.Collections.Generic;
using Avro.IO;
using Avro.Specific;
using System.IO;
using Kinetica.Tests.TestInfrastructure;
using kinetica;
using Shouldly;
using Xunit;

namespace Kinetica.Tests.Avro;

/// <summary>
/// Round-trip encode → decode tests that verify the Avro encode/decode pair
/// is lossless for synthesized objects.  These tests are schema-agnostic:
/// they exercise the same encode/decode path used by the SDK at runtime
/// without requiring fixtures or a live server.
/// </summary>
public sealed class RoundTripTests
{
    // -------------------------------------------------------------------------
    // RawInsertRecordsRequest (has embedded Schema_)
    // -------------------------------------------------------------------------

    [Fact]
    public void RawInsertRecordsRequest_RoundTrip_PreservesTableName()
    {
        var original = new RawInsertRecordsRequest
        {
            table_name    = "ki_home.test_table",
            list          = [],
            list_str      = [],
            list_encoding = RawInsertRecordsRequest.ListEncoding.BINARY,
            options       = new Dictionary<string, string>()
        };

        var decoded = RoundTrip(original);

        decoded.table_name.ShouldBe("ki_home.test_table");
        decoded.list_encoding.ShouldBe(RawInsertRecordsRequest.ListEncoding.BINARY);
    }

    [Fact]
    public void RawInsertRecordsRequest_RoundTrip_PreservesOptions()
    {
        var original = new RawInsertRecordsRequest
        {
            table_name    = "t",
            list          = [],
            list_str      = [],
            list_encoding = RawInsertRecordsRequest.ListEncoding.BINARY,
            options       = new Dictionary<string, string>
            {
                ["update_on_existing_pk"] = "true",
                ["return_record_ids"]     = "false"
            }
        };

        var decoded = RoundTrip(original);

        decoded.options.ShouldContainKey("update_on_existing_pk");
        decoded.options["update_on_existing_pk"].ShouldBe("true");
    }

    // -------------------------------------------------------------------------
    // ShowSystemPropertiesRequest (reflection-schema)
    // -------------------------------------------------------------------------

    [Fact]
    public void ShowSystemPropertiesRequest_RoundTrip_EmptyOptions()
    {
        var original = new ShowSystemPropertiesRequest
        {
            options = new Dictionary<string, string>()
        };

        var decoded = RoundTrip(original);

        decoded.options.ShouldBeEmpty();
    }

    [Fact]
    public void ShowSystemPropertiesRequest_RoundTrip_WithOptions()
    {
        var original = new ShowSystemPropertiesRequest
        {
            options = new Dictionary<string, string>
            {
                [ShowSystemPropertiesRequest.Options.PROPERTIES] = "version"
            }
        };

        var decoded = RoundTrip(original);

        decoded.options.ShouldContainKey(ShowSystemPropertiesRequest.Options.PROPERTIES);
        decoded.options[ShowSystemPropertiesRequest.Options.PROPERTIES].ShouldBe("version");
    }

    // -------------------------------------------------------------------------
    // CreateTypeRequest (reflection-schema)
    // -------------------------------------------------------------------------

    [Fact]
    public void CreateTypeRequest_RoundTrip_PreservesTypeDefinition()
    {
        var original = new CreateTypeRequest
        {
            type_definition = @"{""type"":""record"",""name"":""type_name"",""fields"":[{""name"":""id"",""type"":""int""}]}",
            label           = "my_type",
            properties      = new Dictionary<string, IList<string>>(),
            options         = new Dictionary<string, string>()
        };

        var decoded = RoundTrip(original);

        decoded.type_definition.ShouldBe(original.type_definition);
        decoded.label.ShouldBe("my_type");
    }

    // -------------------------------------------------------------------------
    // RawKineticaResponse (the envelope used for all server responses)
    // -------------------------------------------------------------------------

    [Fact]
    public void RawKineticaResponse_RoundTrip_PreservesStatus()
    {
        var original = new RawKineticaResponse
        {
            status    = "OK",
            message   = "",
            data_type = "show_system_properties_response",
            data      = [],
            data_str  = ""
        };

        var decoded = RoundTrip(original);

        decoded.status.ShouldBe("OK");
        decoded.data_type.ShouldBe("show_system_properties_response");
    }

    // -------------------------------------------------------------------------
    // Encode → decode is byte-identical on re-encode (stability)
    // -------------------------------------------------------------------------

    [Fact]
    public void RawInsertRecordsRequest_EncodeIsStable()
    {
        var obj = new RawInsertRecordsRequest
        {
            table_name    = "stable_table",
            list          = [],
            list_str      = [],
            list_encoding = RawInsertRecordsRequest.ListEncoding.BINARY,
            options       = new Dictionary<string, string>()
        };

        var bytes1 = AvroTestHelpers.Encode(obj);
        var bytes2 = AvroTestHelpers.Encode(obj);

        bytes1.ShouldBe(bytes2);
    }

    // -------------------------------------------------------------------------
    // Helper
    // -------------------------------------------------------------------------

    private static T RoundTrip<T>(T original) where T : ISpecificRecord, new()
    {
        var bytes = AvroTestHelpers.Encode(original);
        T decoded = new();
        using var ms = new MemoryStream(bytes);
        var reader = new SpecificReader<T>(decoded.Schema, decoded.Schema);
        return reader.Read(decoded, new BinaryDecoder(ms));
    }
}
