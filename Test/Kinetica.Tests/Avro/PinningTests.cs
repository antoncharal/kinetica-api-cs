using System.IO;
using Avro.IO;
using Avro.Specific;
using Kinetica.Tests.TestInfrastructure;
using kinetica;
using Shouldly;
using Xunit;

namespace Kinetica.Tests.Avro;

/// <summary>
/// Pinning tests: decode a captured baseline fixture then re-encode it and
/// assert the output bytes are identical to the original capture.
///
/// These tests are the primary guard for PR-05 (Avro library swap): if any
/// encoding detail changes, byte equality will fail before the PR can merge.
/// </summary>
public sealed class PinningTests
{
    // -------------------------------------------------------------------------
    // Request pinning — types with embedded Schema_ field (deterministic)
    // -------------------------------------------------------------------------

    [Theory]
    [InlineData("req-insert-records-20260422093411")]
    [InlineData("req-insert-records-20260422093412")]
    public void RawInsertRecordsRequest_DecodeReEncode_MatchesFixture(string fixtureName)
    {
        AssertDecodeReEncodeIdentity<RawInsertRecordsRequest>(fixtureName);
    }

    [Theory]
    [InlineData("req-update-records-20260422095131")]
    [InlineData("req-update-records-20260422095137")]
    public void RawUpdateRecordsRequest_DecodeReEncode_MatchesFixture(string fixtureName)
    {
        AssertDecodeReEncodeIdentity<RawUpdateRecordsRequest>(fixtureName);
    }

    // -------------------------------------------------------------------------
    // Request pinning — reflection-schema types
    // -------------------------------------------------------------------------

    [Theory]
    [InlineData("req-create-type-20260422093411")]
    [InlineData("req-create-type-20260422093412")]
    public void CreateTypeRequest_DecodeReEncode_MatchesFixture(string fixtureName)
    {
        AssertDecodeReEncodeIdentity<CreateTypeRequest>(fixtureName);
    }

    [Theory]
    [InlineData("req-execute-sql-20260422094359")]
    [InlineData("req-execute-sql-20260422094400")]
    public void ExecuteSqlRequest_DecodeReEncode_MatchesFixture(string fixtureName)
    {
        AssertDecodeReEncodeIdentity<ExecuteSqlRequest>(fixtureName);
    }

    [Theory]
    [InlineData("req-show-system-properties-20260422093412")]
    public void ShowSystemPropertiesRequest_DecodeReEncode_MatchesFixture(string fixtureName)
    {
        AssertDecodeReEncodeIdentity<ShowSystemPropertiesRequest>(fixtureName);
    }

    // -------------------------------------------------------------------------
    // Helper
    // -------------------------------------------------------------------------
    // Helper
    // -------------------------------------------------------------------------

    private static void AssertDecodeReEncodeIdentity<T>(string fixtureName)
        where T : ISpecificRecord, new()
    {
        var original = FixtureLoader.LoadBytes($"baseline/{fixtureName}.bin");

        T seed = new();
        T decoded;
        using (var ms = new MemoryStream(original))
        {
            var reader = new SpecificReader<T>(seed.Schema, seed.Schema);
            decoded = reader.Read(seed, new BinaryDecoder(ms));
        }

        var reEncoded = AvroTestHelpers.Encode(decoded);

        reEncoded.ShouldBe(original,
            $"Re-encoding {fixtureName} produced different bytes — check for Avro schema or encoder changes.");
    }
}
