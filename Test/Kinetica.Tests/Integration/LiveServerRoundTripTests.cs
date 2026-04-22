using System;
using System.Collections.Generic;
using System.Linq;
using kinetica;
using Shouldly;
using Xunit;

namespace Kinetica.Tests.Integration;

/// <summary>
/// Live-server round-trip integration tests gated by the KINETICA_URL
/// environment variable.  These verify that the Apache.Avro 1.12.1 NuGet
/// package produces wire-compatible output accepted by a real Kinetica 7.2
/// server — the definitive PR-05 acceptance gate.
///
/// Set KINETICA_URL, KINETICA_USER, KINETICA_PASS before running:
///   $env:KINETICA_URL  = "http://localhost:9191"
///   $env:KINETICA_USER = "admin"
///   $env:KINETICA_PASS = "admin"
/// </summary>
[Collection("LiveServer")]
public sealed class LiveServerRoundTripTests : IDisposable
{
    private const string TablePrefix = "csharp_sdk_avro_rt_";
    private const int RecordCount = 1_000;

    private readonly kinetica.Kinetica? _kdb;
    private readonly bool _skip;
    private readonly List<string> _tablesToClean = [];

    public LiveServerRoundTripTests()
    {
        var url = Environment.GetEnvironmentVariable("KINETICA_URL");
        _skip = string.IsNullOrWhiteSpace(url);
        if (_skip) return;

        var options = new kinetica.Kinetica.Options
        {
            Username = Environment.GetEnvironmentVariable("KINETICA_USER") ?? "admin",
            Password = Environment.GetEnvironmentVariable("KINETICA_PASS") ?? "admin"
        };

        _kdb = new kinetica.Kinetica(url, options);
    }

    public void Dispose()
    {
        if (_kdb is null) return;

        foreach (var table in _tablesToClean)
        {
            try
            {
                _kdb.clearTable(table, "", new Dictionary<string, string>
                {
                    [ClearTableRequest.Options.NO_ERROR_IF_NOT_EXISTS] = ClearTableRequest.Options.TRUE
                });
            }
            catch
            {
                // best-effort cleanup
            }
        }
    }

    // -----------------------------------------------------------------
    // Test record — covers primitives, nullable, strings, float
    // -----------------------------------------------------------------
    private class RoundTripRecord
    {
        public int Id { get; set; }
        public long BigNum { get; set; }
        public float Score { get; set; }
        public double? NullableValue { get; set; }
        public string? Label { get; set; }
        public long Seq { get; set; }
    }

    // -----------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------

    private static List<RoundTripRecord> GenerateRecords(int count)
    {
        var records = new List<RoundTripRecord>(count);
        var rng = new Random(42); // deterministic seed

        for (int i = 0; i < count; i++)
        {
            records.Add(new RoundTripRecord
            {
                Id = i,
                BigNum = (long)i * 100_000L + rng.Next(),
                Score = (float)(rng.NextDouble() * 1000.0),
                NullableValue = i % 5 == 0 ? null : rng.NextDouble() * 999.0,
                Label = i % 7 == 0 ? null : $"label_{i:D6}_test_data_pad",
                Seq = 1_000_000L + i
            });
        }

        return records;
    }

    private string CreateTableAndType(string suffix, out KineticaType type)
    {
        string tableName = TablePrefix + suffix;
        _tablesToClean.Add(tableName);

        var columnProperties = new Dictionary<string, IList<string>>
        {
            { "Id", [ColumnProperty.PRIMARY_KEY] },
            { "NullableValue", [ColumnProperty.NULLABLE] },
            { "Label", [ColumnProperty.NULLABLE] }
        };

        type = KineticaType.fromClass(typeof(RoundTripRecord), columnProperties);
        string typeId = type.create(_kdb!);

        _kdb!.clearTable(tableName, "", new Dictionary<string, string>
        {
            [ClearTableRequest.Options.NO_ERROR_IF_NOT_EXISTS] = ClearTableRequest.Options.TRUE
        });

        _kdb.createTable(tableName, typeId);
        return tableName;
    }

    // -----------------------------------------------------------------
    // §7.6.1 — Insert 1,000 records via standard insertRecords, retrieve
    //          them back, assert field-level equality.
    // -----------------------------------------------------------------
    [Fact]
    public void InsertAndRetrieve_1000Records_FieldLevelEquality()
    {
        if (_skip) return;

        var tableName = CreateTableAndType("insert", out _);

        var original = GenerateRecords(RecordCount);

        // Insert in batches of 250 (exercises the Avro encode path repeatedly)
        const int batchSize = 250;
        for (int offset = 0; offset < original.Count; offset += batchSize)
        {
            var batch = original.Skip(offset).Take(batchSize).ToList();
            var resp = _kdb!.insertRecords(tableName, batch);
            (resp.count_inserted + resp.count_updated).ShouldBeGreaterThan(0);
        }

        // Retrieve
        var getResp = _kdb!.getRecords<RoundTripRecord>(tableName, 0, RecordCount + 10);
        getResp.data.Count.ShouldBe(RecordCount);

        // Build lookup by Id for comparison (server may return in different order)
        var retrieved = getResp.data.ToDictionary(r => r.Id);

        foreach (var expected in original)
        {
            retrieved.ShouldContainKey(expected.Id);
            var actual = retrieved[expected.Id];

            actual.BigNum.ShouldBe(expected.BigNum);
            actual.Score.ShouldBe(expected.Score, 0.001f);
            actual.NullableValue.ShouldBe(expected.NullableValue);
            actual.Label.ShouldBe(expected.Label);
            actual.Seq.ShouldBe(expected.Seq);
        }
    }

    // -----------------------------------------------------------------
    // §7.6.5 — Multi-head ingest round-trip
    // -----------------------------------------------------------------
    [Fact]
    public void MultiHeadIngest_1000Records_FieldLevelEquality()
    {
        if (_skip) return;

        var tableName = CreateTableAndType("multihead", out var kineticaType);

        var original = GenerateRecords(RecordCount);

        // Use KineticaIngestor for multi-head
        var ingestor = new KineticaIngestor<RoundTripRecord>(
            _kdb!, tableName, batchSize: 200, kineticaType);

        ingestor.insert(original);
        ingestor.flush();

        // Verify count via showTable
        var showResp = _kdb!.showTable(tableName, new Dictionary<string, string>
        {
            [ShowTableRequest.Options.GET_SIZES] = ShowTableRequest.Options.TRUE
        });
        showResp.total_size.ShouldBe(RecordCount);

        // Retrieve and validate
        var getResp = _kdb.getRecords<RoundTripRecord>(tableName, 0, RecordCount + 10);
        getResp.data.Count.ShouldBe(RecordCount);

        var retrieved = getResp.data.ToDictionary(r => r.Id);

        foreach (var expected in original)
        {
            retrieved.ShouldContainKey(expected.Id);
            var actual = retrieved[expected.Id];

            actual.BigNum.ShouldBe(expected.BigNum);
            actual.Score.ShouldBe(expected.Score, 0.001f);
            actual.NullableValue.ShouldBe(expected.NullableValue);
            actual.Label.ShouldBe(expected.Label);
            actual.Seq.ShouldBe(expected.Seq);
        }
    }

    // -----------------------------------------------------------------
    // §7.6.6 — Update records (the endpoint that had a bug in 7.2.2.2)
    // -----------------------------------------------------------------
    [Fact]
    public void UpdateRecords_ModifiesExistingRows()
    {
        if (_skip) return;

        var tableName = CreateTableAndType("update", out _);

        // Insert a small batch
        var records = GenerateRecords(10);
        _kdb!.insertRecords(tableName, records);

        // Update record 0's Score via updateRecords
        var expressions = new List<string> { "Id = 0" };
        var newValuesMaps = new List<IDictionary<string, string>>
        {
            new Dictionary<string, string> { ["Score"] = "999.5" }
        };

        _kdb.updateRecords<RoundTripRecord>(
            tableName,
            expressions,
            newValuesMaps);

        // Retrieve and verify
        var getResp = _kdb.getRecords<RoundTripRecord>(tableName, 0, 100);
        var updated = getResp.data.First(r => r.Id == 0);
        updated.Score.ShouldBe(999.5f, 0.1f);
    }

    // -----------------------------------------------------------------
    // Edge cases: empty strings, all-null nullable fields, extremes
    // -----------------------------------------------------------------
    [Fact]
    public void EdgeCases_EmptyStringAndAllNulls_RoundTrip()
    {
        if (_skip) return;

        var tableName = CreateTableAndType("edge", out _);

        var edgeCases = new List<RoundTripRecord>
        {
            new() { Id = 1, BigNum = long.MinValue, Score = float.MinValue,
                    NullableValue = null, Label = null, Seq = 1 },
            new() { Id = 2, BigNum = long.MaxValue, Score = float.MaxValue,
                    NullableValue = double.MaxValue, Label = "", Seq = 2 },
            new() { Id = 3, BigNum = 0, Score = 0f,
                    NullableValue = 0.0, Label = new string('x', 200), Seq = 3 },
        };

        _kdb!.insertRecords(tableName, edgeCases);

        var getResp = _kdb.getRecords<RoundTripRecord>(tableName, 0, 100);
        getResp.data.Count.ShouldBe(3);

        var retrieved = getResp.data.ToDictionary(r => r.Id);

        // All-null nullable fields
        retrieved[1].NullableValue.ShouldBeNull();
        retrieved[1].Label.ShouldBeNull();
        retrieved[1].BigNum.ShouldBe(long.MinValue);

        // Max values
        retrieved[2].BigNum.ShouldBe(long.MaxValue);
        retrieved[2].Label.ShouldBe("");

        // Long string
        retrieved[3].Label.ShouldBe(new string('x', 200));
    }
}
