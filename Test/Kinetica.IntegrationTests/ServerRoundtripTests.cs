using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Kinetica.IntegrationTests.Fixtures;
using kinetica;
using Shouldly;
using Xunit;

namespace Kinetica.IntegrationTests;

/// <summary>
/// End-to-end scenarios against a live Kinetica 7.2 server.
/// Skipped automatically when <c>KINETICA_URL</c> is not set.
/// </summary>
[Collection(KineticaServerCollection.Name)]
public sealed class ServerRoundtripTests
{
    private readonly KineticaServerFixture _fixture;

    public ServerRoundtripTests(KineticaServerFixture fixture) => _fixture = fixture;

    // -------------------------------------------------------------------------
    // Smoke test
    // -------------------------------------------------------------------------

    [KineticaServerFact]
    public void ShowSystemProperties_Roundtrip()
    {
        var result = _fixture.Kdb!.showSystemProperties();

        result.ShouldNotBeNull();
        result.property_map.ShouldNotBeNull();
        result.property_map.ShouldContainKey("version");
    }

    // -------------------------------------------------------------------------
    // Table CRUD
    // -------------------------------------------------------------------------

    [KineticaServerFact]
    public void CreateTable_InsertRecords_RetrieveRecords_Equal()
    {
        var tableName = _fixture.QualifiedTable("crud_test")!;
        var kdb = _fixture.Kdb!;

        // Create type + table
        var ktype = KineticaType.fromClass(typeof(SimpleRecord));
        var typeId = ktype.create(kdb);
        kdb.createTable(tableName, typeId);

        // Insert
        var records = new List<SimpleRecord>
        {
            new() { id = 1, name = "Alice",   value = 10.5 },
            new() { id = 2, name = "Bob",     value = 20.0 },
            new() { id = 3, name = "Charlie", value = 30.7 },
        };
        var insertResponse = kdb.insertRecords(tableName, records);

        insertResponse.count_inserted.ShouldBe(3);

        // Retrieve
        var getResponse = kdb.getRecords<SimpleRecord>(tableName, 0, 100);
        getResponse.data.Count.ShouldBe(3);
    }

    // -------------------------------------------------------------------------
    // Async ingest
    // -------------------------------------------------------------------------

    [KineticaServerFact]
    public async Task InsertAsync_BatchOf1000_CountersMatch()
    {
        var tableName = _fixture.QualifiedTable("async_ingest_test")!;
        var kdb = _fixture.Kdb!;

        var ktype = KineticaType.fromClass(typeof(SimpleRecord));
        var typeId = ktype.create(kdb);
        kdb.createTable(tableName, typeId);

        var ingestor = new KineticaIngestor<SimpleRecord>(kdb, tableName, 200, ktype);

        for (int i = 0; i < 1000; i++)
            await ingestor.InsertAsync(new SimpleRecord { id = i, name = $"row_{i}", value = i * 1.1 });

        ingestor.flush();

        ingestor.getCountInserted().ShouldBe(1000);
    }

    // -------------------------------------------------------------------------
    // Cancellation
    // -------------------------------------------------------------------------

    [KineticaServerFact]
    public async Task CancelLongRunningExecuteSql_ThrowsOperationCanceledException()
    {
        var kdb = _fixture.Kdb!;
        using var cts = new CancellationTokenSource();
        cts.Cancel(); // pre-cancel for deterministic behavior

        await Should.ThrowAsync<OperationCanceledException>(
            () => kdb.SubmitRequestAsync<ExecuteSqlResponse>(
                "/execute/sql",
                new ExecuteSqlRequest
                {
                    statement = "SELECT 1",
                },
                cancellationToken: cts.Token));
    }

    // -------------------------------------------------------------------------
    // Update records
    // -------------------------------------------------------------------------

    [KineticaServerFact]
    public void UpdateRecords_Roundtrip()
    {
        var tableName = _fixture.QualifiedTable("update_test")!;
        var kdb = _fixture.Kdb!;

        // Create type with primary key on id
        var columnProperties = new Dictionary<string, IList<string>>
        {
            { "id", new List<string> { ColumnProperty.PRIMARY_KEY } }
        };
        var ktype = KineticaType.fromClass(typeof(SimpleRecord), columnProperties);
        var typeId = ktype.create(kdb);
        kdb.createTable(tableName, typeId);

        // Insert initial record
        kdb.insertRecords(tableName, new List<SimpleRecord>
        {
            new() { id = 1, name = "original", value = 100.0 }
        });

        // Update via SQL
        kdb.executeSql($"UPDATE {tableName} SET name = 'updated' WHERE id = 1");

        // Verify
        var result = kdb.getRecords<SimpleRecord>(tableName, 0, 10);
        result.data.Count.ShouldBe(1);
        result.data[0].name.ShouldBe("updated");
    }

    // =========================================================================
    // Test record type
    // =========================================================================

    public class SimpleRecord
    {
        public int id { get; set; }
        public string? name { get; set; }
        public double value { get; set; }
    }
}
