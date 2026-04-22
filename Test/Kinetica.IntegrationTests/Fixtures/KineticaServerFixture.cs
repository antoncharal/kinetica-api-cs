using System;
using System.Collections.Generic;
using kinetica;

namespace Kinetica.IntegrationTests.Fixtures;

/// <summary>
/// Shared fixture that connects to a live Kinetica server and manages a
/// scratch schema for test isolation.  Creates a unique schema on
/// construction and drops it on disposal.  When <c>KINETICA_URL</c> is
/// not set, the fixture initializes in a dormant state — tests using
/// <see cref="KineticaServerFact"/> will be skipped before touching
/// <see cref="Kdb"/>.
/// </summary>
public sealed class KineticaServerFixture : IDisposable
{
    public kinetica.Kinetica? Kdb { get; }
    public string? SchemaName { get; }

    public KineticaServerFixture()
    {
        var url = Environment.GetEnvironmentVariable("KINETICA_URL");

        if (string.IsNullOrEmpty(url))
            return; // Dormant — tests will be skipped by KineticaServerFact.

        Kdb = new kinetica.Kinetica(url);
        SchemaName = $"ki_tests_{Guid.NewGuid():N}"[..30];

        Kdb.createSchema(SchemaName);
    }

    /// <summary>
    /// Returns a fully-qualified table name within the scratch schema.
    /// </summary>
    public string QualifiedTable(string tableName) => $"{SchemaName}.{tableName}";

    public void Dispose()
    {
        if (Kdb is null || SchemaName is null)
            return;

        try
        {
            Kdb.dropSchema(SchemaName,
                new Dictionary<string, string>
                {
                    [DropSchemaRequest.Options.CASCADE] = DropSchemaRequest.Options.TRUE
                });
        }
        catch
        {
            // Best-effort cleanup — don't fail teardown.
        }
    }
}
