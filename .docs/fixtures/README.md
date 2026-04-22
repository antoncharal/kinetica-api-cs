# Wire-Format Fixture Provenance

This directory contains binary wire-format samples captured from the **unmodified**
SDK 7.2.3.0 (commit `b739cf3 Version 7.2.3.0`). They are the reference data used
by PR-07 pinning tests to assert that every subsequent PR preserves binary
compatibility.

## Capture environment

| Key | Value |
|---|---|
| SDK commit | `b739cf3` (Version 7.2.3.0) |
| Capture branch commit | `deb46a2941399e303c7d5278659502acbd3b3483` |
| Kinetica server | Local Docker container, `http://localhost:9191` |
| Kinetica server version | Not returned by this instance's `/show/system/properties` (developer edition) |
| Captured at | 2026-04-22 UTC |
| .NET runtime | .NET 8.0.26 |

## File naming convention

```
{req|res}-{endpoint-slug}-{yyyyMMddHHmmss}.bin
```

- `req-*` — Avro-encoded request body sent to the server.
- `res-*` — Avro-encoded response body received from the server.
- The timestamp suffix disambiguates multiple captures of the same endpoint
  (e.g., from `Example` and from `CaptureDriver`).
- All files are raw binary (Avro-encoded `KineticaData`), not wrapped with an
  Avro container/schema header — they match the exact bytes on the wire.

## Fixtures by target endpoint

The six representative endpoints called out in PR-00 are all covered:

| Endpoint | Fixture prefix | Source | Why included |
|---|---|---|---|
| `/show/system/properties` | `req/res-show-system-properties-*` | CaptureDriver | Simple, no data types |
| `/create/type` | `req/res-create-type-*` | Example | Exercises schema definition |
| `/insert/records` | `req/res-insert-records-*` | Example (×2: small + 500-rec multihead) | Core Avro encoding path |
| `/get/records` | `req/res-get-records-*` | Example | Core Avro decoding path |
| `/execute/sql` | `req/res-execute-sql-*` | CaptureDriver | Response-envelope handling |
| `/update/records` | `req/res-update-records-*` | CaptureDriver | Bug caught in 7.2.2.2 — high regression sensitivity |

### Additional endpoints captured opportunistically

These were produced by the Example run and committed alongside the primary six.
They provide extra coverage for the pinning test suite in PR-07.

| Endpoint | Fixture prefix |
|---|---|
| `/aggregate/groupby` | `req/res-aggregate-groupby-*` |
| `/aggregate/unique` | `req/res-aggregate-unique-*` |
| `/clear/table` | `req/res-clear-table-*` |
| `/create/table` | `req/res-create-table-*` |
| `/filter` | `req/res-filter-*` |
| `/filter/bybox` | `req/res-filter-bybox-*` |
| `/get/records/bycolumn` | `req/res-get-records-bycolumn-*` |
| `/get/records/byseries` | `req/res-get-records-byseries-*` |
| `/show/table` | `req/res-show-table-*` |
| `/show/types` | `req/res-show-types-*` |

## Schema context

### `insert/records` payloads

Captured against `ExampleRecord` (from `Example/Example.cs`):

```
fields: A (int, primary_key), B (int, primary_key, shard_key),
        C (string), D (string, char4, nullable), E (float, nullable),
        F (double, nullable), TIMESTAMP (long, timestamp)
```

The `req-insert-records-*` file from the multihead run contains 500 records
(5 × 100-record batches from `KineticaIngestor`).

### `execute/sql` payloads

Captured against a DDL/DML sequence on `capture_driver_table`:

```sql
CREATE TABLE capture_driver_table (id INTEGER NOT NULL, label VARCHAR(64), value DOUBLE, PRIMARY KEY (id))
INSERT INTO capture_driver_table VALUES (1, 'alpha', 1.1), (2, 'beta', 2.2), (3, 'gamma', 3.3)
SELECT * FROM capture_driver_table
```

Multiple `req/res-execute-sql-*` files correspond to the individual SQL
statements (CREATE, INSERT ×3, SELECT).

### `update/records` payloads

Captured via `RawUpdateRecordsRequest` on `capture_driver_table`:

```
expression:    id = 1
new_values:    { value: "99.9" }
```

## How to use these fixtures in tests (PR-07)

```csharp
// Decode a baseline response fixture and assert structural equality
var baselineBytes = File.ReadAllBytes("fixtures/baseline/res-get-records-20260422093411.bin");
var baseline      = sdk.AvroDecode<GetRecordsResponse<MyRecord>>(new MemoryStream(baselineBytes));

// After PR under test: decode the new response from a live call
var newResponse   = sdk.getRecords<MyRecord>(tableName, 0, 10);

// Assert semantic equality (not byte equality — Avro implementations may differ)
newResponse.data.Should().BeEquivalentTo(baseline.data);
```

> **Do not assert byte equality.** Different Avro encoder implementations may
> produce semantically identical but byte-different output (e.g., union field
> ordering). The fixtures guarantee a known-good decoded object graph, not a
> specific byte sequence.
