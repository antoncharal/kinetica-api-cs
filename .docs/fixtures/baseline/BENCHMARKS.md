# Baseline Performance Benchmarks

Captured with the **unmodified** SDK 7.2.3.0 before any modernization work.
These numbers are the reference floor for PR-03 (async) and PR-04 (HttpClient).

## Environment

| Key | Value |
|---|---|
| Machine | 22 logical CPUs, 64980 MB RAM |
| OS | Microsoft Windows 10.0.26200 |
| .NET runtime | .NET 8.0.26 |
| Kinetica server | unknown |
| Kinetica server URL | http://localhost:9191 |
| Commit SHA | `88931afca48d19cbee2e2a766e12a4d6aaa5b011` |
| Captured at | 2026-04-22 10:03:57 UTC |
| Iterations per measurement | 5 |

## insertRecords

Single synchronous `insertRecords` call, warm connection, pre-created table.
Table is dropped and recreated between iterations to avoid primary key collisions.

| Records | min (ms) | median (ms) | p95 (ms) |
|---|---|---|---|
|    10 |     16 |        18 |       23 |
|   100 |     15 |        16 |       23 |
|  1000 |     60 |        64 |       70 |

## getRecords

Single synchronous `getRecords` call, offset=0, limit=N, against a stable dataset.

| Records | min (ms) | median (ms) | p95 (ms) |
|---|---|---|---|
|    10 |     13 |        15 |       16 |
|   100 |     20 |        21 |       24 |
|  1000 |     65 |        69 |       76 |

## KineticaIngestor throughput

Multi-head `KineticaIngestor`, batch size 250, 1000 records, table with primary key + shard key on `id`.

| Records | min (ms) | median (ms) | p95 (ms) | Throughput (rec/s, median) |
|---|---|---|---|---|
|  1000 |    249 |       257 |      260 |       3891 |

## Notes

- Timings include full round-trip: serialization + network + server processing + deserialization.
- Kinetica instance is a local Docker container; absolute numbers will be lower in production.
- Use these numbers only for **relative regression detection** across PRs, not as absolute targets.
