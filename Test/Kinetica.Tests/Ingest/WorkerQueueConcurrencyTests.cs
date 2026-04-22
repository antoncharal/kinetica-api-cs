using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using kinetica.Utils;
using Shouldly;
using Xunit;

namespace Kinetica.Tests.Ingest;

/// <summary>
/// Concurrency contract tests for <see cref="WorkerQueue{T}"/>
/// introduced in PR-08. Validates that the per-queue lock prevents
/// data loss under contention.
/// </summary>
public sealed class WorkerQueueConcurrencyTests
{
    // -------------------------------------------------------------------------
    // Thread-safety: concurrent inserts don't lose records
    // -------------------------------------------------------------------------

    [Fact]
    public async Task Insert_ConcurrentAccess_NoLostRecords()
    {
        const int batchSize = 50;
        const int totalRecords = 1000;
        var queue = new WorkerQueue<int>(
            new System.Uri("http://localhost:9191/insert/records"),
            batchSize, false, false);

        var flushedBatches = new ConcurrentBag<IList<int>>();

        // Launch 10 concurrent producers inserting 100 records each.
        var tasks = Enumerable.Range(0, 10).Select(producerIndex =>
            Task.Run(() =>
            {
                for (int i = 0; i < 100; i++)
                {
                    var flushed = queue.insert(producerIndex * 100 + i, null);
                    if (flushed != null)
                        flushedBatches.Add(flushed);
                }
            }));

        await Task.WhenAll(tasks);

        // Drain remaining records.
        var remaining = queue.flush();

        // Total records must equal totalRecords.
        var totalFlushed = flushedBatches.Sum(b => b.Count) + remaining.Count;
        totalFlushed.ShouldBe(totalRecords);

        // All flushed batches should have exactly batchSize records (except possibly the last).
        foreach (var batch in flushedBatches)
            batch.Count.ShouldBe(batchSize);

        // Verify no duplicates — every record value 0..999 must appear exactly once.
        var allRecords = flushedBatches.SelectMany(b => b).Concat(remaining).OrderBy(x => x).ToList();
        allRecords.ShouldBe(Enumerable.Range(0, totalRecords).ToList());
    }

    // -------------------------------------------------------------------------
    // Flush under contention: no double-drain
    // -------------------------------------------------------------------------

    [Fact]
    public async Task Flush_ConcurrentCalls_NoDuplicateRecords()
    {
        const int batchSize = 100;
        var queue = new WorkerQueue<int>(
            new System.Uri("http://localhost:9191/insert/records"),
            batchSize, false, false);

        // Pre-fill the queue with 50 records.
        for (int i = 0; i < 50; i++)
            queue.insert(i, null);

        // Flush from two tasks simultaneously.
        var results = new ConcurrentBag<IList<int>>();
        var tasks = Enumerable.Range(0, 2).Select(_ =>
            Task.Run(() => results.Add(queue.flush())));

        await Task.WhenAll(tasks);

        // Exactly one flush should get the 50 records, the other should get 0.
        var totalRecords = results.Sum(r => r.Count);
        totalRecords.ShouldBe(50);
    }

    // -------------------------------------------------------------------------
    // Basic contract: batch triggers flush at capacity
    // -------------------------------------------------------------------------

    [Fact]
    public void Insert_ReachesCapacity_ReturnsFlushedBatch()
    {
        const int batchSize = 5;
        var queue = new WorkerQueue<string>(
            new System.Uri("http://localhost:9191/insert/records"),
            batchSize, false, false);

        for (int i = 0; i < batchSize - 1; i++)
            queue.insert($"record-{i}", null).ShouldBeNull();

        var flushed = queue.insert("record-last", null);
        flushed.ShouldNotBeNull();
        flushed!.Count.ShouldBe(batchSize);
    }

    [Fact]
    public void Insert_BelowCapacity_ReturnsNull()
    {
        var queue = new WorkerQueue<int>(
            new System.Uri("http://localhost:9191/insert/records"),
            10, false, false);

        queue.insert(42, null).ShouldBeNull();
    }

    [Fact]
    public void Flush_EmptyQueue_ReturnsEmptyList()
    {
        var queue = new WorkerQueue<int>(
            new System.Uri("http://localhost:9191/insert/records"),
            10, false, false);

        var result = queue.flush();
        result.ShouldNotBeNull();
        result.Count.ShouldBe(0);
    }
}
