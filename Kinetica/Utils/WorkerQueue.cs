using System;
using System.Collections.Generic;


namespace kinetica.Utils;
/// <summary>
/// Thread-safe insertion queue for a single worker.
/// All buffer mutations are guarded by a per-instance lock.
/// HTTP calls happen <em>outside</em> the lock — the drained batch is caller-owned.
/// </summary>
internal sealed class WorkerQueue<T>
{
    private readonly object _gate = new();
    public System.Uri Url { get; private set; }
    private readonly int capacity;
    private List<T> queue;


    /// <summary>
    /// Creates an insertion queue for a given worker.
    /// </summary>
    /// <param name="url"></param>
    public WorkerQueue( System.Uri url )
    {
        this.Url = url;
        this.capacity = 1;

        queue = [];
    }  // end constructor WorkerQueue<T>



    /// <summary>
    /// Creates an insertion queue for a given worker with the given capacity.
    /// </summary>
    /// <param name="url"></param>
    /// <param name="capacity"></param>
    public WorkerQueue(System.Uri url, int capacity)
    {
        this.Url = url;
        this.capacity = capacity;

        queue = [];

    }  // end constructor WorkerQueue<T>



    /// <summary>
    /// Returns the current queue and creates a new empty one.
    /// Thread-safe: acquires the per-queue lock.
    /// </summary>
    /// <returns>A list of records to be inserted.</returns>
    public IList<T> flush()
    {
        lock (_gate)
        {
            return Drain();
        }
    }  // end flush



    /// <summary>
    /// Inserts a record into the queue (if all conditions are
    /// favourable).  Returns the queue if it becomes full upon insertion.
    /// Thread-safe: acquires the per-queue lock.
    /// </summary>
    /// <param name="record">The record to insert into the queue.</param>
    /// <param name="key">A primary key, if any.</param>
    /// <returns>The list of records (if the queue is full), or null.</returns>
    public IList<T>? insert(T record, RecordKey key)
    {
        lock (_gate)
        {
            queue.Add(record);
            // If the queue is full, then flush and return the 'old' queue
            if (queue.Count == capacity)
                return Drain();
            else // no records to return
                return null;
        }
    }  // end insert

    /// <summary>
    /// Drains the buffer and returns the old list. Must be called under <see cref="_gate"/>.
    /// </summary>
    private IList<T> Drain()
    {
        var old_queue = this.queue;
        queue = new List<T>(this.capacity);
        return old_queue;
    }
}  // end class WorkerQueue

