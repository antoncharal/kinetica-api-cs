using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;


namespace kinetica
{
    /// <summary>
    /// Manages the bulk insertion of records into a Kinetica table with automatic
    /// batch management and support for multi-head ingest.
    /// <para>
    /// Use <see cref="Insert(T)"/> / <see cref="Insert(IList{T})"/> to queue records
    /// for insertion, <see cref="Flush"/> to drain all queued records synchronously,
    /// or the async equivalents (<see cref="InsertAsync(T, CancellationToken)"/>,
    /// <see cref="InsertAsync(IList{T}, CancellationToken)"/>, <see cref="FlushAsync"/>)
    /// for non-blocking operation.
    /// </para>
    /// <para>
    /// <b>Thread safety:</b> Concurrent calls to <see cref="InsertAsync(T, CancellationToken)"/>
    /// and <see cref="InsertAsync(IList{T}, CancellationToken)"/> are safe.
    /// Worker queues are guarded by per-instance locks. Counter reads
    /// (<see cref="getCountInserted"/>, <see cref="getCountUpdated"/>)
    /// are atomic at any time. <see cref="FlushAsync"/> is safe to call
    /// concurrently with inserts, but only one flush executes at a time
    /// per worker queue.
    /// </para>
    /// </summary>
    /// <typeparam name="T">The CLR type of the records being inserted. Must match the
    /// Kinetica table schema described by the <see cref="KineticaType"/> passed at
    /// construction time.</typeparam>
    public sealed class KineticaIngestor<T>
    {
        /// <summary>
        /// Thrown when one or more records cannot be inserted.
        /// </summary>
        /// <remarks>
        /// <b>Design note (E7 — deferred):</b>
        /// Ideally this exception would live at the <c>kinetica</c> namespace level so that
        /// callers could <c>catch (InsertException)</c> without needing to know about
        /// <c>KineticaIngestor&lt;T&gt;</c>.  Promoting it is a breaking API change
        /// (the fully-qualified name changes from
        /// <c>kinetica.KineticaIngestor&lt;T&gt;.InsertException</c> to
        /// <c>kinetica.InsertException</c>) and is therefore deferred to the next major
        /// version bump.
        /// </remarks>
        [Serializable]
        public class InsertException : KineticaException
        {
            /// <summary>The worker URL that rejected the batch.</summary>
            public Uri Url { get; private set; }

            /// <summary>The records that were not delivered to the server.</summary>
            public IReadOnlyList<T> Records { get; private set; }

            /// <inheritdoc cref="Url"/>
            [Obsolete("Use Url instead.")]
            public Uri url => Url;

            /// <inheritdoc cref="Records"/>
            [Obsolete("Use Records instead.")]
            public IList<T> records => (IList<T>)Records;

            /// <summary>
            /// Initialises an <see cref="InsertException"/> with the specified message
            /// and no record or URL context.
            /// </summary>
            /// <param name="msg">A human-readable description of the insert failure.</param>
            public InsertException( string msg ) : base( msg ) { }

            internal InsertException( Uri url_, IList<T> records_, string msg ) : base( msg )
            {
                this.Url = url_;
                this.Records = records_.AsReadOnly();
            }

            /// <inheritdoc />
            public override string ToString() => $"InsertException: {Message}";
        }  // end class InsertException




        // KineticaIngestor Members:
        // =========================

        // Stored as the interface so test doubles can be injected via the internal constructor.
        private readonly IKineticaClient _kdb;

        /// <summary>
        /// The underlying Kinetica client.  Internal — use <see cref="Options"/> and the
        /// public insertion methods rather than reaching into the client directly.
        /// </summary>
        internal Kinetica KineticaDb => (Kinetica)_kdb;

        /// <summary>The fully-qualified name of the target Kinetica table.</summary>
        public string TableName { get; }

        /// <summary>The number of records to accumulate per worker queue before an automatic flush.</summary>
        public int BatchSize { get; }

        /// <summary>
        /// The options passed to the ingestor at construction time. Read-only after construction.
        /// </summary>
        public IReadOnlyDictionary<string, string> Options => _options;
        private readonly Dictionary<string, string> _options;

        [Obsolete("Use KineticaDb instead.")]
        internal Kinetica kineticaDB => (Kinetica)_kdb;

        /// <inheritdoc cref="TableName"/>
        [Obsolete("Use TableName instead.")]
        public string table_name => TableName;

        /// <inheritdoc cref="BatchSize"/>
        [Obsolete("Use BatchSize instead.")]
        public int batch_size => BatchSize;

        /// <inheritdoc cref="Options"/>
        [Obsolete("Use Options instead.")]
        public IDictionary<string, string> options => _options;
        private long _countInserted;
        private long _countUpdated;

        /// <summary>Gets the number of records inserted so far. Reads are atomic.</summary>
        public long CountInserted => Interlocked.Read(ref _countInserted);

        /// <summary>Gets the number of records updated so far. Reads are atomic.</summary>
        public long CountUpdated => Interlocked.Read(ref _countUpdated);
        private KineticaType ktype;
        private Utils.RecordKeyBuilder<T>? primaryKeyBuilder;
        private Utils.RecordKeyBuilder<T>? shardKeyBuilder;
        private IList<int>? routingTable;
        private IList<Utils.WorkerQueue<T>> workerQueues;


        /// <summary>
        /// Creates a <see cref="KineticaIngestor{T}"/> with the specified parameters.
        /// </summary>
        /// <param name="kdb">The <see cref="Kinetica"/> client used to communicate with the server.</param>
        /// <param name="tableName">The fully-qualified name of the target Kinetica table.</param>
        /// <param name="batchSize">The number of records to accumulate per worker queue before
        /// automatically flushing.  Must be ≥ 1.</param>
        /// <param name="ktype">The <see cref="KineticaType"/> describing the schema of <typeparamref name="T"/>.</param>
        /// <param name="options">Optional insert options forwarded to the <c>/insert/records</c>
        /// endpoint (e.g. <c>update_on_existing_pk</c>).  Pass <c>null</c> for defaults.</param>
        /// <param name="workers">Optional pre-built worker list for multi-head ingest.  When
        /// <c>null</c> or empty the ingestor queries the server for worker URLs automatically.</param>
        /// <exception cref="KineticaException">
        /// Thrown when <paramref name="batchSize"/> is less than 1, or when the server returns
        /// an error while fetching worker information.
        /// </exception>
        public KineticaIngestor( Kinetica kdb, string tableName,
                                 int batchSize, KineticaType ktype,
                                 Dictionary<string, string>? options = null,
                                 Utils.WorkerList? workers = null )
            : this( (IKineticaClient)kdb, tableName, batchSize, ktype, options, workers ) { }

        /// <summary>
        /// Internal constructor that accepts an <see cref="IKineticaClient"/> directly,
        /// enabling injection of test doubles via <c>InternalsVisibleTo</c>.
        /// </summary>
        internal KineticaIngestor( IKineticaClient kdb, string tableName,
                                   int batchSize, KineticaType ktype,
                                   Dictionary<string, string>? options = null,
                                   Utils.WorkerList? workers = null )
        {
            this._kdb = kdb;
            this.TableName = tableName;
            this.ktype = ktype;

            if ( batchSize < 1 )
                throw new KineticaException( $"Batch size must be greater than one; given {batchSize}." );
            this.BatchSize = batchSize;

            // Defensive copy — callers cannot mutate the ingestor's options after construction.
            _options = options is not null ? new Dictionary<string, string>(options) : [];

            InitKeyBuilders();
            InitWorkerQueues( kdb, workers );
        }   // end constructor KineticaIngestor

        /// <summary>
        /// Resolves the primary and shard key builders for type <typeparamref name="T"/>.
        /// Sets <see cref="primaryKeyBuilder"/> and <see cref="shardKeyBuilder"/> to
        /// <c>null</c> when no key is defined; collapses the shard key onto the primary
        /// key when both point to the same column set.
        /// </summary>
        private void InitKeyBuilders()
        {
            this.primaryKeyBuilder = new Utils.RecordKeyBuilder<T>( true,  this.ktype );
            this.shardKeyBuilder   = new Utils.RecordKeyBuilder<T>( false, this.ktype );

            if ( this.primaryKeyBuilder.hasKey() )
            {
                if ( !this.shardKeyBuilder.hasKey()
                     || this.shardKeyBuilder.hasSameKey( this.primaryKeyBuilder ) )
                    this.shardKeyBuilder = this.primaryKeyBuilder;
            }
            else
            {
                this.primaryKeyBuilder = null;
                if ( !this.shardKeyBuilder.hasKey() )
                    this.shardKeyBuilder = null;
            }
        }

        /// <summary>
        /// Acquires the list of worker URLs (from the server if not provided), constructs
        /// per-worker <see cref="Utils.WorkerQueue{T}"/> instances, and fetches the routing
        /// table for multi-head ingest.  Falls back to a single queue pointing at the
        /// primary server URL when multi-head is disabled.
        /// </summary>
        private void InitWorkerQueues( IKineticaClient kdb, Utils.WorkerList? workers )
        {
            this.workerQueues = [];
            try
            {
                if ( ( workers == null ) || ( workers.Count == 0 ) )
                {
                    if ( kdb is Kinetica concreteKdb )
                        workers = new Utils.WorkerList( concreteKdb );
                    // If kdb is a test double, leave workers empty — single-head mode.
                }

                if ( ( workers != null ) && ( workers.Count > 0 ) )
                {
                    foreach ( System.Uri workerUrl in workers )
                    {
                        string strWorkerUrl = workerUrl.ToString();
                        strWorkerUrl = strWorkerUrl.EndsWith('/') ? strWorkerUrl[..^1] : strWorkerUrl;
                        System.Uri url = new( $"{strWorkerUrl}/insert/records" );
                        this.workerQueues.Add( new Utils.WorkerQueue<T>( url, this.BatchSize ) );
                    }

                    this.routingTable = kdb.adminShowShards().rank;
                    for ( int i = 0; i < routingTable.Count; ++i )
                        if ( this.routingTable[i] > this.workerQueues.Count )
                            throw new KineticaException( "Not enough worker URLs specified." );
                }
                else
                {
                    string strWorkerUrl = kdb.Uri.ToString();
                    strWorkerUrl = strWorkerUrl.EndsWith('/') ? strWorkerUrl[..^1] : strWorkerUrl;
                    System.Uri url = new( $"{strWorkerUrl}/insert/records" );
                    this.workerQueues.Add( new Utils.WorkerQueue<T>( url, this.BatchSize ) );
                    this.routingTable = null;
                }
            }
            catch ( KineticaException ) { throw; }
            catch ( Exception ex ) { throw new KineticaException( ex.ToString() ); }
        }


        /// <summary>
        /// Returns the count of records inserted so far.  An atomic operation.
        /// </summary>
        /// <returns>The number of records inserted into Kinetica through this
        /// ingestor so far.</returns>
        [Obsolete("Use CountInserted property instead.")]
        public long getCountInserted()
        {
            return Interlocked.Read( ref _countInserted );
        }


        /// <summary>
        /// Returns the count of records updated so far.  An atomic operation.
        /// </summary>
        /// <returns>The number of records updated into Kinetica through this
        /// ingestor so far.</returns>
        [Obsolete("Use CountUpdated property instead.")]
        public long getCountUpdated()
        {
            return Interlocked.Read( ref _countUpdated );
        }


        /// <summary>
        /// Determines which worker queue a record should be routed to based on
        /// the shard key and routing table (or random if no key is available).
        /// </summary>
        /// <returns>
        /// A tuple of the target <see cref="Utils.WorkerQueue{T}"/> and its zero-based
        /// index in <see cref="workerQueues"/>.  The index is read directly from the
        /// routing table — no O(n) <c>IndexOf</c> scan is needed at the call site.
        /// </returns>
        private (Utils.WorkerQueue<T> Queue, int Index) RouteFor(T record, out Utils.RecordKey? primaryKey)
        {
            primaryKey = null;
            Utils.RecordKey? shardKey = null;

            if (this.primaryKeyBuilder != null)
                primaryKey = this.primaryKeyBuilder.build(record);
            if (this.shardKeyBuilder != null)
                shardKey = this.shardKeyBuilder.build(record);

            if (this.routingTable == null)
                return (this.workerQueues[0], 0);
            if (shardKey == null)
            {
                int rnd = Random.Shared.Next(this.workerQueues.Count);
                return (this.workerQueues[rnd], rnd);
            }

            int idx = shardKey.route(this.routingTable);
            return (this.workerQueues[idx], idx);
        }


        /// <summary>
        /// Ensures that all queued records are inserted into Kinetica.
        /// </summary>
        /// <exception cref="InsertException">
        /// Thrown when one or more records cannot be delivered to the server.
        /// The exception carries the undelivered records and the worker URL that
        /// failed, allowing callers to re-queue or log the failures.
        /// </exception>
        public void Flush()
        {
            foreach ( Utils.WorkerQueue<T> workerQueue in this.workerQueues )
            {
                IList<T> queue = workerQueue.flush();
                flush( queue, workerQueue.Url );
            }
        }

        /// <summary>Flushes all queued records.</summary>
        [Obsolete("Use Flush() instead.")]
        public void flush()
        {
            foreach ( Utils.WorkerQueue<T> workerQueue in this.workerQueues )
            {
                // Flush the queue
                IList<T> queue = workerQueue.flush();
                // Actually insert the records
                flush( queue, workerQueue.Url );
            }
        }  // end public flush


        /// <summary>
        /// Insert the given list of records to the database residing at the given URL.
        /// Upon any error, thrown InsertException with the queue of records passed into it.
        /// </summary>
        /// <param name="queue">The list or records to insert.</param>
        /// <param name="url">The address of the Kinetica worker.</param>
        private void flush( IList<T> queue, System.Uri url )
        {
            if ( queue.Count == 0 )
                return; // nothing to do since the queue is empty

            try
            {
                // Create the /insert/records request and response objects
                // -------------------------------------------------------
                // Encode the records into binary
                List<byte[]> encodedQueue = [];
                foreach ( var record in queue ) encodedQueue.Add( _kdb.AvroEncode( record ) );
                RawInsertRecordsRequest request = new( this.TableName, encodedQueue, this._options);

                InsertRecordsResponse response = new();

                if ( url == null )
                {
                    response = _kdb.insertRecordsRaw( request );
                }
                else
                {
                    response = _kdb.SubmitRequest<InsertRecordsResponse>( url, request );
                }

                // Save the counts of inserted and updated records
                System.Threading.Interlocked.Add( ref _countInserted, response.count_inserted );
                System.Threading.Interlocked.Add( ref _countUpdated, response.count_updated );
            }
            catch ( Exception ex )
            {
                throw new InsertException( url, queue, ex.Message );
            }
        }  // end private flush()



        /// <summary>
        /// Queues a record for insertion into Kinetica.
        /// </summary>
        public void Insert( T record )
        {
            var (workerQueue, _) = RouteFor(record, out var primaryKey);
            IList<T> queue = workerQueue.insert( record, primaryKey );
            if ( queue != null )
                this.flush( queue, workerQueue.Url );
        }

        /// <summary>Queues a record for insertion.</summary>
        [Obsolete("Use Insert() instead.")]
        public void insert( T record )
        {
            var (workerQueue, _) = RouteFor(record, out var primaryKey);

            // Insert the record into the queue
            IList<T> queue = workerQueue.insert( record, primaryKey );

            // If inserting the queue resulted in flushing the queue, then flush it
            // properly
            if ( queue != null )
            {
                this.flush( queue, workerQueue.Url );
            }
        }  // end insert( record )



        /// <summary>Queues a list of records for insertion into Kinetica.</summary>
        public void Insert( IList<T> records)
        {
            for ( int i = 0; i < records.Count; ++i )
            {
                try { this.Insert( records[ i ] ); }
                catch ( InsertException ex )
                {
                    IList<T> queue = (IList<T>)ex.Records;
                    for ( int j = i + 1; j < records.Count; ++j ) queue.Add( records[ j ] );
                    throw;
                }
            }
        }

        /// <summary>Queues a list of records for insertion.</summary>
        [Obsolete("Use Insert(IList<T>) instead.")]
        public void insert( IList<T> records)
        {
            // Insert one record at a time
            for ( int i = 0; i < records.Count; ++i )
            {
                try
                {
                    this.insert( records[ i ] );
                }
                catch ( InsertException ex )
                {
                    // Add the remaining records to the insertion exception
                    // record queue
                    IList<T> queue = (IList<T>)ex.Records;

                    for ( int j = i + 1; j < records.Count; ++j )
                    {
                        queue.Add( records[ j ] );
                    }

                    // Rethrow, preserving the original stack trace
                    throw;
                }  // end try-catch
            }  // end outer for loop
        }  // end insert( records )


        // -------------------------------------------------------------------------
        // Async API
        // -------------------------------------------------------------------------

        /// <summary>
        /// Ensures all queued records are flushed asynchronously.
        /// Worker queues are drained in parallel with bounded concurrency
        /// (capped at <see cref="Environment.ProcessorCount"/>), matching the
        /// fan-out behaviour of <see cref="InsertAsync(IList{T}, CancellationToken)"/>.
        /// </summary>
        /// <param name="cancellationToken">Token to cancel the operation.</param>
        /// <returns>
        /// An <see cref="IngestionResult"/> with the aggregate inserted/updated counts
        /// and the number of batches sent.  Returns a zero-count result when all
        /// queues are empty.
        /// </returns>
        /// <exception cref="OperationCanceledException">
        /// Thrown when <paramref name="cancellationToken"/> fires and no bucket
        /// failures have accumulated.  When both cancellation and bucket failures
        /// occur, see <see cref="AggregateException"/> below.
        /// </exception>
        /// <exception cref="AggregateException">
        /// Thrown when one or more worker-queue flushes fail.  Each inner exception
        /// is an <see cref="InsertException"/> carrying the failed records and the
        /// worker URL.  If cancellation also fired, the <see cref="OperationCanceledException"/>
        /// is prepended to the inner exception list.
        /// </exception>
        public async Task<IngestionResult> FlushAsync(CancellationToken cancellationToken = default)
        {
            // Drain every worker queue upfront (each flush() is lock-guarded internally).
            var pending = this.workerQueues
                .Select(wq => (queue: wq.flush(), wq.Url))
                .Where(p => p.queue.Count > 0)
                .ToList();

            if (pending.Count == 0)
                return new IngestionResult(0, 0, 0);

            var maxConcurrency = Math.Min(pending.Count, Environment.ProcessorCount);
            using var gate = new SemaphoreSlim(maxConcurrency, maxConcurrency);
            var results    = new System.Collections.Concurrent.ConcurrentBag<IngestionResult>();
            var exceptions = new System.Collections.Concurrent.ConcurrentBag<Exception>();

            var tasks = pending.Select(async p =>
            {
                await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    results.Add(await FlushAsync(p.queue, p.Url, cancellationToken).ConfigureAwait(false));
                }
                catch (OperationCanceledException) { throw; }
                catch (Exception ex)               { exceptions.Add(ex); }
                finally                            { gate.Release(); }
            }).ToList();

            try
            {
                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
            catch (OperationCanceledException oce)
            {
                if (!exceptions.IsEmpty)
                    throw new AggregateException(exceptions.Prepend(oce));
                throw;
            }

            if (!exceptions.IsEmpty)
                throw new AggregateException(exceptions);

            long totalInserted  = 0;
            long totalUpdated   = 0;
            int  flushedBatches = 0;
            foreach (var r in results)
            {
                totalInserted  += r.Inserted;
                totalUpdated   += r.Updated;
                flushedBatches += r.FlushedBatches;
            }

            return new IngestionResult(totalInserted, totalUpdated, flushedBatches);
        }

        private async Task<IngestionResult> FlushAsync(
            IList<T> queue,
            Uri url,
            CancellationToken cancellationToken)
        {
            if (queue.Count == 0)
                return new IngestionResult(0, 0, 0);

            try
            {
                List<byte[]> encodedQueue = [];
                foreach (var record in queue)
                    encodedQueue.Add(_kdb.AvroEncode(record));

                var request = new RawInsertRecordsRequest(this.TableName, encodedQueue, this._options);

                InsertRecordsResponse response;
                if (url == null)
                {
                    response = await this.KineticaDb
                        .SubmitRequestAsync<InsertRecordsResponse>("/insert/records", request,
                            cancellationToken: cancellationToken)
                        .ConfigureAwait(false);
                }
                else
                {
                    response = await this.KineticaDb
                        .SubmitRequestAsync<InsertRecordsResponse>(url, request,
                            cancellationToken: cancellationToken)
                        .ConfigureAwait(false);
                }

                System.Threading.Interlocked.Add(ref _countInserted, response.count_inserted);
                System.Threading.Interlocked.Add(ref _countUpdated, response.count_updated);

                return new IngestionResult(response.count_inserted, response.count_updated, 1);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new InsertException(url, queue, ex.Message);
            }
        }

        /// <summary>
        /// Queues <paramref name="record"/> for insertion.  If the queue reaches
        /// the batch size, flushes asynchronously before returning.
        /// </summary>
        public async Task InsertAsync(T record, CancellationToken cancellationToken = default)
        {
            var (workerQueue, _) = RouteFor(record, out var primaryKey);

            var queue = workerQueue.insert(record, primaryKey);
            if (queue != null)
                await FlushAsync(queue, workerQueue.Url, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Queues each record for insertion asynchronously with parallel fan-out
        /// across worker queues.  Records are bucketed by their shard-routed worker
        /// (O(1) routing — index returned directly from <c>RouteFor</c>), then each
        /// bucket is inserted concurrently with bounded parallelism capped at
        /// <see cref="Environment.ProcessorCount"/>.
        /// </summary>
        /// <param name="records">The records to insert.  A zero-length list is a no-op.</param>
        /// <param name="cancellationToken">Token to cancel the operation.</param>
        /// <exception cref="OperationCanceledException">
        /// Thrown when <paramref name="cancellationToken"/> fires and no bucket
        /// failures have accumulated.  Callers awaiting this method will see
        /// <see cref="OperationCanceledException"/> on clean cancellation.
        /// </exception>
        /// <exception cref="AggregateException">
        /// Thrown when one or more buckets fail to insert.  Each inner exception is
        /// an <see cref="InsertException"/> carrying the failed records and the
        /// worker URL.  If cancellation also fired, the
        /// <see cref="OperationCanceledException"/> is prepended to the inner list,
        /// so callers should check <see cref="AggregateException.InnerExceptions"/>
        /// rather than assuming the first inner exception indicates the root cause.
        /// </exception>
        public async Task InsertAsync(IList<T> records, CancellationToken cancellationToken = default)
        {
            if (records.Count == 0)
                return;

            // Bucket records per worker queue — RouteFor returns the index directly
            // from the routing table, so no O(n) IndexOf scan is needed here.
            var buckets = new Dictionary<int, List<T>>();
            foreach (var record in records)
            {
                var (_, index) = RouteFor(record, out _);
                if (!buckets.TryGetValue(index, out var list))
                    buckets[index] = list = [];
                list.Add(record);
            }

            // Fan out per bucket with bounded concurrency.
            var maxConcurrency = Math.Min(buckets.Count, Environment.ProcessorCount);
            using var gate = new SemaphoreSlim(maxConcurrency, maxConcurrency);
            var exceptions = new System.Collections.Concurrent.ConcurrentBag<Exception>();

            var tasks = new List<Task>(buckets.Count);
            foreach (var kvp in buckets)
            {
                var workerIndex = kvp.Key;
                var bucketRecords = kvp.Value;
                tasks.Add(InsertBucketAsync(workerIndex, bucketRecords, gate, exceptions, cancellationToken));
            }

            try
            {
                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
            catch (OperationCanceledException oce)
            {
                if (!exceptions.IsEmpty)
                    throw new AggregateException(exceptions.Prepend(oce));
                throw;
            }

            if (!exceptions.IsEmpty)
                throw new AggregateException(exceptions);
        }

        private async Task InsertBucketAsync(
            int workerIndex,
            List<T> records,
            SemaphoreSlim gate,
            System.Collections.Concurrent.ConcurrentBag<Exception> exceptions,
            CancellationToken cancellationToken)
        {
            await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var workerQueue = this.workerQueues[workerIndex];
                foreach (var record in records)
                {
                    Utils.RecordKey? primaryKey = this.primaryKeyBuilder?.build(record);
                    var flushed = workerQueue.insert(record, primaryKey);
                    if (flushed != null)
                        await FlushAsync(flushed, workerQueue.Url, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                throw; // let Task.WhenAll handle cancellation
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
            }
            finally
            {
                gate.Release();
            }
        }



    }  // end class KineticaIngestor<T>


    /// <summary>
    /// Result returned by <see cref="KineticaIngestor{T}.FlushAsync"/>.
    /// </summary>
    public readonly record struct IngestionResult(
        long Inserted,
        long Updated,
        int FlushedBatches);


}  // end namespace kinetica


