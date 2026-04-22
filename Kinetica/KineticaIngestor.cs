using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;


namespace kinetica
{
    /// <summary>
    /// Manages the insertion into GPUdb of large numbers of records in bulk,
    /// with automatic batch management and support for multi-head ingest.
    /// Use the <see cref="insert(record)"/> and <see cref="insert(List)"/>
    /// methods to queue records for insertion, and the <see cref="flush"/>
    /// method to ensure that all queued records have been inserted.
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
    /// <typeparam name="T">The type of object being inserted.</typeparam>
    public class KineticaIngestor<T>
    {
        [Serializable]
        public class InsertException<T> : KineticaException
        {
            public Uri Url { get; private set; }
            public IReadOnlyList<T> Records { get; private set; }

            [Obsolete("Use Url instead.")]
            public Uri url => Url;

            [Obsolete("Use Records instead.")]
            public IList<T> records => (IList<T>)Records;

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
        public Kinetica KineticaDb { get; }
        public string TableName { get; }
        public int BatchSize { get; }
        public IDictionary<string, string> Options { get; }

        [Obsolete("Use KineticaDb instead.")]
        public Kinetica kineticaDB => KineticaDb;

        [Obsolete("Use TableName instead.")]
        public string table_name => TableName;

        [Obsolete("Use BatchSize instead.")]
        public int batch_size => BatchSize;

        [Obsolete("Use Options instead.")]
        public IDictionary<string, string> options => Options;
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
        /// 
        /// </summary>
        /// <param name="kdb"></param>
        /// <param name="tableName"></param>
        /// <param name="batchSize"></param>
        /// <param name="ktype"></param>
        /// <param name="options"></param>
        /// <param name="workers"></param>
        public KineticaIngestor( Kinetica kdb, string tableName,
                                 int batchSize, KineticaType ktype,
                                 Dictionary<string, string>? options = null,
                                 Utils.WorkerList? workers = null )
        {
            this.KineticaDb = kdb;
            this.TableName = tableName;
            this.ktype = ktype;

            // Validate and save the batch size
            if ( batchSize < 1 )
                throw new KineticaException( $"Batch size must be greater than one; given {batchSize}." );
            this.BatchSize = batchSize;

            this.Options = options;

            // Set up the primary and shard key builders
            // -----------------------------------------
            this.primaryKeyBuilder = new Utils.RecordKeyBuilder<T>( true,  this.ktype );
            this.shardKeyBuilder   = new Utils.RecordKeyBuilder<T>( false, this.ktype );

            // Based on the Java implementation
            if ( this.primaryKeyBuilder.hasKey() )
            {   // There is a primary key for the given T
                // Now check if there is a distinct shard key
                if ( !this.shardKeyBuilder.hasKey()
                     || this.shardKeyBuilder.hasSameKey( this.primaryKeyBuilder ) )
                    this.shardKeyBuilder = this.primaryKeyBuilder; // no distinct shard key
            }
            else  // there is no primary key for the given T
            {
                this.primaryKeyBuilder = null;

                // Check if there is shard key for T
                if ( !this.shardKeyBuilder.hasKey() )
                    this.shardKeyBuilder = null;
            }  // done setting up the key builders


            // Set up the worker queues
            // -------------------------
            this.workerQueues = [];
            try
            {
                // If no workers are given, try to get them from Kinetica
                if ( ( workers == null ) || ( workers.Count == 0 ) )
                {
                    workers = new Utils.WorkerList( kdb );
                }

                // If we end up with multiple workers, either given by the
                // user or obtained from Kinetica, then use those
                if ( ( workers != null ) && ( workers.Count > 0 ) )
                {
                    // Add worker queues per worker
                    foreach ( System.Uri workerUrl in workers )
                    {
                        string strWorkerUrl = workerUrl.ToString();
                        strWorkerUrl = strWorkerUrl.EndsWith('/') ? strWorkerUrl[..^1] : strWorkerUrl;
                        string insert_records_worker_url_str = $"{strWorkerUrl}/insert/records";
                        System.Uri url = new( insert_records_worker_url_str );
                        Utils.WorkerQueue<T> worker_queue = new( url, batchSize );
                        this.workerQueues.Add( worker_queue );
                    }

                    // Get the worker rank information from Kinetica
                    this.routingTable = kdb.adminShowShards().rank;
                    // Check that enough worker URLs are specified
                    for ( int i = 0; i < routingTable.Count; ++i )
                    {
                        if ( this.routingTable[i] > this.workerQueues.Count )
                            throw new KineticaException( "Not enough worker URLs specified." );
                    }
                }
                else // multihead-ingest is NOT turned on; use the regular Kinetica IP address
                {
                    string strWorkerUrl = kdb.URL.ToString();
                    strWorkerUrl = strWorkerUrl.EndsWith('/') ? strWorkerUrl[..^1] : strWorkerUrl;
                    string insertRecordsUrlStr = $"{strWorkerUrl}/insert/records";
                    System.Uri url = new( insertRecordsUrlStr );
                    Utils.WorkerQueue<T> worker_queue = new( url, batchSize );
                    this.workerQueues.Add( worker_queue );
                    this.routingTable = null;
                }
            }
            catch ( Exception ex )
            {
                throw new KineticaException( ex.ToString() );
            }

        }   // end constructor KineticaIngestor


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
        private Utils.WorkerQueue<T> RouteFor(T record, out Utils.RecordKey? primaryKey)
        {
            primaryKey = null;
            Utils.RecordKey? shardKey = null;

            if (this.primaryKeyBuilder != null)
                primaryKey = this.primaryKeyBuilder.build(record);
            if (this.shardKeyBuilder != null)
                shardKey = this.shardKeyBuilder.build(record);

            if (this.routingTable == null)
                return this.workerQueues[0];
            if (shardKey == null)
                return this.workerQueues[Random.Shared.Next(this.workerQueues.Count)];

            return this.workerQueues[shardKey.route(this.routingTable)];
        }


        /// <summary>
        /// Ensures that all queued records are inserted into Kinetica.
        /// </summary>
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
                foreach ( var record in queue ) encodedQueue.Add( this.KineticaDb.AvroEncode( record ) );
                RawInsertRecordsRequest request = new( this.TableName, encodedQueue, this.Options);

                InsertRecordsResponse response = new();

                if ( url == null )
                {
                    response = this.KineticaDb.insertRecordsRaw( request );
                }
                else
                {
                    response = this.KineticaDb.SubmitRequest<InsertRecordsResponse>( url, request );
                }

                // Save the counts of inserted and updated records
                System.Threading.Interlocked.Add( ref _countInserted, response.count_inserted );
                System.Threading.Interlocked.Add( ref _countUpdated, response.count_updated );
            }
            catch ( Exception ex )
            {
                throw new InsertException<T>( url, queue, ex.Message );
            }
        }  // end private flush()



        /// <summary>
        /// Queues a record for insertion into Kinetica.
        /// </summary>
        public void Insert( T record )
        {
            var workerQueue = RouteFor(record, out var primaryKey);
            IList<T> queue = workerQueue.insert( record, primaryKey );
            if ( queue != null )
                this.flush( queue, workerQueue.Url );
        }

        /// <summary>Queues a record for insertion.</summary>
        [Obsolete("Use Insert() instead.")]
        public void insert( T record )
        {
            var workerQueue = RouteFor(record, out var primaryKey);

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
                catch ( InsertException<T> ex )
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
                catch ( InsertException<T> ex )
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
        /// Worker queues are drained in parallel with bounded concurrency,
        /// matching the fan-out behaviour of <see cref="InsertAsync(IList{T}, CancellationToken)"/>.
        /// </summary>
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
                    encodedQueue.Add(this.KineticaDb.AvroEncode(record));

                var request = new RawInsertRecordsRequest(this.TableName, encodedQueue, this.Options);

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
                throw new InsertException<T>(url, queue, ex.Message);
            }
        }

        /// <summary>
        /// Queues <paramref name="record"/> for insertion.  If the queue reaches
        /// the batch size, flushes asynchronously before returning.
        /// </summary>
        public async Task InsertAsync(T record, CancellationToken cancellationToken = default)
        {
            var workerQueue = RouteFor(record, out var primaryKey);

            var queue = workerQueue.insert(record, primaryKey);
            if (queue != null)
                await FlushAsync(queue, workerQueue.Url, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Queues each record for insertion asynchronously with parallel fan-out
        /// across worker queues. Records are bucketed by their shard-routed worker,
        /// then each bucket is inserted concurrently with bounded parallelism.
        /// On partial failure, throws <see cref="AggregateException"/> containing
        /// one <see cref="InsertException{T}"/> per failed bucket.
        /// </summary>
        public async Task InsertAsync(IList<T> records, CancellationToken cancellationToken = default)
        {
            if (records.Count == 0)
                return;

            // Bucket records per worker queue.
            var buckets = new Dictionary<int, List<T>>();
            foreach (var record in records)
            {
                var workerQueue = RouteFor(record, out _);
                int index = this.workerQueues.IndexOf(workerQueue);
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
