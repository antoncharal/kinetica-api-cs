using Avro.IO;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;


namespace kinetica
{
    /// <summary>
    /// Retrieves records from a Kinetica table by shard key, with optional
    /// server-side expression filtering.  Supports both synchronous
    /// (<see cref="GetRecordsByKey"/>) and asynchronous
    /// (<see cref="GetRecordsByKeyAsync"/>) retrieval with full cancellation
    /// support.  Multi-head retrieval is used automatically when the server
    /// has multiple worker nodes and the table is sharded.
    /// </summary>
    /// <typeparam name="T">The CLR type of the records being retrieved. Must
    /// match the Kinetica table schema.</typeparam>
    public sealed class RecordRetriever<T> where T : new()
    {
        private readonly IKineticaClient _kdb;

        /// <summary>The underlying Kinetica client. Internal — do not call server methods directly.</summary>
        internal Kinetica KineticaDb => (Kinetica)_kdb;

        /// <summary>The fully-qualified name of the Kinetica table this retriever is bound to.</summary>
        public string TableName { get; }

        /// <inheritdoc cref="KineticaDb"/>
        [Obsolete("Use KineticaDb instead.")]
        public Kinetica kineticaDB => (Kinetica)_kdb;

        /// <inheritdoc cref="TableName"/>
        [Obsolete("Use TableName instead.")]
        public string table_name => TableName;
        private KineticaType ktype;
        private Utils.RecordKeyBuilder<T> shard_key_builder;
        private IList<int> routing_table;
        private IList<Utils.WorkerQueue<T>> worker_queues;


        /// <summary>Creates a <see cref="RecordRetriever{T}"/> for the given table.</summary>
        /// <param name="kdb">The <see cref="Kinetica"/> client used to communicate with the server.</param>
        /// <param name="table_name">The fully-qualified name of the Kinetica table to retrieve from.</param>
        /// <param name="ktype">The <see cref="KineticaType"/> describing the schema of <typeparamref name="T"/>.</param>
        /// <param name="workers">Optional pre-built worker list for multi-head retrieval.  When
        /// <c>null</c> or empty the retriever queries the server for worker URLs automatically.</param>
        /// <exception cref="KineticaException">
        /// Thrown when the server returns insufficient worker URLs for the table's shard routing table.
        /// </exception>
        public RecordRetriever( Kinetica kdb, string table_name,
                                KineticaType ktype,
                                Utils.WorkerList workers = null )
            : this( (IKineticaClient)kdb, table_name, ktype, workers ) { }

        /// <summary>
        /// Internal constructor for test injection via <c>InternalsVisibleTo</c>.
        /// </summary>
        internal RecordRetriever( IKineticaClient kdb, string table_name,
                                  KineticaType ktype,
                                  Utils.WorkerList workers = null )
        {
            this._kdb = kdb;
            this.TableName = table_name;
            this.ktype = ktype;

            // Set up the shard key builder
            // ----------------------------
            this.shard_key_builder = new Utils.RecordKeyBuilder<T>(false, this.ktype);
            // Check if there is shard key for T
            if (!this.shard_key_builder.hasKey())
                this.shard_key_builder = null;


            // Set up the worker queues
            // -------------------------
            this.worker_queues = new List<Utils.WorkerQueue<T>>();
            try
            {
                // If no workers are given, try to get them from Kinetica
                if ((workers == null) || (workers.Count == 0))
                {
                    if (kdb is Kinetica concreteKdb)
                        workers = new Utils.WorkerList(concreteKdb);
                    // If kdb is a test double, leave workers empty — single-head mode.
                }

                // If we end up with multiple workers, either given by the
                // user or obtained from Kinetica, then use those
                if ((workers != null) && (workers.Count > 0))
                {
                    // Add worker queues per worker
                    foreach (System.Uri worker_url in workers)
                    {
                        string get_records_worker_url_str = (worker_url.ToString() + "get/records");
                        System.Uri url = new System.Uri( get_records_worker_url_str );
                        Utils.WorkerQueue<T> worker_queue = new Utils.WorkerQueue<T>( url );
                        this.worker_queues.Add( worker_queue );
                    }

                    // Get the worker rank information from Kinetica
                    this.routing_table = _kdb.adminShowShards().rank;
                    // Check that enough worker URLs are specified
                    for (int i = 0; i < routing_table.Count; ++i)
                    {
                        if (this.routing_table[i] > this.worker_queues.Count)
                            throw new KineticaException("Not enough worker URLs specified.");
                    }
                }
                else // multihead-ingest is NOT turned on; use the regular Kinetica IP address
                {
                    string get_records_url_str = ( _kdb.Uri.ToString() + "get/records" );
                    System.Uri url = new System.Uri( get_records_url_str );
                    Utils.WorkerQueue<T> worker_queue = new Utils.WorkerQueue<T>( url );
                    this.worker_queues.Add(worker_queue);
                    this.routing_table = null;
                }
            }
            catch (Exception ex)
            {
                throw new KineticaException(ex.ToString());
            }

        }   // end constructor RecordRetriever


        /// <summary>
        /// Builds the /get/records request and resolves the target URL for a given record
        /// and optional expression. Shared by sync and async retrieval paths.
        /// </summary>
        private (GetRecordsRequest request, Uri? workerUrl) BuildRequest(
            T record, string expression = null)
        {
            string full_expression = this.shard_key_builder.buildExpression(record);
            if (full_expression == null)
                throw new KineticaException("No expression could be made from given record.");
            if (expression != null)
                full_expression = full_expression + " and (" + expression + ")";

            IDictionary<string, string> options = new Dictionary<string, string>();
            options[GetRecordsRequest.Options.EXPRESSION]        = full_expression;
            options[GetRecordsRequest.Options.FAST_INDEX_LOOKUP] = GetRecordsRequest.Options.TRUE;

            var request = new GetRecordsRequest(this.table_name, 0, Kinetica.END_OF_SET, options);

            Uri? workerUrl = null;
            if (this.routing_table != null)
            {
                Utils.RecordKey shard_key = this.shard_key_builder.build(record);
                workerUrl = this.worker_queues[shard_key.route(this.routing_table)].Url;
            }

            return (request, workerUrl);
        }

        private GetRecordsResponse<T> DecodeRawResponse(RawGetRecordsResponse raw_response)
        {
            var decoded_response = new GetRecordsResponse<T>();
            decoded_response.table_name              = raw_response.table_name;
            decoded_response.type_name               = raw_response.type_name;
            decoded_response.type_schema             = raw_response.type_schema;
            decoded_response.has_more_records        = raw_response.has_more_records;
            decoded_response.total_number_of_records = raw_response.total_number_of_records;

            decoded_response.data = new List<T>(
                _kdb.DecodeRawBinaryDataUsingRecordType<T>( ktype, raw_response.records_binary ) );

            return decoded_response;
        }


        /// <summary>
        /// Retrieves records for a given shard key, optionally further limited by an
        /// additional expression.
        /// </summary>
        /// <param name="record">A record whose shard-key fields are used to locate
        /// the target worker and filter the query.</param>
        /// <param name="expression">An optional server-side filter expression applied
        /// in addition to the shard-key predicate.  Pass <c>null</c> for no extra filter.</param>
        /// <returns>A <see cref="GetRecordsResponse{T}"/> containing the matching records.</returns>
        /// <exception cref="KineticaException">
        /// Thrown when the table has no shard key, when the server returns an error,
        /// or when the worker URL is unreachable.
        /// </exception>
        public GetRecordsResponse<T> GetRecordsByKey( T record,
                                                      string expression = null )
        {
            if ( this.shard_key_builder == null)
                throw new KineticaException( "Cannot get by key from unsharded table: " + this.TableName );

            try
            {
                var (request, workerUrl) = BuildRequest(record, expression);

                if (workerUrl == null)
                    return _kdb.getRecords<T>(request);

                var raw_response = _kdb.SubmitRequest<RawGetRecordsResponse>(workerUrl, request);
                return DecodeRawResponse(raw_response);
            } catch ( KineticaException ex )
            {
                throw new KineticaException( "Error in retrieving records by key: ", ex );
            } catch ( Exception ex )
            {
                throw new KineticaException( "Error in retrieving records by key: ", ex );
            }
        }  // end GetRecordsByKey()

        /// <summary>Retrieves records for a given shard key.</summary>
        /// <inheritdoc cref="GetRecordsByKey"/>
        [Obsolete("Use GetRecordsByKey instead.")]
        public GetRecordsResponse<T> getRecordsByKey( T record,
                                                      string expression = null )
            => GetRecordsByKey(record, expression);


        /// <summary>
        /// Async overload of <see cref="GetRecordsByKey"/>.  Cancellable, non-blocking.
        /// </summary>
        /// <param name="record">A record whose shard-key fields are used to locate
        /// the target worker and filter the query.</param>
        /// <param name="expression">An optional server-side filter expression.
        /// Pass <c>null</c> for no extra filter.</param>
        /// <param name="cancellationToken">Token to cancel the operation.</param>
        /// <returns>A <see cref="GetRecordsResponse{T}"/> containing the matching records.</returns>
        /// <exception cref="KineticaException">
        /// Thrown when the table has no shard key, the server returns an error,
        /// or the worker URL is unreachable.
        /// </exception>
        /// <exception cref="OperationCanceledException">
        /// Thrown when <paramref name="cancellationToken"/> is cancelled before
        /// the server responds.
        /// </exception>
        public async Task<GetRecordsResponse<T>> GetRecordsByKeyAsync(
            T record,
            string expression = null,
            CancellationToken cancellationToken = default)
        {
            if (this.shard_key_builder == null)
                throw new KineticaException("Cannot get by key from unsharded table: " + this.table_name);

            try
            {
                var (request, workerUrl) = BuildRequest(record, expression);

                if (workerUrl == null)
                {
                    return await kineticaDB
                        .SubmitRequestAsync<GetRecordsResponse<T>>("/get/records", request,
                            cancellationToken: cancellationToken)
                        .ConfigureAwait(false);
                }

                var raw_response = await kineticaDB
                    .SubmitRequestAsync<RawGetRecordsResponse>(workerUrl, request,
                        cancellationToken: cancellationToken)
                    .ConfigureAwait(false);
                return DecodeRawResponse(raw_response);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (KineticaException ex)
            {
                throw new KineticaException("Error in retrieving records by key: ", ex);
            }
            catch (Exception ex)
            {
                throw new KineticaException("Error in retrieving records by key: ", ex);
            }
        }  // end GetRecordsByKeyAsync

    }   // end class RecordRetriever

}   // end namespace kinetica

