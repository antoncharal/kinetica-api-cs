using Avro.IO;
using System;
using System.Collections.Generic;
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
    /// </summary>
    /// <typeparam name="T">The type of object being inserted.</typeparam>
    public class RecordRetriever<T> where T : new()
    {

        public Kinetica KineticaDb { get; }
        public string TableName { get; }

        [Obsolete("Use KineticaDb instead.")]
        public Kinetica kineticaDB => KineticaDb;

        [Obsolete("Use TableName instead.")]
        public string table_name => TableName;
        private KineticaType ktype;
        private Utils.RecordKeyBuilder<T> shard_key_builder;
        private IList<int> routing_table;
        private IList<Utils.WorkerQueue<T>> worker_queues;


        /// <summary>
        /// Create a RecordRetriever object with the given parameters.
        /// </summary>
        /// <param name="kdb"></param>
        /// <param name="table_name"></param>
        /// <param name="ktype"></param>
        /// <param name="workers"></param>
        public RecordRetriever( Kinetica kdb, string table_name,
                                KineticaType ktype,
                                Utils.WorkerList workers = null)
        {
            this.KineticaDb = kdb;
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
                    workers = new Utils.WorkerList(kdb);
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
                    this.routing_table = kdb.adminShowShards().rank;
                    // Check that enough worker URLs are specified
                    for (int i = 0; i < routing_table.Count; ++i)
                    {
                        if (this.routing_table[i] > this.worker_queues.Count)
                            throw new KineticaException("Not enough worker URLs specified.");
                    }
                }
                else // multihead-ingest is NOT turned on; use the regular Kinetica IP address
                {
                    string get_records_url_str = ( kdb.URL.ToString() + "get/records" );
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

            kineticaDB.DecodeRawBinaryDataUsingRecordType(
                ktype, raw_response.records_binary, decoded_response.data);

            return decoded_response;
        }


        /// <summary>
        /// Retrieves records for a given shard key, optionally further limited by an
        /// additional expression.
        /// </summary>
        public GetRecordsResponse<T> GetRecordsByKey( T record,
                                                      string expression = null )
        {
            if ( this.shard_key_builder == null)
                throw new KineticaException( "Cannot get by key from unsharded table: " + this.TableName );

            try
            {
                var (request, workerUrl) = BuildRequest(record, expression);

                if (workerUrl == null)
                    return kineticaDB.getRecords<T>(request);

                var raw_response = this.KineticaDb.SubmitRequest<RawGetRecordsResponse>(workerUrl, request);
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
        [Obsolete("Use GetRecordsByKey instead.")]
        public GetRecordsResponse<T> getRecordsByKey( T record,
                                                      string expression = null )
            => GetRecordsByKey(record, expression);


        /// <summary>
        /// Async overload of <see cref="getRecordsByKey"/>.  Cancellable, non-blocking.
        /// </summary>
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
