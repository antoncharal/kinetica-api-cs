using Avro.IO;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace kinetica;
/// <summary>
/// C# client API for the Kinetica database.
/// </summary>
/// <remarks>
/// <para>
/// This is the client-side C# application programming interface (API) for Kinetica.
/// The source code is hosted at
/// <a href="https://github.com/kineticadb/kinetica-api-cs">https://github.com/kineticadb/kinetica-api-cs</a>.
/// </para>
/// <para>
/// The solution contains two projects:
/// <list type="bullet">
///   <item><description>
///     <b>Kinetica</b> — the main client library in the <c>kinetica</c> namespace.
///     <see cref="Kinetica"/> implements the full API surface.  The
///     <c>Protocol/</c> subdirectory contains request/response classes for every
///     server endpoint.
///   </description></item>
///   <item><description>
///     <b>Example</b> — a short usage example.  Specify the hostname of a Kinetica
///     server (e.g. <c>http://127.0.0.1:9191</c>) before running it.
///   </description></item>
/// </list>
/// </para>
/// </remarks>
public sealed partial class Kinetica : IDisposable, IKineticaClient
{
    private bool _disposed;
    /// <summary>
    /// No Limit
    /// </summary>
    public const int END_OF_SET = -9999;

    /// <summary>
    /// Connection Options
    /// </summary>
    public class Options
    {
        /// <summary>
        /// Optional: User Name for Kinetica security
        /// </summary>
        public string Username { get; set; } = string.Empty;

        /// <summary>
        /// Optional: Password for user
        /// </summary>
        public string Password { get; set; } = string.Empty;

        /// <summary>
        /// Optional: OauthToken for user
        /// </summary>
        public string OauthToken { get; set; } = string.Empty;
        /// <summary>
        /// Use Snappy
        /// </summary>
        public bool UseSnappy { get; set; } = false;

        /// <summary>
        /// Thread Count. Must be ≥ 1.
        /// </summary>
        public int ThreadCount
        {
            get => _threadCount;
            set
            {
                ArgumentOutOfRangeException.ThrowIfNegativeOrZero(value);
                _threadCount = value;
            }
        }
        private int _threadCount = 1;

        /// <summary>
        /// Optional: HTTP request timeout in seconds. Must be &gt; 0 when specified.
        /// Defaults to 100 seconds (matching the previous <c>HttpWebRequest.Timeout</c> default).
        /// </summary>
        public int? TimeoutSeconds
        {
            get => _timeoutSeconds;
            set
            {
                if ( value.HasValue )
                    ArgumentOutOfRangeException.ThrowIfNegativeOrZero( value.Value, nameof( TimeoutSeconds ) );
                _timeoutSeconds = value;
            }
        }
        private int? _timeoutSeconds;

        /// <summary>
        /// When <c>true</c> (the default), every request is sent using HTTP/1.1.
        /// Set to <c>false</c> to allow the <see cref="HttpClient"/> to negotiate
        /// HTTP/2 or HTTP/3 with the server.
        /// </summary>
        /// <remarks>
        /// Kinetica servers typically do not support HTTP/2 and mis-negotiation would
        /// break the connection silently — this option exists for deployments that do.
        /// </remarks>
        public bool Version11HttpOnly { get; set; } = true;
    }

    /// <summary>
    /// API Version
    /// </summary>
    /// <returns>Version String for API</returns>
    public static string GetApiVersion() { return API_VERSION; }

    /// <summary>
    /// The server endpoint as a <see cref="System.Uri"/>.
    /// </summary>
    public Uri Uri { get; private set; }

    /// <summary>
    /// The server endpoint as a string.
    /// </summary>
    public string UrlString => Uri.ToString();

    /// <inheritdoc cref="Uri"/>
    [Obsolete("Use Uri instead.")]
    public Uri URL => Uri;

    /// <inheritdoc cref="UrlString"/>
    [Obsolete("Use UrlString instead.")]
    public string Url => UrlString;

    /// <summary>
    /// Optional: User Name for Kinetica security
    /// </summary>
    public string? Username { get; private set; } = null;

    /// <summary>
    /// Optional: Password for user
    /// </summary>
    private string? Password { get; set; } = null;

    /// <summary>
    /// Optional: OauthToken for user
    /// </summary>
    private string? OauthToken { get; set; } = null;

    /// <summary>
    /// Optional: Authorization for connections.
    /// </summary>
    private string? Authorization { get; set; } = null;

    /// <summary>
    /// Use Snappy
    /// </summary>
    public bool UseSnappy { get; set; } = false;

    /// <summary>
    /// Thread Count. Must be ≥ 1.
    /// </summary>
    public int ThreadCount
    {
        get => _threadCount;
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(value);
            _threadCount = value;
        }
    }
    private int _threadCount = 1;

    // HTTP transport — default uses HttpClientTransport; tests inject a fake.
    private readonly IHttpTransport _transport;

    /// <summary>
    /// Manages the Avro type-ID, label, and CLR-type lookups for known Kinetica types.
    /// Extracted from inline <see cref="ConcurrentDictionary{TKey, TValue}"/> fields to
    /// satisfy the Single Responsibility Principle (E1b).
    /// </summary>
    private readonly Internal.KineticaTypeRegistry _typeRegistry = new();

    /// <summary>
    /// API Constructor
    /// </summary>
    /// <param name="url">URL for Kinetica Server (including "http:" and port)</param>
    /// <param name="options">Optional connection options</param>
    public Kinetica( string url, Options? options = null )
        : this( url,
                new HttpClientTransport(
                    TimeSpan.FromSeconds( options?.TimeoutSeconds ?? 100 ),
                    options?.Version11HttpOnly ?? true ),
                options ) { }

    /// <summary>
    /// Test constructor — accepts an injected <see cref="IHttpTransport"/>
    /// so that unit tests can intercept HTTP calls without a live server.
    /// </summary>
    internal Kinetica( string url, IHttpTransport transport, Options? options = null )
    {
        Uri = new Uri( url );
        _transport = transport;
        if ( null != options )
        {
            Username = options.Username;
            Password = options.Password;
            OauthToken = options.OauthToken;
            Authorization = Internal.AuthorizationHeaderBuilder.Create( options.Username, options.Password, options.OauthToken );
            // Clear plaintext credentials immediately after the Authorization header
            // is built — they are not needed beyond this point.
            Password = null;
            OauthToken = null;
            UseSnappy = options.UseSnappy;
            ThreadCount = options.ThreadCount;
        }
    }

    /// <summary>
    /// Given a table name, add its record type to enable proper encoding of records
    /// for insertion or updates.
    /// </summary>
    /// <param name="tableName">Name of the table.</param>
    /// <param name="obj_type">The type associated with the table.</param>
    /// <exception cref="KineticaException">
    /// Thrown when the table does not exist, the server cannot return a type ID
    /// for the table, or the server returns an error during the type lookup.
    /// </exception>
    public void AddTableType( string tableName, Type obj_type )
    {
        try
        {
            // Get the type from the table
            KineticaType ktype = KineticaType.FromTable( this, tableName );
            if ( ktype.TypeId == null )
                throw new KineticaException( $"Could not get type ID for table '{tableName}'" );
            this._typeRegistry.TryAddByTypeId( ktype.TypeId, ktype );

            // Save a mapping of the object to the KineticaType
            if ( obj_type != null )
                this.SetKineticaSourceClassToTypeMapping( obj_type, ktype );

        } catch ( KineticaException ex )
        {
            throw new KineticaException( "Error creating type from table", ex );
        }
    }  // end AddTableType

    /// <summary>
    /// Async overload of <see cref="AddTableType"/>.
    /// </summary>
    /// <param name="tableName">The table to discover the type from.</param>
    /// <param name="obj_type">The CLR type to associate with the discovered Kinetica type.</param>
    /// <param name="cancellationToken">Token to cancel the async operation.</param>
    /// <exception cref="KineticaException">
    /// Thrown when the table does not exist, the server cannot return a type ID
    /// for the table, or the server returns an error during the type lookup.
    /// </exception>
    /// <exception cref="OperationCanceledException">
    /// Thrown when <paramref name="cancellationToken"/> is cancelled before
    /// the server responds.
    /// </exception>
    public async Task AddTableTypeAsync(
        string tableName,
        Type obj_type,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var ktype = await KineticaType.FromTableAsync(this, tableName, cancellationToken)
                .ConfigureAwait(false);
            if (ktype.TypeId == null)
                throw new KineticaException($"Could not get type ID for table '{tableName}'");
            this._typeRegistry.TryAddByTypeId(ktype.TypeId, ktype);
            if (obj_type != null)
                this.SetKineticaSourceClassToTypeMapping(obj_type, ktype);
        }
        catch (KineticaException ex)
        {
            throw new KineticaException("Error creating type from table", ex);
        }
    }  // end AddTableTypeAsync

    /// <summary>
    /// Saves an object class type to a KineticaType association.  If the class type already exists
    /// in the map, replaces the old KineticaType value.
    /// </summary>
    /// <param name="objectType">The type of the object.</param>
    /// <param name="kineticaType">The associated KinetiaType object.</param>
    public void SetKineticaSourceClassToTypeMapping( Type? objectType, KineticaType kineticaType )
    {
        if ( objectType is not null )
            this._typeRegistry.MapObjectTypeToKineticaType( objectType, kineticaType );
    }  // end SetKineticaSourceClassToTypeMapping



    /// <summary>
    /// Given a KineticaType object for a certain record type, decode binary data into distinct
    /// records (objects).
    /// </summary>
    /// <typeparam name="T">The type of the records.</typeparam>
    /// <param name="record_type">The type for the records.</param>
    /// <param name="records_binary">The binary encoded data to be decoded.</param>
    /// <returns>The decoded objects/records.</returns>
    public IReadOnlyList<T> DecodeRawBinaryDataUsingRecordType<T>( KineticaType record_type,
                                                                    IList<byte[]> records_binary ) where T : new()
    {
        var records = new List<T>( records_binary.Count );
        foreach ( var bin_record in records_binary )
            records.Add( AvroDecode<T>( bin_record, record_type ) );
        return records;
    }  // DecodeRawBinaryDataUsingRecordType

    /// <inheritdoc cref="DecodeRawBinaryDataUsingRecordType{T}(KineticaType, IList{byte[]})"/>
    /// <param name="records">Pre-allocated list to populate.</param>
    [Obsolete("Use the overload that returns IReadOnlyList<T> instead.")]
    public void DecodeRawBinaryDataUsingRecordType<T>( KineticaType record_type,
                                                       IList<byte[]> records_binary,
                                                       IList<T> records ) where T : new()
    {
        foreach ( var decoded in DecodeRawBinaryDataUsingRecordType<T>( record_type, records_binary ) )
            records.Add( decoded );
    }  // DecodeRawBinaryDataUsingRecordType (Obsolete)


    /// <summary>
    /// Given a schema string for a certain record type, decode binary data into distinct
    /// records (objects).
    /// </summary>
    /// <typeparam name="T">The type of the records.</typeparam>
    /// <param name="schema_string">The schema for the records.</param>
    /// <param name="records_binary">The binary encoded data to be decoded.</param>
    /// <returns>The decoded objects/records.</returns>
    public IReadOnlyList<T> DecodeRawBinaryDataUsingSchemaString<T>( string schema_string,
                                                                      IList<byte[]> records_binary ) where T : new()
    {
        KineticaType ktype = new( "", schema_string, null );
        var records = new List<T>( records_binary.Count );
        foreach ( var bin_record in records_binary )
            records.Add( AvroDecode<T>( bin_record, ktype ) );
        return records;
    }  // DecodeRawBinaryDataUsingSchemaString

    /// <inheritdoc cref="DecodeRawBinaryDataUsingSchemaString{T}(string, IList{byte[]})"/>
    /// <param name="records">Pre-allocated list to populate.</param>
    [Obsolete("Use the overload that returns IReadOnlyList<T> instead.")]
    public void DecodeRawBinaryDataUsingSchemaString<T>( string schema_string,
                                                         IList<byte[]> records_binary,
                                                         IList<T> records ) where T : new()
    {
        foreach ( var decoded in DecodeRawBinaryDataUsingSchemaString<T>( schema_string, records_binary ) )
            records.Add( decoded );
    }  // DecodeRawBinaryDataUsingSchemaString (Obsolete)

    /// <summary>
    /// Given a list of schema strings, decode binary data into distinct records (objects).
    /// </summary>
    /// <typeparam name="T">The type of the records.</typeparam>
    /// <param name="schema_strings">The schemas for the records.</param>
    /// <param name="lists_records_binary">The binary encoded data in a 2D list.</param>
    /// <returns>The decoded objects/records in a 2D list.</returns>
    public IReadOnlyList<IReadOnlyList<T>> DecodeRawBinaryDataUsingSchemaString<T>( IList<string> schema_strings,
                                                                                     IList<IList<byte[]>> lists_records_binary ) where T : new()
    {
        if ( schema_strings.Count != lists_records_binary.Count )
            throw new KineticaException( "List of schemas and list of binary encoded data do not match in count." );

        var record_lists = new List<IReadOnlyList<T>>( schema_strings.Count );
        for ( int i = 0; i < schema_strings.Count; ++i )
            record_lists.Add( DecodeRawBinaryDataUsingSchemaString<T>( schema_strings[i], lists_records_binary[i] ) );
        return record_lists;
    }  // DecodeRawBinaryDataUsingSchemaString (2D)

    /// <inheritdoc cref="DecodeRawBinaryDataUsingSchemaString{T}(IList{string}, IList{IList{byte[]}})"/>
    /// <param name="record_lists">Pre-allocated list of lists to populate.</param>
    [Obsolete("Use the overload that returns IReadOnlyList<IReadOnlyList<T>> instead.")]
    public void DecodeRawBinaryDataUsingSchemaString<T>( IList<string> schema_strings,
                                                         IList<IList<byte[]>> lists_records_binary,
                                                         IList<IList<T>> record_lists ) where T : new()
    {
        foreach ( var inner in DecodeRawBinaryDataUsingSchemaString<T>( schema_strings, lists_records_binary ) )
            record_lists.Add( new List<T>( inner ) );
    }  // DecodeRawBinaryDataUsingSchemaString 2D (Obsolete)


    /// <summary>
    /// Given IDs of record types registered with Kinetica, decode binary data into distinct
    /// records (objects).
    /// </summary>
    /// <typeparam name="T">The type of the records.</typeparam>
    /// <param name="type_ids">The IDs for each of the records' types.</param>
    /// <param name="records_binary">The binary encoded data to be decoded.</param>
    /// <returns>The decoded objects/records.</returns>
    public IReadOnlyList<T> DecodeRawBinaryDataUsingTypeIDs<T>( IList<string> type_ids,
                                                                 IList<byte[]> records_binary ) where T : new()
    {
        if ( type_ids.Count != records_binary.Count )
            throw new KineticaException( "Unequal numbers of type IDs and binary encoded data objects provided." );

        var records = new List<T>( records_binary.Count );
        for ( int i = 0; i < records_binary.Count; ++i )
        {
            KineticaType ktype = KineticaType.fromTypeID( this, type_ids[ i ] );
            records.Add( AvroDecode<T>( records_binary[ i ], ktype ) );
        }
        return records;
    }  // DecodeRawBinaryDataUsingTypeIDs

    /// <inheritdoc cref="DecodeRawBinaryDataUsingTypeIDs{T}(IList{string}, IList{byte[]})"/>
    /// <param name="records">Pre-allocated list to populate.</param>
    [Obsolete("Use the overload that returns IReadOnlyList<T> instead.")]
    public void DecodeRawBinaryDataUsingTypeIDs<T>( IList<string> type_ids,
                                                    IList<byte[]> records_binary,
                                                    IList<T> records ) where T : new()
    {
        foreach ( var decoded in DecodeRawBinaryDataUsingTypeIDs<T>( type_ids, records_binary ) )
            records.Add( decoded );
    }  // DecodeRawBinaryDataUsingTypeIDs (Obsolete)


    /// <summary>
    /// Given IDs of record types registered with Kinetica, decode binary data into distinct
    /// records (objects) in a 2D list.
    /// </summary>
    /// <typeparam name="T">The type of the records.</typeparam>
    /// <param name="type_ids">The IDs for each of the lists of records.</param>
    /// <param name="lists_records_binary">The binary encoded data in a 2D list.</param>
    /// <returns>The decoded objects/records in a 2D list.</returns>
    public IReadOnlyList<IReadOnlyList<T>> DecodeRawBinaryDataUsingTypeIDs<T>( IList<string> type_ids,
                                                                                IList<IList<byte[]>> lists_records_binary ) where T : new()
    {
        if ( type_ids.Count != lists_records_binary.Count )
            throw new KineticaException( "Unequal numbers of type IDs and binary encoded data objects provided." );

        var record_lists = new List<IReadOnlyList<T>>( type_ids.Count );
        for ( int i = 0; i < lists_records_binary.Count; ++i )
        {
            KineticaType ktype = KineticaType.fromTypeID( this, type_ids[ i ] );
            var records = new List<T>( lists_records_binary[i].Count );
            foreach ( var bin_record in lists_records_binary[i] )
                records.Add( AvroDecode<T>( bin_record, ktype ) );
            record_lists.Add( records );
        }
        return record_lists;
    }  // DecodeRawBinaryDataUsingTypeIDs (2D)

    /// <inheritdoc cref="DecodeRawBinaryDataUsingTypeIDs{T}(IList{string}, IList{IList{byte[]}})"/>
    /// <param name="record_lists">Pre-allocated list of lists to populate.</param>
    [Obsolete("Use the overload that returns IReadOnlyList<IReadOnlyList<T>> instead.")]
    public void DecodeRawBinaryDataUsingTypeIDs<T>( IList<string> type_ids,
                                                    IList<IList<byte[]>> lists_records_binary,
                                                    IList<IList<T>> record_lists ) where T : new()
    {
        foreach ( var inner in DecodeRawBinaryDataUsingTypeIDs<T>( type_ids, lists_records_binary ) )
            record_lists.Add( new List<T>( inner ) );
    }  // DecodeRawBinaryDataUsingTypeIDs 2D (Obsolete)


    /// <summary>
    /// Send request object to Kinetica server, and get response
    /// </summary>
    /// <typeparam name="TResponse">Kinetica Response Object Type</typeparam>
    /// <param name="url">The specific URL to send the request to</param>
    /// <param name="request">Kinetica Request Object</param>
    /// <param name="avroEncoding">Use Avro Encoding</param>
    /// <param name="cancellationToken">Optional cancellation token</param>
    /// <returns>Response Object</returns>
    internal TResponse SubmitRequest<TResponse>( Uri url, object request, bool avroEncoding = true, CancellationToken cancellationToken = default ) where TResponse : new()
    {
        var requestBytes = EncodeRequest( request, avroEncoding );
        var raw = SubmitRequestRaw( url.ToString(), requestBytes, avroEncoding, false, cancellationToken );
        return DecodeKineticaResponse<TResponse>( raw, avroEncoding );
    }  // end SubmitRequest( URL )


    /// <summary>
    /// Send request object to Kinetica server, and get response.
    /// Primary overload — no compression parameter.
    /// </summary>
    /// <typeparam name="TResponse">Kinetica Response Object Type</typeparam>
    /// <param name="endpoint">Kinetica Endpoint to call</param>
    /// <param name="request">Kinetica Request Object</param>
    /// <param name="avroEncoding">Use Avro Encoding</param>
    /// <param name="cancellationToken">Optional cancellation token</param>
    /// <returns>Response Object</returns>
    private TResponse SubmitRequest<TResponse>(string endpoint, object request, CancellationToken cancellationToken) where TResponse : new()
    {
        return SubmitRequest<TResponse>(endpoint, request, enableCompression: false, avroEncoding: true, cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Send request object to Kinetica server, and get response.
    /// The <paramref name="enableCompression"/> parameter is no longer used but is
    /// retained for backward compatibility with auto-generated <c>KineticaFunctions.cs</c>.
    /// It will be removed when the schema processor is updated (PR-06).
    /// </summary>
    private TResponse SubmitRequest<TResponse>(string endpoint, object request, bool enableCompression, bool avroEncoding = true, CancellationToken cancellationToken = default) where TResponse : new()
    {
        var requestBytes = EncodeRequest( request, avroEncoding );
        var raw = SubmitRequestRaw( endpoint, requestBytes, avroEncoding, cancellationToken: cancellationToken );
        return DecodeKineticaResponse<TResponse>( raw, avroEncoding );
    }  // end SubmitRequest( endpoint )



    /// <summary>
    /// Send encoded request to Kinetica server, and get Kinetica Response
    /// </summary>
    /// <param name="url">Kinetica Endpoint to call (with or without the full host path)</param>
    /// <param name="requestBytes">Binary data to send</param>
    /// <param name="avroEncoding">Use Avro encoding</param>
    /// <param name="only_endpoint_given">If true, prefix the given url
    /// with <member cref="Url" /></param>
    /// <param name="cancellationToken">Optional cancellation token</param>
    /// <returns>RawKineticaResponse Object</returns>
    private RawKineticaResponse? SubmitRequestRaw(string url, byte[] requestBytes, bool avroEncoding, bool only_endpoint_given = true, CancellationToken cancellationToken = default)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(Kinetica));

        if ( only_endpoint_given )
            url = (UrlString + url);

        var contentType = avroEncoding ? "application/octet-stream" : "application/json";

        try
        {
            // CS0618: IHttpTransport.Post is marked [Obsolete] — the suppression is
            // intentional; this is the one permitted callsite until PR-06 removes it.
#pragma warning disable CS0618
            var responseBytes = _transport.Post(
                url, requestBytes, contentType, Authorization, cancellationToken);
#pragma warning restore CS0618
            return DecodeResponse(responseBytes, avroEncoding);
        }
        catch (OperationCanceledException)
        {
            throw; // propagate unwrapped — matches .NET convention
        }
        catch (KineticaTransportException ex)
        {
            var serverMessage = TryDecodeErrorMessage(ex.Body, avroEncoding);
            var message = serverMessage is { Length: > 0 }
                ? serverMessage
                : $"Kinetica server returned HTTP {ex.StatusCode} for {url}.";
            throw new KineticaException(message, ex.StatusCode, ex);
        }
        catch (Exception ex) when (ex is not KineticaException)
        {
            throw new KineticaException(ex.ToString(), ex);
        }
    }

    private RawKineticaResponse? DecodeResponse(byte[] bytes, bool avroEncoding)
    {
        if (avroEncoding)
            return AvroDecode<RawKineticaResponse>(bytes);

        var s = Encoding.UTF8.GetString(bytes).Replace("\\U", "\\u");
        return JsonConvert.DeserializeObject<RawKineticaResponse>(s);
    }

    /// <summary>
    /// Attempts to decode the Kinetica error envelope from a transport error body.
    /// Returns <c>null</c> on any decode failure — a malformed error body must not
    /// mask the original transport error.
    /// </summary>
    private string? TryDecodeErrorMessage(byte[] body, bool avroEncoding)
    {
        if (body.Length == 0)
            return null;

        try
        {
            var envelope = DecodeResponse(body, avroEncoding);
            return envelope?.message;
        }
        catch (Exception)
        {
            return null;
        }
    }

    private byte[] EncodeRequest(object request, bool avroEncoding)
    {
        if (avroEncoding)
            return AvroEncode(request);
        return Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(request));
    }

    private TResponse DecodeKineticaResponse<TResponse>(RawKineticaResponse? raw, bool avroEncoding) where TResponse : new()
    {
        if (avroEncoding)
            return AvroDecode<TResponse>(raw!.data);
        var s = raw!.data_str.Replace("\\U", "\\u");
        return JsonConvert.DeserializeObject<TResponse>(s)!;
    }

    // -------------------------------------------------------------------------
    // Async core
    // -------------------------------------------------------------------------

    private async Task<RawKineticaResponse?> SubmitRequestRawAsync(
        string url,
        byte[] requestBytes,
        bool avroEncoding,
        bool only_endpoint_given = true,
        CancellationToken cancellationToken = default)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(Kinetica));

        if (only_endpoint_given)
            url = UrlString + url;

        var contentType = avroEncoding ? "application/octet-stream" : "application/json";

        try
        {
            var responseBytes = await _transport
                .PostAsync(url, requestBytes, contentType, Authorization, cancellationToken)
                .ConfigureAwait(false);

            // Check cancellation before beginning Avro/JSON decode.  For large
            // payloads the decode can hold the thread for noticeable time after
            // the caller has already cancelled — failing fast here avoids that
            // wasted work and returns control sooner.
            cancellationToken.ThrowIfCancellationRequested();

            return DecodeResponse(responseBytes, avroEncoding);
        }
        catch (OperationCanceledException)
        {
            throw; // propagate unwrapped — matches .NET convention
        }
        catch (KineticaTransportException ex)
        {
            var serverMessage = TryDecodeErrorMessage(ex.Body, avroEncoding);
            var message = serverMessage is { Length: > 0 }
                ? serverMessage
                : $"Kinetica server returned HTTP {ex.StatusCode} for {url}.";
            throw new KineticaException(message, ex.StatusCode, ex);
        }
        catch (Exception ex) when (ex is not KineticaException)
        {
            throw new KineticaException(ex.ToString(), ex);
        }
    }

    /// <summary>
    /// Async overload for direct-URL requests (used by multi-head ingest / retriever).
    /// </summary>
    internal async Task<TResponse> SubmitRequestAsync<TResponse>(
        Uri url,
        object request,
        bool avroEncoding = true,
        CancellationToken cancellationToken = default) where TResponse : new()
    {
        var requestBytes = EncodeRequest(request, avroEncoding);
        var raw = await SubmitRequestRawAsync(
            url.ToString(), requestBytes, avroEncoding, false, cancellationToken)
            .ConfigureAwait(false);
        return DecodeKineticaResponse<TResponse>(raw, avroEncoding);
    }

    /// <summary>
    /// Async overload for endpoint-based requests.
    /// Pattern for the schema generator to target in PR-06.
    /// </summary>
    internal async Task<TResponse> SubmitRequestAsync<TResponse>(
        string endpoint,
        object request,
        bool avroEncoding = true,
        CancellationToken cancellationToken = default) where TResponse : new()
    {
        var requestBytes = EncodeRequest(request, avroEncoding);
        var raw = await SubmitRequestRawAsync(
            endpoint, requestBytes, avroEncoding,
            cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        return DecodeKineticaResponse<TResponse>(raw, avroEncoding);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed) return;
        (_transport as IDisposable)?.Dispose();
        _disposed = true;
        GC.SuppressFinalize(this);
    }

    // -----------------------------------------------------------------------
    // IKineticaClient — explicit interface implementations
    //
    // Several of the methods required by the interface are `internal` on
    // Kinetica (e.g. SubmitRequest, AvroEncode) or have slightly different
    // parameter lists to the no-arg/short-form callers used by KineticaIngestor
    // and RecordRetriever.  Explicit implementations bridge that gap without
    // widening any method's public accessibility.
    // -----------------------------------------------------------------------

    /// <inheritdoc cref="IKineticaClient.adminShowShards"/>
    AdminShowShardsResponse IKineticaClient.adminShowShards() => adminShowShards();

    /// <inheritdoc cref="IKineticaClient.AvroEncode"/>
    byte[] IKineticaClient.AvroEncode( object obj ) => AvroEncode( obj );

    /// <inheritdoc cref="IKineticaClient.insertRecordsRaw"/>
    InsertRecordsResponse IKineticaClient.insertRecordsRaw( RawInsertRecordsRequest request ) => insertRecordsRaw( request );

    /// <inheritdoc cref="IKineticaClient.SubmitRequest{TResponse}(Uri,object,bool,CancellationToken)"/>
    TResponse IKineticaClient.SubmitRequest<TResponse>( Uri url, object request, bool avroEncoding, CancellationToken ct )
        => SubmitRequest<TResponse>( url, request, avroEncoding, ct );

    /// <inheritdoc cref="IKineticaClient.SubmitRequestAsync{TResponse}(Uri,object,bool,CancellationToken)"/>
    Task<TResponse> IKineticaClient.SubmitRequestAsync<TResponse>( Uri url, object request, bool avroEncoding, CancellationToken ct )
        => SubmitRequestAsync<TResponse>( url, request, avroEncoding, ct );

    /// <inheritdoc cref="IKineticaClient.SubmitRequestAsync{TResponse}(string,object,bool,CancellationToken)"/>
    Task<TResponse> IKineticaClient.SubmitRequestAsync<TResponse>( string endpoint, object request, bool avroEncoding, CancellationToken ct )
        => SubmitRequestAsync<TResponse>( endpoint, request, avroEncoding, ct );

    /// <inheritdoc cref="IKineticaClient.getRecords{T}"/>
    GetRecordsResponse<T> IKineticaClient.getRecords<T>( GetRecordsRequest request ) => getRecords<T>( request );

    /// <inheritdoc cref="IKineticaClient.DecodeRawBinaryDataUsingRecordType{T}"/>
    IReadOnlyList<T> IKineticaClient.DecodeRawBinaryDataUsingRecordType<T>( KineticaType recordType, IList<byte[]> recordsBinary )
        => DecodeRawBinaryDataUsingRecordType<T>( recordType, recordsBinary );

    private void SetDecoderIfMissing(string typeId, string label, string schemaString, IDictionary<string, IList<string>> properties)
    {
        // If the table is a collection, it does not have a proper type so ignore it

        if (typeId == "<collection>")
        {
            return;
        }

        _typeRegistry.RegisterIfAbsent( typeId, label, schemaString, properties );
    }


    /// <summary>
    /// Retrieve a KineticaType object by the type label.
    /// </summary>
    /// <param name="typeName">The label/name of the type.</param>
    /// <returns></returns>
    private KineticaType? GetType(string typeName) => _typeRegistry.FindByLabel( typeName );


    /// <summary>
    /// Given a class type, look up the associated KineticaType.  If none is found, return null.
    /// </summary>
    /// <param name="objectType">The type of the object whose associated KineticaType we need.</param>
    /// <returns></returns>
    private KineticaType? LookupKineticaType( Type objectType ) => _typeRegistry.FindByObjectType( objectType );


    /// <summary>
    /// Encode specified object using Avro
    /// </summary>
    /// <param name="obj">Object to encode</param>
    /// <returns>Byte array of binary Avro-encoded data</returns>
    internal byte[] AvroEncode(object obj)
    {
        // Create a stream that will allow us to view the underlying memory
        using ( var ms = new MemoryStream())
        {
            // Write the object to the memory stream
            // If obj is an ISpecificRecord, this is more efficient
            if ( obj is Avro.Specific.ISpecificRecord)
            {
                var schema = (obj as Avro.Specific.ISpecificRecord).Schema;
                Avro.Specific.SpecificDefaultWriter writer = new(schema);
                writer.Write(schema, obj, new BinaryEncoder(ms));
            }
            else // Not an ISpecificRecord - this way is less efficient
            {
                // Get the KineticaType associated with the object to be encoded
                Type obj_type = obj.GetType();
                KineticaType? ktype = LookupKineticaType( obj_type );
                if ( ktype == null )
                {
                    throw new KineticaException( "No known KineticaType associated with the given object.  " +
                                                 "Need a known KineticaType to encode the object." );
                }

                // Make a copy of the object to send as a GenericRecord, then write that to the memory stream
                var schema = KineticaData.SchemaFromType( obj.GetType(), ktype );
                var recordToSend = MakeGenericRecord( obj, ktype );
                var writer = new Avro.Generic.DefaultWriter(schema);
                writer.Write(schema, recordToSend, new BinaryEncoder(ms));
            }

            // Get the memory from the stream
            return ms.ToArray();
        }
    }  // end AvroEncode

    /// <summary>
    /// Make a copy of an object as an Avro GenericRecord
    /// </summary>
    /// <param name="obj">Original object</param>
    /// <param name="ktype">An associated KineticaType object that
    /// describes the original object.</param>
    /// <returns>GenericRecord object which is a copy of the specified object</returns>
    private Avro.Generic.GenericRecord MakeGenericRecord( object obj, KineticaType ktype )
    {
        // Get the schema
        var schema = KineticaData.SchemaFromType( obj.GetType(), ktype );

        // Create a new GenericRecord for this schema
        var recordToSend = new Avro.Generic.GenericRecord(schema);

        // Copy each field from obj to recordToSend
        foreach ( var field in schema.Fields)
        {
            var property = obj.GetType()
                            .GetProperties()
                            .FirstOrDefault(prop => prop.Name.ToLowerInvariant() == field.Name.ToLowerInvariant());

            if (property == null) continue;

            recordToSend.Add(field.Name, property.GetValue(obj, null));
        }

        // Return the newly created object
        return recordToSend;
    }

    /// <summary>
    /// Decode binary Avro data into an object.
    /// </summary>
    /// <typeparam name="T">Type of expected object</typeparam>
    /// <param name="bytes">Binary Avro data</param>
    /// <param name="ktype">An optional KineticaType object to help in decoding the object.</param>
    /// <returns>New object</returns>
    private T AvroDecode<T>(byte[] bytes, KineticaType? ktype = null) where T : new()
    {
        // Get the schema
        var schema = KineticaData.SchemaFromType( typeof(T), ktype );

        // Create a stream to read the binary data
        using (var ms = new MemoryStream(bytes))
        {
            // Create a new object to return
            T obj = new();
            if (obj is Avro.Specific.ISpecificRecord)
            {
                var reader = new Avro.Specific.SpecificDefaultReader(schema, schema);
                reader.Read(obj, new BinaryDecoder(ms));
            }
            else
            {
                // Not ISpecificRecord, so first read into a new GenericRecord
                var reader = new Avro.Generic.DefaultReader(schema, schema);
                Avro.Generic.GenericRecord recordToReceive = new(schema);
                reader.Read(recordToReceive, new BinaryDecoder(ms));

                // Now, copy all the fields from the GenericRecord to obj
                foreach (var field in schema.Fields)
                {
                    var property = obj.GetType()
                                    .GetProperties()
                                    .FirstOrDefault(prop => prop.Name.ToLowerInvariant() == field.Name.ToLowerInvariant());

                    if (property == null) continue;

                    // Try to get the property
                    if (recordToReceive.TryGetValue(field.Name, out object val))
                    {
                        // If successful, write the property to obj
                        property.SetValue(obj, val);
                    }
                }  // end foreach
            }  // end if-else

            // Return the new object
            return obj;
        }  // end using
    }  // end AvroDecode<T>


    /// <summary>
    /// Decode binary Avro data from a stream into an object
    /// </summary>
    /// <typeparam name="T">Type of expected object</typeparam>
    /// <param name="stream">Stream to read for object data</param>
    /// <returns>New object</returns>
    private T AvroDecode<T>(Stream stream) where T : Avro.Specific.ISpecificRecord, new()
    {
        // T obj = new T(); // Activator.CreateInstance<T>();
        var schema = KineticaData.SchemaFromType( typeof(T), null );
        var reader = new Avro.Specific.SpecificReader<T>(schema, schema);
        return reader.Read(default, new BinaryDecoder(stream));
    }
    /*
    private T AvroDecode<T>(string str) where T : new()
    {
        return AvroDecode<T>(Encoding.UTF8.GetBytes(str));
    }
    */
}  // end class Kinetica
