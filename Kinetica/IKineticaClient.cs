using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace kinetica;
/// <summary>
/// Internal contract over the operations that <see cref="KineticaIngestor{T}"/> and
/// <see cref="RecordRetriever{T}"/> depend on from the Kinetica client.
/// <para>
/// Keeping this interface <c>internal</c> avoids leaking implementation details
/// through the public API surface.  Test assemblies (covered by
/// <c>InternalsVisibleTo</c> in Kinetica.csproj) can substitute this interface
/// with NSubstitute or any other mock framework, enabling isolated unit tests of
/// the ingestor's routing, batching, and error-handling logic without requiring a
/// live server.
/// </para>
/// </summary>
internal interface IKineticaClient
{
    /// <summary>The base server URI, used for single-head ingest/retrieval URL construction.</summary>
    Uri Uri { get; }

    /// <summary>Returns the current shard routing table from the server.</summary>
    AdminShowShardsResponse adminShowShards();

    /// <summary>Avro-encodes a single record into a byte array.</summary>
    byte[] AvroEncode( object obj );

    /// <summary>Inserts pre-encoded records via the primary endpoint (single-head path).</summary>
    InsertRecordsResponse insertRecordsRaw( RawInsertRecordsRequest request );

    /// <summary>
    /// Synchronous HTTP request targeting a full <paramref name="url"/>.
    /// Used for multi-head ingest and shard-key-routed retrieval.
    /// </summary>
    TResponse SubmitRequest<TResponse>(
        Uri url,
        object request,
        bool avroEncoding = true,
        CancellationToken cancellationToken = default ) where TResponse : new();

    /// <summary>Asynchronous counterpart for multi-head paths (full URL).</summary>
    Task<TResponse> SubmitRequestAsync<TResponse>(
        Uri url,
        object request,
        bool avroEncoding = true,
        CancellationToken cancellationToken = default ) where TResponse : new();

    /// <summary>
    /// Asynchronous HTTP request targeting a relative <paramref name="endpoint"/>.
    /// Used for single-head async ingest.
    /// </summary>
    Task<TResponse> SubmitRequestAsync<TResponse>(
        string endpoint,
        object request,
        bool avroEncoding = true,
        CancellationToken cancellationToken = default ) where TResponse : new();

    /// <summary>Retrieves records via the standard endpoint (single-head retrieval).</summary>
    GetRecordsResponse<T> getRecords<T>( GetRecordsRequest request ) where T : new();

    /// <summary>Decodes raw binary record data using an explicit KineticaType descriptor.</summary>
    IReadOnlyList<T> DecodeRawBinaryDataUsingRecordType<T>(
        KineticaType recordType,
        IList<byte[]> recordsBinary ) where T : new();
}
