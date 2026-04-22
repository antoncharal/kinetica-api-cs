using System.Threading;
using System.Threading.Tasks;

namespace kinetica
{
    /// <summary>
    /// Abstraction over the raw HTTP POST layer.  Default implementation uses
    /// <see cref="HttpClientTransport"/>; tests inject a fake.
    /// </summary>
    /// <remarks>
    /// <b>Design note (E9 — ISP):</b>
    /// This interface combines the synchronous (<see cref="Post"/>) and asynchronous
    /// (<see cref="PostAsync"/>) concerns.  Currently both are required together because
    /// <see cref="Kinetica"/> delegates to both paths from the same transport field.
    /// Once the synchronous path is deprecated (see Section F — async improvements),
    /// this interface should be split into two: <c>IHttpTransport</c> (async only) and,
    /// if still needed, <c>ISyncHttpTransport</c>.  The split should be made at that
    /// point to honour the Interface Segregation Principle.
    /// </remarks>
    internal interface IHttpTransport
    {
        /// <summary>
        /// Synchronous POST.  Blocks the calling thread until the response is received.
        /// </summary>
        byte[] Post(string url, byte[] body, string contentType, string? authorization, CancellationToken cancellationToken);

        /// <summary>
        /// Asynchronous POST.  Returns a task that completes when the response is received.
        /// <para>Cancellation is honoured at the socket level — the returned task throws
        /// <see cref="OperationCanceledException"/> when the token fires.</para>
        /// </summary>
        Task<byte[]> PostAsync(string url, byte[] body, string contentType, string? authorization, CancellationToken cancellationToken);
    }
}
