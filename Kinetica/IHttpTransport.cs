using System.Threading;
using System.Threading.Tasks;

namespace kinetica
{
    /// <summary>
    /// Abstraction over the raw HTTP POST layer.  Default implementation uses
    /// <see cref="HttpClientTransport"/>; tests inject a fake.
    /// </summary>
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
