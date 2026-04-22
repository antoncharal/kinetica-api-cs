using System.Threading;

namespace kinetica
{
    /// <summary>
    /// Abstraction over the raw HTTP POST layer.  Default implementation uses
    /// <see cref="HttpClientTransport"/>; tests inject a fake.
    /// </summary>
    internal interface IHttpTransport
    {
        /// <summary>
        /// POST <paramref name="body"/> to <paramref name="url"/> and return
        /// the raw response bytes on success.
        /// </summary>
        /// <param name="url">Fully-qualified URL to POST to.</param>
        /// <param name="body">Request body bytes.</param>
        /// <param name="contentType">Value for the Content-Type header.</param>
        /// <param name="authorization">Optional Authorization header value
        /// (e.g. "Basic …" or "Bearer …").  Omitted when null.</param>
        /// <param name="cancellationToken">Token to cancel the request.</param>
        /// <returns>Raw response body bytes.</returns>
        /// <exception cref="KineticaTransportException">
        /// Thrown when the server returns a non-2xx status code.  Carries the
        /// raw response body so <see cref="Kinetica"/> can decode the error envelope.
        /// </exception>
        /// <exception cref="KineticaException">
        /// Thrown on network-level failure.
        /// </exception>
        byte[] Post(string url, byte[] body, string contentType, string? authorization, CancellationToken cancellationToken);
    }
}
