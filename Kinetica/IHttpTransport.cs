using System;

namespace kinetica
{
    /// <summary>
    /// Abstraction over the raw HTTP POST layer.  Default implementation uses
    /// <see cref="System.Net.HttpWebRequest"/>; tests inject a fake.
    /// </summary>
    internal interface IHttpTransport
    {
        /// <summary>
        /// POST <paramref name="body"/> to <paramref name="url"/> and return
        /// the raw response bytes on HTTP 200 OK.
        /// </summary>
        /// <param name="url">Fully-qualified URL to POST to.</param>
        /// <param name="body">Request body bytes.</param>
        /// <param name="contentType">Value for the Content-Type header.</param>
        /// <param name="authorization">Optional Authorization header value
        /// (e.g. "Basic …" or "Bearer …").  Omitted when null.</param>
        /// <returns>Raw response body bytes.</returns>
        /// <exception cref="KineticaException">
        /// Thrown on network failure.  HTTP-level errors (non-200) propagate
        /// as <see cref="System.Net.WebException"/> so that
        /// <see cref="Kinetica"/> can decode the error body.
        /// </exception>
        byte[] Post(string url, byte[] body, string contentType, string? authorization);
    }
}
