using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;

namespace kinetica
{
    /// <summary>
    /// <see cref="IHttpTransport"/> implementation backed by <see cref="HttpClient"/>
    /// with <see cref="SocketsHttpHandler"/> for connection pooling and DNS refresh.
    /// Replaces the legacy <see cref="HttpWebRequestTransport"/> as of PR-03.
    /// </summary>
    internal sealed class HttpClientTransport : IHttpTransport, IDisposable
    {
        private readonly HttpClient _client;
        private readonly bool _ownsClient;

        public HttpClientTransport(TimeSpan timeout)
        {
            var handler = new SocketsHttpHandler
            {
                PooledConnectionLifetime    = TimeSpan.FromMinutes(2),
                PooledConnectionIdleTimeout = TimeSpan.FromMinutes(2),
                AutomaticDecompression      = DecompressionMethods.None,
                UseCookies                  = false,
                AllowAutoRedirect           = false,
            };
            _client = new HttpClient(handler, disposeHandler: true)
            {
                Timeout = timeout,
            };
            _ownsClient = true;
        }

        /// <summary>
        /// Injection constructor for tests.  The caller owns the <paramref name="client"/>
        /// lifetime; this instance will not dispose it.
        /// </summary>
        internal HttpClientTransport(HttpClient client)
        {
            _client = client;
            _ownsClient = false;
        }

        public byte[] Post(
            string url,
            byte[] body,
            string contentType,
            string? authorization,
            CancellationToken cancellationToken)
        {
            using var request = BuildRequest(url, body, contentType, authorization);
            using var response = _client.Send(request, cancellationToken);
            return ReadOrThrow(response, cancellationToken);
        }

        public async Task<byte[]> PostAsync(
            string url,
            byte[] body,
            string contentType,
            string? authorization,
            CancellationToken cancellationToken)
        {
            using var request = BuildRequest(url, body, contentType, authorization);
            // Intent: ResponseHeadersRead lets us inspect status/headers before reading
            // the body; the body itself is always fully buffered because Kinetica response
            // envelopes are Avro/JSON blobs that must be parsed as a whole — not streams.
            // Do not "optimize" this to stream-decode without changing the envelope contract.
            using var response = await _client
                .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
                .ConfigureAwait(false);

            var bytes = await response.Content
                .ReadAsByteArrayAsync(cancellationToken)
                .ConfigureAwait(false);

            if (response.IsSuccessStatusCode)
                return bytes;

            throw new KineticaTransportException((int)response.StatusCode, bytes);
        }

        private static HttpRequestMessage BuildRequest(
            string url,
            byte[] body,
            string contentType,
            string? authorization)
        {
            var request = new HttpRequestMessage(HttpMethod.Post, url)
            {
                Content = new ByteArrayContent(body),
                // Force HTTP/1.1 to preserve existing behaviour — Kinetica servers
                // typically do not support HTTP/2 and mis-negotiation would break
                // the connection silently.
                Version = HttpVersion.Version11,
            };
            request.Content.Headers.ContentType  = MediaTypeHeaderValue.Parse(contentType);
            request.Content.Headers.ContentLength = body.Length;

            if (!string.IsNullOrEmpty(authorization))
            {
                var space = authorization.IndexOf(' ');
                if (space > 0)
                {
                    request.Headers.Authorization = new AuthenticationHeaderValue(
                        authorization[..space],
                        authorization[(space + 1)..]);
                }
            }

            return request;
        }

        private static byte[] ReadOrThrow(
            HttpResponseMessage response,
            CancellationToken cancellationToken)
        {
            using var stream = response.Content.ReadAsStream(cancellationToken);
            using var buffer = new MemoryStream();
            stream.CopyTo(buffer);
            var bytes = buffer.ToArray();

            if (response.IsSuccessStatusCode)
                return bytes;

            // The Kinetica server encodes error bodies with the same Avro/JSON
            // envelope as success responses.  Hand the raw bytes back to the
            // caller via KineticaTransportException so that SubmitRequestRaw
            // can decode the server's error message.
            throw new KineticaTransportException((int)response.StatusCode, bytes);
        }

        public void Dispose()
        {
            if (_ownsClient)
                _client.Dispose();
        }
    }

    /// <summary>
    /// Thrown by <see cref="HttpClientTransport"/> when the server responds with
    /// a non-2xx status code.  The raw response body is preserved so that
    /// <see cref="Kinetica"/> can decode the Kinetica error envelope.
    /// </summary>
    internal sealed class KineticaTransportException : Exception
    {
        public int StatusCode { get; }
        public byte[] Body { get; }

        public KineticaTransportException(int statusCode, byte[] body)
            : base($"Kinetica server returned HTTP {statusCode}.")
        {
            StatusCode = statusCode;
            Body       = body;
        }
    }
}
