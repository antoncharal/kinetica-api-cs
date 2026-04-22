using System;
using System.Threading;
using System.Threading.Tasks;
using kinetica;

namespace Kinetica.Tests.TestInfrastructure
{
    /// <summary>
    /// Test double for <see cref="IHttpTransport"/>.
    /// Captures the last call's arguments and returns a configurable response.
    /// </summary>
    internal sealed class FakeTransport : IHttpTransport
    {
        public string? LastUrl { get; private set; }
        public byte[]? LastBody { get; private set; }
        public string? LastContentType { get; private set; }
        public string? LastAuthorization { get; private set; }
        public CancellationToken LastCancellationToken { get; private set; }

        /// <summary>Bytes returned to the caller on the next <see cref="Post"/> call.</summary>
        public byte[] ResponseBytes { get; set; } = Array.Empty<byte>();

        /// <summary>When set, <see cref="Post"/> throws this exception instead of returning.</summary>
        public Exception? ThrowOnPost { get; set; }

        public byte[] Post(string url, byte[] body, string contentType, string? authorization, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            LastUrl               = url;
            LastBody              = body;
            LastContentType       = contentType;
            LastAuthorization     = authorization;
            LastCancellationToken = cancellationToken;

            if (ThrowOnPost is not null)
                throw ThrowOnPost;

            return ResponseBytes;
        }

        public Task<byte[]> PostAsync(string url, byte[] body, string contentType, string? authorization, CancellationToken cancellationToken)
        {
            // ThrowIfCancellationRequested before recording to mirror real transport behaviour.
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(Post(url, body, contentType, authorization, cancellationToken));
        }
    }
}
