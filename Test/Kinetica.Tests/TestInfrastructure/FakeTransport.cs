using System;
using System.Threading;
using System.Threading.Tasks;
using kinetica;

namespace Kinetica.Tests.TestInfrastructure
{
    /// <summary>
    /// Test double for <see cref="IHttpTransport"/>.
    /// Captures the last call's arguments and returns a configurable response.
    /// <para><see cref="PostAsync"/> genuinely yields (<c>await Task.Yield()</c>)
    /// so that concurrent callers interleave.  An optional
    /// <see cref="PostAsyncGate"/> holds all in-flight requests until the test
    /// releases the gate, enabling deterministic concurrency assertions.</para>
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

        /// <summary>
        /// When set, <see cref="PostAsync"/> awaits this TCS before returning,
        /// allowing the test to hold all in-flight requests at a gate.
        /// </summary>
        public TaskCompletionSource<byte[]>? PostAsyncGate { get; set; }

        /// <summary>Number of times <see cref="PostAsync"/> has been entered.</summary>
        public int PostAsyncInvocations => _invocations;
        private int _invocations;

        public byte[] Post(string url, byte[] body, string contentType, string? authorization, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            RecordArgs(url, body, contentType, authorization, cancellationToken);

            if (ThrowOnPost is not null)
                throw ThrowOnPost;

            return ResponseBytes;
        }

        public async Task<byte[]> PostAsync(string url, byte[] body, string contentType, string? authorization, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _invocations);
            cancellationToken.ThrowIfCancellationRequested();

            // Always yield so concurrent callers interleave.
            await Task.Yield();

            RecordArgs(url, body, contentType, authorization, cancellationToken);

            if (ThrowOnPost is not null)
                throw ThrowOnPost;

            if (PostAsyncGate is not null)
                return await PostAsyncGate.Task.WaitAsync(cancellationToken).ConfigureAwait(false);

            return ResponseBytes;
        }

        private void RecordArgs(string url, byte[] body, string contentType, string? authorization, CancellationToken cancellationToken)
        {
            LastUrl               = url;
            LastBody              = body;
            LastContentType       = contentType;
            LastAuthorization     = authorization;
            LastCancellationToken = cancellationToken;
        }
    }
}
