using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Kinetica.Tests.TestInfrastructure;
using kinetica;
using Shouldly;
using Xunit;

namespace Kinetica.Tests.Http;

/// <summary>
/// Tests for <see cref="HttpClientTransport"/> covering timeout, cancellation,
/// large payloads, authorization header formatting, and <see cref="kinetica.Kinetica"/>
/// dispose semantics.
/// </summary>
public sealed class HttpClientTransportTests
{
    // -------------------------------------------------------------------------
    // Cancellation
    // -------------------------------------------------------------------------

    [Fact]
    public void Post_WithPreCancelledToken_ThrowsOperationCanceledException()
    {
        using var handler = new StaticResponseHandler(Array.Empty<byte>(), HttpStatusCode.OK);
        using var httpClient = new HttpClient(handler);
        using var transport = new HttpClientTransport(httpClient);

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Should.Throw<OperationCanceledException>(() =>
            transport.Post("http://localhost:9191/test", Array.Empty<byte>(),
                "application/json", null, cts.Token));
    }

    // -------------------------------------------------------------------------
    // Timeout
    // -------------------------------------------------------------------------

    [Fact]
    public void Post_WhenTimeoutExpires_ThrowsOperationCanceledException()
    {
        using var handler = new SlowResponseHandler(delay: TimeSpan.FromSeconds(10));
        using var httpClient = new HttpClient(handler) { Timeout = TimeSpan.FromMilliseconds(50) };
        using var transport = new HttpClientTransport(httpClient);

        Should.Throw<OperationCanceledException>(() =>
            transport.Post("http://localhost:9191/test", Array.Empty<byte>(),
                "application/json", null, CancellationToken.None));
    }

    // -------------------------------------------------------------------------
    // Large response
    // -------------------------------------------------------------------------

    [Fact]
    public void Post_LargeResponse_ReadsFullBody()
    {
        const int tenMb = 10 * 1024 * 1024;
        var expected = new byte[tenMb];
        new Random(42).NextBytes(expected);

        using var handler = new StaticResponseHandler(expected, HttpStatusCode.OK);
        using var httpClient = new HttpClient(handler);
        using var transport = new HttpClientTransport(httpClient);

        var result = transport.Post("http://localhost:9191/test", Array.Empty<byte>(),
            "application/json", null, CancellationToken.None);

        result.ShouldBe(expected);
    }

    // -------------------------------------------------------------------------
    // Authorization header formatting
    // -------------------------------------------------------------------------

    [Fact]
    public void Post_BasicAuthorization_SetsSchemeAndParameter()
    {
        HttpRequestMessage? captured = null;
        using var handler = new CapturingHandler(req => captured = req, Array.Empty<byte>());
        using var httpClient = new HttpClient(handler);
        using var transport = new HttpClientTransport(httpClient);

        transport.Post("http://localhost:9191/test", Array.Empty<byte>(),
            "application/json", "Basic dXNlcjpwYXNz", CancellationToken.None);

        captured!.Headers.Authorization!.Scheme.ShouldBe("Basic");
        captured.Headers.Authorization.Parameter.ShouldBe("dXNlcjpwYXNz");
    }

    [Fact]
    public void Post_BearerAuthorization_SetsSchemeAndParameter()
    {
        HttpRequestMessage? captured = null;
        using var handler = new CapturingHandler(req => captured = req, Array.Empty<byte>());
        using var httpClient = new HttpClient(handler);
        using var transport = new HttpClientTransport(httpClient);

        transport.Post("http://localhost:9191/test", Array.Empty<byte>(),
            "application/json", "Bearer my-token", CancellationToken.None);

        captured!.Headers.Authorization!.Scheme.ShouldBe("Bearer");
        captured.Headers.Authorization.Parameter.ShouldBe("my-token");
    }

    [Fact]
    public void Post_NoAuthorization_OmitsAuthorizationHeader()
    {
        HttpRequestMessage? captured = null;
        using var handler = new CapturingHandler(req => captured = req, Array.Empty<byte>());
        using var httpClient = new HttpClient(handler);
        using var transport = new HttpClientTransport(httpClient);

        transport.Post("http://localhost:9191/test", Array.Empty<byte>(),
            "application/json", null, CancellationToken.None);

        captured!.Headers.Authorization.ShouldBeNull();
    }

    // -------------------------------------------------------------------------
    // HTTP/1.1 forced
    // -------------------------------------------------------------------------

    [Fact]
    public void Post_AlwaysUsesHttp11()
    {
        HttpRequestMessage? captured = null;
        using var handler = new CapturingHandler(req => captured = req, Array.Empty<byte>());
        using var httpClient = new HttpClient(handler);
        using var transport = new HttpClientTransport(httpClient);

        transport.Post("http://localhost:9191/test", Array.Empty<byte>(),
            "application/json", null, CancellationToken.None);

        captured!.Version.ShouldBe(HttpVersion.Version11);
    }

    // -------------------------------------------------------------------------
    // Error response → KineticaTransportException
    // -------------------------------------------------------------------------

    [Fact]
    public void Post_ServerReturns500_ThrowsKineticaTransportException()
    {
        var errorBody = new byte[] { 0x01, 0x02, 0x03 };
        using var handler = new StaticResponseHandler(errorBody, HttpStatusCode.InternalServerError);
        using var httpClient = new HttpClient(handler);
        using var transport = new HttpClientTransport(httpClient);

        var ex = Should.Throw<KineticaTransportException>(() =>
            transport.Post("http://localhost:9191/test", Array.Empty<byte>(),
                "application/json", null, CancellationToken.None));

        ex.StatusCode.ShouldBe(500);
        ex.Body.ShouldBe(errorBody);
    }

    // -------------------------------------------------------------------------
    // Kinetica.Dispose semantics
    // -------------------------------------------------------------------------

    [Fact]
    public void Kinetica_Dispose_CalledTwice_DoesNotThrow()
    {
        var transport = new FakeTransport
        {
            ResponseBytes = AvroTestHelpers.BuildShowSystemPropertiesResponse()
        };
        var sdk = new kinetica.Kinetica("http://localhost:9191", transport);

        sdk.Dispose();

        Should.NotThrow(() => sdk.Dispose());
    }

    [Fact]
    public void Kinetica_Dispose_ThenRequest_ThrowsObjectDisposedException()
    {
        var transport = new FakeTransport
        {
            ResponseBytes = AvroTestHelpers.BuildShowSystemPropertiesResponse()
        };
        var sdk = new kinetica.Kinetica("http://localhost:9191", transport);
        sdk.Dispose();

        Should.Throw<ObjectDisposedException>(() => sdk.showSystemProperties());
    }

    // =========================================================================
    // Async path — PostAsync
    // =========================================================================

    [Fact]
    public async Task PostAsync_Success_ReturnsResponseBytes()
    {
        var expected = new byte[] { 0xCA, 0xFE };
        using var handler = new StaticResponseHandler(expected, HttpStatusCode.OK);
        using var httpClient = new HttpClient(handler);
        using var transport = new HttpClientTransport(httpClient);

        var result = await transport.PostAsync("http://localhost:9191/test",
            Array.Empty<byte>(), "application/octet-stream", null, CancellationToken.None);

        result.ShouldBe(expected);
    }

    [Fact]
    public async Task PostAsync_500Response_ThrowsKineticaTransportException_WithBody()
    {
        var errorBody = new byte[] { 0x01, 0x02, 0x03 };
        using var handler = new StaticResponseHandler(errorBody, HttpStatusCode.InternalServerError);
        using var httpClient = new HttpClient(handler);
        using var transport = new HttpClientTransport(httpClient);

        var ex = await Should.ThrowAsync<KineticaTransportException>(
            () => transport.PostAsync("http://localhost:9191/test",
                Array.Empty<byte>(), "application/json", null, CancellationToken.None));

        ex.StatusCode.ShouldBe(500);
        ex.Body.ShouldBe(errorBody);
    }

    [Fact]
    public async Task PostAsync_PreCancelledToken_ThrowsOperationCanceledException()
    {
        using var handler = new StaticResponseHandler(Array.Empty<byte>(), HttpStatusCode.OK);
        using var httpClient = new HttpClient(handler);
        using var transport = new HttpClientTransport(httpClient);

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Should.ThrowAsync<OperationCanceledException>(
            () => transport.PostAsync("http://localhost:9191/test",
                Array.Empty<byte>(), "application/json", null, cts.Token));
    }

    [Fact]
    public async Task PostAsync_CancelDuringSend_PropagatesOperationCanceledException()
    {
        using var cts = new CancellationTokenSource();
        using var handler = new CancellingHandler(cts);
        using var httpClient = new HttpClient(handler);
        using var transport = new HttpClientTransport(httpClient);

        await Should.ThrowAsync<OperationCanceledException>(
            () => transport.PostAsync("http://localhost:9191/test",
                Array.Empty<byte>(), "application/json", null, cts.Token));
    }

    [Fact]
    public async Task PostAsync_SetsAuthorizationHeader_WhenProvided()
    {
        HttpRequestMessage? captured = null;
        using var handler = new CapturingHandler(req => captured = req, Array.Empty<byte>());
        using var httpClient = new HttpClient(handler);
        using var transport = new HttpClientTransport(httpClient);

        await transport.PostAsync("http://localhost:9191/test",
            Array.Empty<byte>(), "application/json", "Bearer my-async-token", CancellationToken.None);

        captured!.Headers.Authorization!.Scheme.ShouldBe("Bearer");
        captured.Headers.Authorization.Parameter.ShouldBe("my-async-token");
    }

    [Fact]
    public async Task PostAsync_SetsContentTypeHeader_FromParameter()
    {
        HttpRequestMessage? captured = null;
        using var handler = new CapturingHandler(req => captured = req, Array.Empty<byte>());
        using var httpClient = new HttpClient(handler);
        using var transport = new HttpClientTransport(httpClient);

        await transport.PostAsync("http://localhost:9191/test",
            new byte[] { 0x01 }, "application/octet-stream", null, CancellationToken.None);

        captured!.Content!.Headers.ContentType!.MediaType.ShouldBe("application/octet-stream");
    }

    [Fact]
    public async Task PostAsync_UsesHttpVersion11()
    {
        HttpRequestMessage? captured = null;
        using var handler = new CapturingHandler(req => captured = req, Array.Empty<byte>());
        using var httpClient = new HttpClient(handler);
        using var transport = new HttpClientTransport(httpClient);

        await transport.PostAsync("http://localhost:9191/test",
            Array.Empty<byte>(), "application/json", null, CancellationToken.None);

        captured!.Version.ShouldBe(HttpVersion.Version11);
    }

    // =========================================================================
    // Private test handlers
    // =========================================================================

    /// <summary>Returns a fixed byte array with the given status code.</summary>
    private sealed class StaticResponseHandler : HttpMessageHandler
    {
        private readonly byte[] _body;
        private readonly HttpStatusCode _status;

        public StaticResponseHandler(byte[] body, HttpStatusCode status)
        {
            _body   = body;
            _status = status;
        }

        protected override HttpResponseMessage Send(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var response = new HttpResponseMessage(_status)
            {
                Content = new ByteArrayContent(_body),
            };
            return response;
        }

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
            => Task.FromResult(Send(request, cancellationToken));
    }

    /// <summary>Blocks until the cancellation token fires, simulating a slow server.</summary>
    private sealed class SlowResponseHandler : HttpMessageHandler
    {
        private readonly TimeSpan _delay;

        public SlowResponseHandler(TimeSpan delay) => _delay = delay;

        protected override HttpResponseMessage Send(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            // WaitOne returns true when the handle is set (cancelled) or false on timeout.
            // Either way, the subsequent ThrowIfCancellationRequested will fire if the
            // HttpClient's internal timeout token has been triggered.
            cancellationToken.WaitHandle.WaitOne(_delay);
            cancellationToken.ThrowIfCancellationRequested();
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new ByteArrayContent(Array.Empty<byte>()),
            };
        }

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
            => Task.FromResult(Send(request, cancellationToken));
    }

    /// <summary>Captures the outgoing <see cref="HttpRequestMessage"/> for assertion.</summary>
    private sealed class CapturingHandler : HttpMessageHandler
    {
        private readonly Action<HttpRequestMessage> _capture;
        private readonly byte[] _responseBody;

        public CapturingHandler(Action<HttpRequestMessage> capture, byte[] responseBody)
        {
            _capture      = capture;
            _responseBody = responseBody;
        }

        protected override HttpResponseMessage Send(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            _capture(request);
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new ByteArrayContent(_responseBody),
            };
        }

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
            => Task.FromResult(Send(request, cancellationToken));
    }

    /// <summary>Invokes a callback then returns a fixed response — for tracking invocation.</summary>
    private sealed class CallbackHandler : HttpMessageHandler
    {
        private readonly Action<HttpRequestMessage> _callback;
        private readonly byte[] _responseBody;

        public CallbackHandler(Action<HttpRequestMessage> callback, byte[] responseBody)
        {
            _callback     = callback;
            _responseBody = responseBody;
        }

        protected override HttpResponseMessage Send(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            _callback(request);
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new ByteArrayContent(_responseBody),
            };
        }

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            _callback(request);
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new ByteArrayContent(_responseBody),
            });
        }
    }

    /// <summary>Cancels the provided CTS during SendAsync, simulating mid-flight cancellation.</summary>
    private sealed class CancellingHandler : HttpMessageHandler
    {
        private readonly CancellationTokenSource _cts;

        public CancellingHandler(CancellationTokenSource cts) => _cts = cts;

        protected override HttpResponseMessage Send(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            _cts.Cancel();
            cancellationToken.ThrowIfCancellationRequested();
            return new HttpResponseMessage(HttpStatusCode.OK);
        }

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            _cts.Cancel();
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));
        }
    }
}
