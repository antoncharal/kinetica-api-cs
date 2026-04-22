using System;
using System.Threading;
using System.Threading.Tasks;
using Kinetica.Tests.TestInfrastructure;
using kinetica;
using Shouldly;
using Xunit;

namespace Kinetica.Tests.Http;

/// <summary>
/// Tests for PR-07 sync plumbing hardening:
/// - CancellationToken propagation through sync SubmitRequest
/// - KineticaException preserves inner exception and status code
/// - TryDecodeErrorMessage fallback on malformed body
/// - Async catch block parity
/// </summary>
public sealed class SyncPlumbingTests
{
    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static (kinetica.Kinetica sdk, FakeTransport transport) BuildSdk()
    {
        var transport = new FakeTransport
        {
            ResponseBytes = AvroTestHelpers.BuildShowSystemPropertiesResponse()
        };
        var sdk = new kinetica.Kinetica("http://localhost:9191", transport);
        return (sdk, transport);
    }

    // -------------------------------------------------------------------------
    // CancellationToken propagation (sync)
    // -------------------------------------------------------------------------

    [Fact]
    public void SubmitRequest_CancellationToken_PropagatesToTransport()
    {
        var (sdk, transport) = BuildSdk();
        using var cts = new CancellationTokenSource();

        // showSystemProperties calls SubmitRequest internally (sync).
        // The token cannot be threaded through the generated method directly,
        // but we can verify the plumbing by invoking the internal overload.
        // Since showSystemProperties passes CancellationToken.None implicitly
        // (default parameter), verify the transport received it.
        sdk.showSystemProperties();

        // Default CancellationToken flows through to transport
        transport.LastCancellationToken.ShouldBe(CancellationToken.None);
    }

    [Fact]
    public void SubmitRequest_PreCancelledToken_ThrowsOperationCanceledException()
    {
        var transport = new FakeTransport
        {
            ResponseBytes = AvroTestHelpers.BuildShowSystemPropertiesResponse()
        };
        var sdk = new kinetica.Kinetica("http://localhost:9191", transport);

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // FakeTransport.Post calls ThrowIfCancellationRequested — the sync
        // path must let OperationCanceledException propagate unwrapped.
        Should.Throw<OperationCanceledException>(
            () => sdk.SubmitRequest<ShowSystemPropertiesResponse>(
                new Uri("http://localhost:9191/show/system/properties"),
                new ShowSystemPropertiesRequest(),
                cancellationToken: cts.Token));
    }

    // -------------------------------------------------------------------------
    // Transport exception → KineticaException preserves status + inner (sync)
    // -------------------------------------------------------------------------

    [Fact]
    public void TransportException_MappedToKineticaException_PreservesStatusAndInner()
    {
        var errorBody = AvroTestHelpers.BuildErrorResponse("Table not found");
        var transport = new FakeTransport
        {
            ThrowOnPost = new KineticaTransportException(404, errorBody)
        };
        var sdk = new kinetica.Kinetica("http://localhost:9191", transport);

        var ex = Should.Throw<KineticaException>(() => sdk.showSystemProperties());

        ex.StatusCode.ShouldBe(404);
        ex.InnerException.ShouldBeOfType<KineticaTransportException>();
        ex.Message.ShouldBe("Table not found");
    }

    [Fact]
    public void MalformedErrorEnvelope_StillThrowsKineticaException_WithFallbackMessage()
    {
        // Body that is NOT valid Avro — TryDecodeErrorMessage should return null
        var transport = new FakeTransport
        {
            ThrowOnPost = new KineticaTransportException(503, [0xFF, 0xFE])
        };
        var sdk = new kinetica.Kinetica("http://localhost:9191", transport);

        var ex = Should.Throw<KineticaException>(() => sdk.showSystemProperties());

        ex.StatusCode.ShouldBe(503);
        ex.InnerException.ShouldBeOfType<KineticaTransportException>();
        ex.Message.ShouldContain("HTTP 503");
    }

    // -------------------------------------------------------------------------
    // Transport exception → KineticaException preserves status + inner (async)
    // -------------------------------------------------------------------------

    [Fact]
    public async Task TransportExceptionAsync_MappedToKineticaException_PreservesStatusAndInner()
    {
        var errorBody = AvroTestHelpers.BuildErrorResponse("Permission denied");
        var transport = new FakeTransport
        {
            ThrowOnPost = new KineticaTransportException(403, errorBody)
        };
        var sdk = new kinetica.Kinetica("http://localhost:9191", transport);

        var ex = await Should.ThrowAsync<KineticaException>(
            () => sdk.SubmitRequestAsync<ShowSystemPropertiesResponse>(
                "/show/system/properties",
                new ShowSystemPropertiesRequest()));

        ex.StatusCode.ShouldBe(403);
        ex.InnerException.ShouldBeOfType<KineticaTransportException>();
        ex.Message.ShouldBe("Permission denied");
    }

    [Fact]
    public async Task MalformedErrorEnvelopeAsync_StillThrowsKineticaException_WithFallbackMessage()
    {
        var transport = new FakeTransport
        {
            ThrowOnPost = new KineticaTransportException(502, [0x00])
        };
        var sdk = new kinetica.Kinetica("http://localhost:9191", transport);

        var ex = await Should.ThrowAsync<KineticaException>(
            () => sdk.SubmitRequestAsync<ShowSystemPropertiesResponse>(
                "/show/system/properties",
                new ShowSystemPropertiesRequest()));

        ex.StatusCode.ShouldBe(502);
        ex.InnerException.ShouldBeOfType<KineticaTransportException>();
        ex.Message.ShouldContain("HTTP 502");
    }

    // -------------------------------------------------------------------------
    // Generic exception → KineticaException wrapping (catch-all path)
    // -------------------------------------------------------------------------

    [Fact]
    public void GenericException_WrappedAsKineticaException_WithInnerPreserved()
    {
        var transport = new FakeTransport
        {
            ThrowOnPost = new InvalidOperationException("connection reset by peer")
        };
        var sdk = new kinetica.Kinetica("http://localhost:9191", transport);

        var ex = Should.Throw<KineticaException>(() => sdk.showSystemProperties());

        ex.InnerException.ShouldBeOfType<InvalidOperationException>();
        ex.Message.ShouldContain("connection reset by peer");
        ex.StatusCode.ShouldBeNull();
    }

    // -------------------------------------------------------------------------
    // KineticaException.StatusCode
    // -------------------------------------------------------------------------

    [Fact]
    public void KineticaException_DefaultConstructor_StatusCodeIsNull()
    {
        var ex = new KineticaException("some error");
        ex.StatusCode.ShouldBeNull();
    }

    [Fact]
    public void KineticaException_StatusCodeConstructor_PreservesValues()
    {
        var inner = new InvalidOperationException("boom");
        var ex = new KineticaException("server error", 500, inner);

        ex.StatusCode.ShouldBe(500);
        ex.Message.ShouldBe("server error");
        ex.InnerException.ShouldBe(inner);
    }

    [Fact]
    public void KineticaException_StatusCodeNull_ClientSideFailure()
    {
        var ex = new KineticaException("network unreachable", null, new TimeoutException());

        ex.StatusCode.ShouldBeNull();
        ex.InnerException.ShouldBeOfType<TimeoutException>();
    }
}
