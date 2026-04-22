using System.Collections.Generic;
using Kinetica.Tests.TestInfrastructure;
using kinetica;
using Shouldly;
using Xunit;

namespace Kinetica.Tests.Http;

/// <summary>
/// Contract tests for the <see cref="IHttpTransport"/> seam.
/// Each test configures a <see cref="FakeTransport"/>, calls a real SDK method,
/// and asserts that the transport received the correct URL, Content-Type,
/// and Authorization header.
/// </summary>
public sealed class SubmitRequestTests
{
    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static (kinetica.Kinetica sdk, FakeTransport transport) BuildSdk(
        kinetica.Kinetica.Options? options = null)
    {
        var transport = new FakeTransport
        {
            ResponseBytes = AvroTestHelpers.BuildShowSystemPropertiesResponse()
        };
        var sdk = new kinetica.Kinetica("http://localhost:9191", transport, options);
        return (sdk, transport);
    }

    // -------------------------------------------------------------------------
    // Content-Type
    // -------------------------------------------------------------------------

    [Fact]
    public void AvroRequest_SetsOctetStreamContentType()
    {
        var (sdk, transport) = BuildSdk();

        sdk.showSystemProperties();

        transport.LastContentType.ShouldBe("application/octet-stream");
    }

    // -------------------------------------------------------------------------
    // URL composition
    // -------------------------------------------------------------------------

    [Fact]
    public void ShowSystemProperties_PostsToCorrectEndpoint()
    {
        var (sdk, transport) = BuildSdk();

        sdk.showSystemProperties();

        transport.LastUrl.ShouldBe("http://localhost:9191/show/system/properties");
    }

    // -------------------------------------------------------------------------
    // Authorization: no credentials → no header
    // -------------------------------------------------------------------------

    [Fact]
    public void NoCredentials_SendsNullAuthorizationHeader()
    {
        var (sdk, transport) = BuildSdk();

        sdk.showSystemProperties();

        transport.LastAuthorization.ShouldBeNull();
    }

    // -------------------------------------------------------------------------
    // Authorization: Bearer (OAuth)
    // -------------------------------------------------------------------------

    [Fact]
    public void OauthToken_SendsBearerAuthorizationHeader()
    {
        var options = new kinetica.Kinetica.Options { OauthToken = "my-token" };
        var (sdk, transport) = BuildSdk(options);

        sdk.showSystemProperties();

        transport.LastAuthorization.ShouldBe("Bearer my-token");
    }

    // -------------------------------------------------------------------------
    // Authorization: Basic (username / password)
    // -------------------------------------------------------------------------

    [Fact]
    public void UsernamePassword_SendsBasicAuthorizationHeader()
    {
        var options = new kinetica.Kinetica.Options { Username = "admin", Password = "pass" };
        var (sdk, transport) = BuildSdk(options);

        sdk.showSystemProperties();

        transport.LastAuthorization.ShouldNotBeNull();
        transport.LastAuthorization!.ShouldStartWith("Basic ");
    }

    // -------------------------------------------------------------------------
    // Request body is non-empty
    // -------------------------------------------------------------------------

    [Fact]
    public void AvroRequest_SendsNonEmptyBody()
    {
        var (sdk, transport) = BuildSdk();

        sdk.showSystemProperties();

        transport.LastBody.ShouldNotBeNull();
        transport.LastBody!.Length.ShouldBeGreaterThan(0);
    }

    // -------------------------------------------------------------------------
    // Transport exception → KineticaException propagation
    // -------------------------------------------------------------------------

    [Fact]
    public void TransportException_WrappedAsKineticaException()
    {
        var transport = new FakeTransport
        {
            ThrowOnPost = new System.Exception("simulated network failure")
        };
        var sdk = new kinetica.Kinetica("http://localhost:9191", transport);

        Should.Throw<KineticaException>(() => sdk.showSystemProperties());
    }

    [Fact]
    public void KineticaExceptionFromTransport_PropagatesAsKineticaException()
    {
        var transport = new FakeTransport
        {
            ThrowOnPost = new KineticaException("upstream error")
        };
        var sdk = new kinetica.Kinetica("http://localhost:9191", transport);

        Should.Throw<KineticaException>(() => sdk.showSystemProperties());
    }
}
