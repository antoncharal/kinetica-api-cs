using System;
using System.Text;
using Kinetica.Tests.TestInfrastructure;
using kinetica;
using Shouldly;
using Xunit;

namespace Kinetica.Tests.Unit;

/// <summary>
/// Tests for <see cref="Kinetica.CreateAuthorizationHeader"/> which is
/// <c>internal</c> and exposed via <c>InternalsVisibleTo("Kinetica.Tests")</c>.
/// </summary>
public sealed class AuthorizationTests
{
    private static kinetica.Kinetica BuildSdk(string? username = null, string? password = null, string? oauthToken = null)
    {
        var options = new kinetica.Kinetica.Options
        {
            Username   = username   ?? string.Empty,
            Password   = password   ?? string.Empty,
            OauthToken = oauthToken ?? string.Empty
        };
        return new kinetica.Kinetica("http://localhost:9191", new FakeTransport(), options);
    }

    [Fact]
    public void NoCredentials_ReturnsNull()
    {
        var sdk = BuildSdk();
        sdk.CreateAuthorizationHeader().ShouldBeNull();
    }

    [Fact]
    public void OauthToken_ReturnsBearerHeader()
    {
        var sdk = BuildSdk(oauthToken: "my-token");
        sdk.CreateAuthorizationHeader().ShouldBe("Bearer my-token");
    }

    [Fact]
    public void UsernameAndPassword_ReturnsBasicHeader()
    {
        var sdk = BuildSdk(username: "admin", password: "pass");
        var header = sdk.CreateAuthorizationHeader();

        header.ShouldNotBeNull();
        header.ShouldStartWith("Basic ");
        var encoded = header!["Basic ".Length..];
        var decoded = Encoding.GetEncoding("ISO-8859-1").GetString(Convert.FromBase64String(encoded));
        decoded.ShouldBe("admin:pass");
    }

    [Fact]
    public void OauthToken_TakesPrecedenceOverUsernamePassword()
    {
        var sdk = BuildSdk(username: "user", password: "pass", oauthToken: "oauth-token");
        sdk.CreateAuthorizationHeader().ShouldBe("Bearer oauth-token");
    }
}
