using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Kinetica.Tests.TestInfrastructure;
using kinetica;
using Shouldly;
using Xunit;

namespace Kinetica.Tests.Http;

/// <summary>
/// Contract tests for the async API surface introduced in PR-04.
/// Tests cover: cancellation, parallel fan-out, error mapping, and
/// sync/async result equivalence.
/// </summary>
public sealed class AsyncContractTests
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
    // Happy-path async
    // -------------------------------------------------------------------------

    [Fact]
    public async Task SubmitRequestAsync_ReturnsDecodedResponse()
    {
        var (sdk, _) = BuildSdk();

        var result = await sdk.SubmitRequestAsync<ShowSystemPropertiesResponse>(
            "/show/system/properties",
            new ShowSystemPropertiesRequest());

        result.ShouldNotBeNull();
        result.property_map.ShouldNotBeNull();
    }

    // -------------------------------------------------------------------------
    // Cancellation propagates as OperationCanceledException (not KineticaException)
    // -------------------------------------------------------------------------

    [Fact]
    public async Task SubmitRequestAsync_PreCancelledToken_ThrowsOperationCanceledException()
    {
        var (sdk, _) = BuildSdk();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Should.ThrowAsync<OperationCanceledException>(
            () => sdk.SubmitRequestAsync<ShowSystemPropertiesResponse>(
                "/show/system/properties",
                new ShowSystemPropertiesRequest(),
                cancellationToken: cts.Token));
    }

    [Fact]
    public async Task AddTableTypeAsync_PreCancelledToken_ThrowsOperationCanceledException()
    {
        var (sdk, _) = BuildSdk();
        // ResponseBytes don't matter — the pre-cancel check fires before any decode.
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Should.ThrowAsync<OperationCanceledException>(
            () => sdk.AddTableTypeAsync("my_table", typeof(object), cts.Token));
    }

    // -------------------------------------------------------------------------
    // Network error maps to KineticaException
    // -------------------------------------------------------------------------

    [Fact]
    public async Task SubmitRequestAsync_TransportThrows_MapsToKineticaException()
    {
        var (sdk, transport) = BuildSdk();
        transport.ThrowOnPost = new InvalidOperationException("socket refused");

        await Should.ThrowAsync<KineticaException>(
            () => sdk.SubmitRequestAsync<ShowSystemPropertiesResponse>(
                "/show/system/properties",
                new ShowSystemPropertiesRequest()));
    }

    // -------------------------------------------------------------------------
    // Parallel fan-out: 100 concurrent calls — no deadlock, no thread saturation
    // -------------------------------------------------------------------------

    [Fact]
    public async Task SubmitRequestAsync_ParallelFanOut_Completes()
    {
        var (sdk, transport) = BuildSdk();

        var tasks = Enumerable.Range(0, 100)
            .Select(_ => sdk.SubmitRequestAsync<ShowSystemPropertiesResponse>(
                "/show/system/properties",
                new ShowSystemPropertiesRequest()))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        results.Length.ShouldBe(100);
        foreach (var r in results)
            r.ShouldNotBeNull();

        transport.PostAsyncInvocations.ShouldBe(100);
    }

    // -------------------------------------------------------------------------
    // Sync / async equivalence: same transport bytes → same decoded result
    // -------------------------------------------------------------------------

    [Fact]
    public async Task SyncAndAsync_ProduceEquivalentResults()
    {
        var responseBytes = AvroTestHelpers.BuildShowSystemPropertiesResponse(
            new Dictionary<string, string> { ["version"] = "7.2.0.0" });

        var (sdkSync, transportSync) = BuildSdk();
        transportSync.ResponseBytes = responseBytes;
        var (sdkAsync, transportAsync) = BuildSdk();
        transportAsync.ResponseBytes = responseBytes;

        var syncResult  = sdkSync.showSystemProperties();
        var asyncResult = await sdkAsync.SubmitRequestAsync<ShowSystemPropertiesResponse>(
            "/show/system/properties",
            new ShowSystemPropertiesRequest());

        syncResult.property_map["version"].ShouldBe(asyncResult.property_map["version"]);
        transportSync.LastBody.ShouldBe(transportAsync.LastBody);
    }

    // -------------------------------------------------------------------------
    // Transport content-type and authorization forwarded from async path
    // -------------------------------------------------------------------------

    [Fact]
    public async Task SubmitRequestAsync_SetsOctetStreamContentType()
    {
        var (sdk, transport) = BuildSdk();

        await sdk.SubmitRequestAsync<ShowSystemPropertiesResponse>(
            "/show/system/properties",
            new ShowSystemPropertiesRequest());

        transport.LastContentType.ShouldBe("application/octet-stream");
    }

    [Fact]
    public async Task SubmitRequestAsync_BearerAuth_ForwardedToTransport()
    {
        var options = new kinetica.Kinetica.Options { OauthToken = "async-token" };
        var (sdk, transport) = BuildSdk(options);

        await sdk.SubmitRequestAsync<ShowSystemPropertiesResponse>(
            "/show/system/properties",
            new ShowSystemPropertiesRequest());

        transport.LastAuthorization.ShouldBe("Bearer async-token");
    }

    // -------------------------------------------------------------------------
    // URL composition preserved in async path
    // -------------------------------------------------------------------------

    [Fact]
    public async Task SubmitRequestAsync_ComposesFullUrl()
    {
        var (sdk, transport) = BuildSdk();

        await sdk.SubmitRequestAsync<ShowSystemPropertiesResponse>(
            "/show/system/properties",
            new ShowSystemPropertiesRequest());

        transport.LastUrl.ShouldBe("http://localhost:9191/show/system/properties");
    }

    // -------------------------------------------------------------------------
    // Dispose + async
    // -------------------------------------------------------------------------

    [Fact]
    public async Task SubmitRequestAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        var (sdk, _) = BuildSdk();
        sdk.Dispose();

        await Should.ThrowAsync<ObjectDisposedException>(
            () => sdk.SubmitRequestAsync<ShowSystemPropertiesResponse>(
                "/show/system/properties",
                new ShowSystemPropertiesRequest()));
    }
}
