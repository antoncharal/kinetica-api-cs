using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using kinetica;

namespace Kinetica.Benchmarks;

/// <summary>
/// Measures async fan-out throughput vs serial sync baseline.
/// Acceptance criterion from PR-04 §14: async ≥ 5× sync at concurrency 100.
/// </summary>
[ShortRunJob]
[MemoryDiagnoser(false)]
public class AsyncFanOutBenchmark
{
    private kinetica.Kinetica _sdk = null!;
    private byte[] _responseBytes = null!;

    [GlobalSetup]
    public void Setup()
    {
        _responseBytes = BuildMinimalShowSystemPropertiesResponse();
        var transport = new BenchmarkTransport(_responseBytes);
        _sdk = new kinetica.Kinetica("http://localhost:9191", transport);
    }

    [Benchmark(Baseline = true)]
    public void Sync_Serial_100()
    {
        for (int i = 0; i < 100; i++)
            _sdk.showSystemProperties();
    }

    [Benchmark]
    public async Task Async_FanOut_100()
    {
        var tasks = Enumerable.Range(0, 100)
            .Select(_ => _sdk.SubmitRequestAsync<ShowSystemPropertiesResponse>(
                "/show/system/properties",
                new ShowSystemPropertiesRequest()))
            .ToArray();

        await Task.WhenAll(tasks);
    }

    /// <summary>
    /// Build a minimal Avro-encoded ShowSystemPropertiesResponse wrapped in
    /// a RawKineticaResponse envelope, without depending on the test project.
    /// </summary>
    private static byte[] BuildMinimalShowSystemPropertiesResponse()
    {
        var inner = new ShowSystemPropertiesResponse
        {
            property_map = new System.Collections.Generic.Dictionary<string, string>()
        };

        using var innerMs = new System.IO.MemoryStream();
        var innerWriter = new Avro.Specific.SpecificDefaultWriter(inner.Schema);
        innerWriter.Write(inner.Schema, inner, new Avro.IO.BinaryEncoder(innerMs));
        var innerBytes = innerMs.ToArray();

        var raw = new RawKineticaResponse
        {
            status    = "OK",
            message   = "",
            data_type = "show_system_properties_response",
            data      = innerBytes,
            data_str  = ""
        };

        using var ms = new System.IO.MemoryStream();
        var writer = new Avro.Specific.SpecificDefaultWriter(raw.Schema);
        writer.Write(raw.Schema, raw, new Avro.IO.BinaryEncoder(ms));
        return ms.ToArray();
    }
}

/// <summary>
/// Minimal transport for benchmarks. Synchronous path blocks; async path
/// yields then returns, simulating a fast network round-trip so we measure
/// the SDK's orchestration overhead, not I/O latency.
/// </summary>
internal sealed class BenchmarkTransport : IHttpTransport
{
    private readonly byte[] _responseBytes;

    public BenchmarkTransport(byte[] responseBytes)
        => _responseBytes = responseBytes;

    public byte[] Post(string url, byte[] body, string contentType, string? authorization,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return _responseBytes;
    }

    public async Task<byte[]> PostAsync(string url, byte[] body, string contentType,
        string? authorization, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await Task.Yield();
        return _responseBytes;
    }
}
