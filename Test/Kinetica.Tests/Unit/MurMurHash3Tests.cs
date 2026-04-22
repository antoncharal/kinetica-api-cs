using System.Text;
using kinetica;
using Shouldly;
using Xunit;

namespace Kinetica.Tests.Unit;

public sealed class MurMurHash3Tests
{
    // -------------------------------------------------------------------------
    // Empty input
    // -------------------------------------------------------------------------

    [Fact]
    public void EmptyInput_WithSeedZero_ReturnsBothValuesZero()
    {
        MurMurHash3.murmurhash3_x64_128([], 0, 0, 0, out var result);

        result.val1.ShouldBe(0L);
        result.val2.ShouldBe(0L);
    }

    // -------------------------------------------------------------------------
    // Determinism — same input always produces same output
    // -------------------------------------------------------------------------

    [Theory]
    [InlineData("Kinetica")]
    [InlineData("hello world")]
    [InlineData("a")]
    [InlineData("abcdefghijklmnop")] // exactly 16 bytes — one full block
    [InlineData("abcdefghijklmnopq")] // 17 bytes — 1 block + 1 tail byte
    public void Deterministic_SameInput_SameHash(string input)
    {
        var key = Encoding.UTF8.GetBytes(input);
        MurMurHash3.murmurhash3_x64_128(key, 0, (uint)key.Length, 0, out var r1);
        MurMurHash3.murmurhash3_x64_128(key, 0, (uint)key.Length, 0, out var r2);

        r1.val1.ShouldBe(r2.val1);
        r1.val2.ShouldBe(r2.val2);
    }

    // -------------------------------------------------------------------------
    // Different inputs → different hashes (collision avoidance for test cases)
    // -------------------------------------------------------------------------

    [Theory]
    [InlineData("abc", "def")]
    [InlineData("abc", "abcd")]
    [InlineData("Kinetica", "kinetica")]  // case sensitivity
    public void DifferentInputs_ProduceDifferentHashes(string a, string b)
    {
        var keyA = Encoding.UTF8.GetBytes(a);
        var keyB = Encoding.UTF8.GetBytes(b);

        MurMurHash3.murmurhash3_x64_128(keyA, 0, (uint)keyA.Length, 0, out var hashA);
        MurMurHash3.murmurhash3_x64_128(keyB, 0, (uint)keyB.Length, 0, out var hashB);

        // At least one half must differ for a meaningful collision test.
        (hashA.val1 != hashB.val1 || hashA.val2 != hashB.val2).ShouldBeTrue();
    }

    // -------------------------------------------------------------------------
    // Seed affects output
    // -------------------------------------------------------------------------

    [Fact]
    public void DifferentSeeds_ProduceDifferentHashes()
    {
        var key = Encoding.UTF8.GetBytes("test-key");
        MurMurHash3.murmurhash3_x64_128(key, 0, (uint)key.Length, 0,  out var h0);
        MurMurHash3.murmurhash3_x64_128(key, 0, (uint)key.Length, 42, out var h42);

        (h0.val1 != h42.val1 || h0.val2 != h42.val2).ShouldBeTrue();
    }

    // -------------------------------------------------------------------------
    // Tail-byte processing: inputs of length 1–15 (non-block-aligned)
    // -------------------------------------------------------------------------

    [Theory]
    [InlineData(1)]
    [InlineData(7)]
    [InlineData(9)]
    [InlineData(15)]
    public void NonBlockAligned_Input_DoesNotThrow(int length)
    {
        var key = new byte[length];
        MurMurHash3.murmurhash3_x64_128(key, 0, (uint)key.Length, 10, out var result);

        // Just verify the call completed and returns a LongPair instance.
        result.ShouldNotBeNull();
    }

    // -------------------------------------------------------------------------
    // Large input (> 16 bytes — verifies block processing path)
    // -------------------------------------------------------------------------

    [Fact]
    public void LargeInput_HashIsStable()
    {
        var key = Encoding.UTF8.GetBytes(new string('A', 1024));
        MurMurHash3.murmurhash3_x64_128(key, 0, (uint)key.Length, 0, out var r1);
        MurMurHash3.murmurhash3_x64_128(key, 0, (uint)key.Length, 0, out var r2);

        r1.val1.ShouldBe(r2.val1);
        r1.val2.ShouldBe(r2.val2);
    }
}
