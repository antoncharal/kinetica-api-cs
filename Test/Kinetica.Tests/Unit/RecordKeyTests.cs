using System.Collections.Generic;
using kinetica;
using kinetica.Utils;
using Shouldly;
using Xunit;

namespace Kinetica.Tests.Unit;

public sealed class RecordKeyTests
{
    // -------------------------------------------------------------------------
    // Construction guard
    // -------------------------------------------------------------------------

    [Fact]
    public void Constructor_SizeZero_Throws()
    {
        Should.Throw<KineticaException>(() => new RecordKey(0));
    }

    [Fact]
    public void Constructor_NegativeSize_Throws()
    {
        Should.Throw<KineticaException>(() => new RecordKey(-1));
    }

    [Fact]
    public void Constructor_PositiveSize_IsValid()
    {
        var key = new RecordKey(4);
        key.isValid().ShouldBeTrue();
    }

    // -------------------------------------------------------------------------
    // addInt — four-byte field
    // -------------------------------------------------------------------------

    [Fact]
    public void AddInt_NullValue_WritesZeroBytes()
    {
        // Buffer of exactly 4 bytes — null fills with zeros.
        var key = new RecordKey(4);
        key.addInt(null); // should not throw
        key.isValid().ShouldBeTrue();
    }

    [Fact]
    public void AddInt_ExceedsBufferSize_Throws()
    {
        var key = new RecordKey(2); // too small for int (4 bytes)
        Should.Throw<KineticaException>(() => key.addInt(42));
    }

    [Fact]
    public void AddInt_TwiceInFourByteBuffer_SecondCallThrows()
    {
        var key = new RecordKey(4); // only room for one int
        key.addInt(1);
        Should.Throw<KineticaException>(() => key.addInt(2));
    }

    // -------------------------------------------------------------------------
    // addLong — eight-byte field
    // -------------------------------------------------------------------------

    [Fact]
    public void AddLong_NullValue_WritesZeroBytes()
    {
        var key = new RecordKey(8);
        key.addLong(null);
        key.isValid().ShouldBeTrue();
    }

    [Fact]
    public void AddLong_ExceedsBufferSize_Throws()
    {
        var key = new RecordKey(4);
        Should.Throw<KineticaException>(() => key.addLong(42L));
    }

    // -------------------------------------------------------------------------
    // addInt8 — one-byte field
    // -------------------------------------------------------------------------

    [Fact]
    public void AddInt8_NullValue_WritesZeroByte()
    {
        var key = new RecordKey(1);
        key.addInt8(null);
        key.isValid().ShouldBeTrue();
    }

    [Fact]
    public void AddInt8_ExceedsBufferSize_Throws()
    {
        var key = new RecordKey(1);
        key.addInt8(1);
        Should.Throw<KineticaException>(() => key.addInt8(2)); // buffer already full
    }

    // -------------------------------------------------------------------------
    // addInt16 — two-byte field
    // -------------------------------------------------------------------------

    [Fact]
    public void AddInt16_NullValue_WritesTwoZeroBytes()
    {
        var key = new RecordKey(2);
        key.addInt16(null);
        key.isValid().ShouldBeTrue();
    }

    // -------------------------------------------------------------------------
    // Hash stability — same buffer contents → same hashCode
    // -------------------------------------------------------------------------

    [Fact]
    public void HashCode_SameIntValue_IsStable()
    {
        var k1 = new RecordKey(4);
        k1.addInt(12345);
        k1.computHashes();

        var k2 = new RecordKey(4);
        k2.addInt(12345);
        k2.computHashes();

        k1.hashCode().ShouldBe(k2.hashCode());
    }

    [Fact]
    public void HashCode_DifferentIntValues_AreDifferent()
    {
        var k1 = new RecordKey(4);
        k1.addInt(1);
        k1.computHashes();

        var k2 = new RecordKey(4);
        k2.addInt(2);
        k2.computHashes();

        k1.hashCode().ShouldNotBe(k2.hashCode());
    }

    [Fact]
    public void RoutingHash_SameInput_IsStable()
    {
        var k1 = new RecordKey(8);
        k1.addLong(999L);
        k1.computHashes();

        var k2 = new RecordKey(8);
        k2.addLong(999L);
        k2.computHashes();

        // Route to the same worker given the same routing table.
        var routingTable = new List<int> { 1, 2, 3, 4 };
        k1.route(routingTable).ShouldBe(k2.route(routingTable));
    }
}
