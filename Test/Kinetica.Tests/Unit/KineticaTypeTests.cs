using System.Collections.Generic;
using Avro;
using kinetica;
using Shouldly;
using Xunit;

namespace Kinetica.Tests.Unit;

public sealed class KineticaTypeTests
{
    // -------------------------------------------------------------------------
    // Column construction
    // -------------------------------------------------------------------------

    [Theory]
    [InlineData(KineticaType.Column.ColumnType.BYTES,  "bytes")]
    [InlineData(KineticaType.Column.ColumnType.DOUBLE, "double")]
    [InlineData(KineticaType.Column.ColumnType.FLOAT,  "float")]
    [InlineData(KineticaType.Column.ColumnType.INT,    "int")]
    [InlineData(KineticaType.Column.ColumnType.LONG,   "long")]
    [InlineData(KineticaType.Column.ColumnType.STRING, "string")]
    public void Column_GetTypeString_ReturnsPrimitiveAvroName(
        KineticaType.Column.ColumnType columnType, string expectedAvroName)
    {
        var col = new KineticaType.Column("col", columnType);
        col.getTypeString().ShouldBe(expectedAvroName);
    }

    [Fact]
    public void Column_WithNullableProperty_IsNullable()
    {
        var col = new KineticaType.Column("col", KineticaType.Column.ColumnType.STRING,
                                          [ColumnProperty.NULLABLE]);
        col.isNullable().ShouldBeTrue();
    }

    [Fact]
    public void Column_WithoutNullableProperty_IsNotNullable()
    {
        var col = new KineticaType.Column("col", KineticaType.Column.ColumnType.INT);
        col.isNullable().ShouldBeFalse();
    }

    [Fact]
    public void Column_EmptyName_Throws()
    {
        Should.Throw<System.ArgumentException>(() =>
            new KineticaType.Column("", KineticaType.Column.ColumnType.INT));
    }

    // -------------------------------------------------------------------------
    // KineticaType construction from columns
    // -------------------------------------------------------------------------

    [Fact]
    public void KineticaType_FromColumns_ColumnCountMatches()
    {
        var cols = new List<KineticaType.Column>
        {
            new("id",   KineticaType.Column.ColumnType.INT),
            new("name", KineticaType.Column.ColumnType.STRING)
        };
        var ktype = new KineticaType("test_label", cols);
        ktype.getColumnCount().ShouldBe(2);
    }

    [Fact]
    public void KineticaType_HasColumn_ReturnsTrueForDeclaredColumn()
    {
        var cols = new List<KineticaType.Column>
        {
            new("score", KineticaType.Column.ColumnType.DOUBLE)
        };
        var ktype = new KineticaType(cols);
        ktype.hasColumn("score").ShouldBeTrue();
        ktype.hasColumn("missing").ShouldBeFalse();
    }

    [Fact]
    public void KineticaType_DuplicateColumnName_Throws()
    {
        var cols = new List<KineticaType.Column>
        {
            new("x", KineticaType.Column.ColumnType.FLOAT),
            new("x", KineticaType.Column.ColumnType.FLOAT)
        };
        Should.Throw<System.ArgumentException>(() => new KineticaType(cols));
    }

    [Fact]
    public void KineticaType_EmptyColumnList_Throws()
    {
        Should.Throw<System.ArgumentException>(() =>
            new KineticaType(new List<KineticaType.Column>()));
    }

    // -------------------------------------------------------------------------
    // getSchemaString — column-based type produces parseable Avro JSON
    // -------------------------------------------------------------------------

    [Fact]
    public void GetSchemaString_FromColumns_ProducesParseableAvroJson()
    {
        var cols = new List<KineticaType.Column>
        {
            new("id",    KineticaType.Column.ColumnType.LONG),
            new("label", KineticaType.Column.ColumnType.STRING)
        };
        var ktype = new KineticaType("my_type", cols);
        var schemaStr = ktype.getSchemaString();

        schemaStr.ShouldNotBeNullOrWhiteSpace();

        // Must parse as a valid Avro record schema without throwing.
        var schema = RecordSchema.Parse(schemaStr);
        schema.ShouldNotBeNull();
        schema.Name.ShouldBe("type_name"); // Kinetica always uses "type_name"
    }

    [Fact]
    public void GetSchemaString_FromColumns_ContainsAllFieldNames()
    {
        var cols = new List<KineticaType.Column>
        {
            new("alpha", KineticaType.Column.ColumnType.INT),
            new("beta",  KineticaType.Column.ColumnType.FLOAT)
        };
        var ktype = new KineticaType(cols);
        var schemaStr = ktype.getSchemaString();

        schemaStr.ShouldContain("alpha");
        schemaStr.ShouldContain("beta");
    }

    // -------------------------------------------------------------------------
    // fromDynamicSchema
    // -------------------------------------------------------------------------

    [Fact]
    public void FromDynamicSchema_MismatchedHeadersAndTypes_Throws()
    {
        var schemaStr = @"{""type"":""record"",""name"":""t"",""fields"":[{""name"":""id"",""type"":{""type"":""array"",""items"":""int""}}]}";
        Should.Throw<KineticaException>(() =>
            KineticaType.fromDynamicSchema(schemaStr,
                ["id", "name"],       // 2 headers
                ["int"]));            // 1 type → mismatch
    }

    [Fact]
    public void FromDynamicSchema_StringColumn_CreatesStringType()
    {
        // Kinetica dynamic schemas use "column_N" (1-indexed) as field names;
        // the human-readable column name comes from columnHeaders.
        var schemaStr = @"{""type"":""record"",""name"":""t"",""fields"":[{""name"":""column_1"",""type"":{""type"":""array"",""items"":[""string"",""null""]}}]}";
        var ktype = KineticaType.fromDynamicSchema(schemaStr, new object[] { "label" }, new object[] { "string" });

        ktype.hasColumn("label").ShouldBeTrue();
        ktype.getColumn("label").getType().ShouldBe(KineticaType.Column.ColumnType.STRING);
        // The column should be nullable because the schema union includes null.
        ktype.getColumn("label").isNullable().ShouldBeTrue();
    }

    // -------------------------------------------------------------------------
    // Column property propagation
    // -------------------------------------------------------------------------

    [Fact]
    public void Column_WithPrimaryKeyProperty_ExposesProperty()
    {
        var col = new KineticaType.Column("id", KineticaType.Column.ColumnType.INT,
                                          [ColumnProperty.PRIMARY_KEY]);
        col.getProperties().ShouldContain(ColumnProperty.PRIMARY_KEY);
    }
}
