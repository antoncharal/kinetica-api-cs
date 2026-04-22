using Avro;
using Avro.IO;
using System;
using System.Collections.Generic;
using System.IO;



namespace kinetica;
/// <summary>
/// Convenience class for using Avro.Generic.GenericRecord objects.
/// </summary>
public class KineticaRecord : Avro.Generic.GenericRecord
{
    /// <summary>
    /// Creates a <see cref="KineticaRecord"/> backed by the given Avro record schema.
    /// </summary>
    /// <param name="schema">The Avro <see cref="RecordSchema"/> that defines the
    /// fields of this record.</param>
    public KineticaRecord( RecordSchema schema ) : base( schema ) {}

    /// <summary>
    /// Convert the contents of the record to a string.
    /// </summary>
    /// <returns>A string containing the field names and values.</returns>
    public string ContentsToString()
    {
        System.Text.StringBuilder sb = new System.Text.StringBuilder();
        sb.Append( "Contents: " );
        sb.Append( "{ " );
        object value;
        string value_string;
        foreach ( Avro.Field field in this.Schema.Fields )
        //foreach ( KeyValuePair<string, object> kv in this.contents )
        {
            // Append the field name
            sb.Append( field.Name );
            sb.Append( ": " );

            // Get the field value (handle nulls)
            this.TryGetValue( field.Name, out value );
            if ( value != null )
                value_string = $"{value}";
            else
                value_string = "<null>";

            // Append the field value
            sb.Append( value_string );
            sb.Append( ", " );
        }
        sb.Remove( (sb.Length - 2), 2 ); // remove the trailing comma and space
        sb.Append( " }" );
        return sb.ToString();
    }  // end ContentsToString


    /// <summary>
    /// Decodes binary encoded data of a dynamically created table returned by the server.
    /// </summary>
    /// <param name="dynamic_table_schema_string">The schema string for the dynamically created table.</param>
    /// <param name="encoded_data">The binary encoded data.</param>
    /// <returns>A list of KineticaRecord objects with the decoded data.</returns>
    public static IList<KineticaRecord> DecodeDynamicTableRecords( string dynamic_table_schema_string, byte[] encoded_data )
    {
        // Parse the outer schema
        Schema dynamic_table_schema;
        try
        {
            dynamic_table_schema = Avro.Schema.Parse( dynamic_table_schema_string );
        }
        catch ( Exception ex )
        {
            throw new KineticaException( ex.ToString() );
        }

        IList<KineticaRecord> records = new List<KineticaRecord>();

        using ( var ms = new MemoryStream( encoded_data ) )
        {
            // Decode the top-level GenericRecord which holds columns as arrays
            var reader  = new Avro.Generic.DefaultReader( dynamic_table_schema, dynamic_table_schema );
            var decoder = new BinaryDecoder( ms );
            var obj     = (Avro.Generic.GenericRecord) reader.Read( null, dynamic_table_schema, dynamic_table_schema, decoder );

            // Extract column headers and types
            if ( !obj.TryGetValue( "column_headers",    out object? column_headers_0 ) )
                throw new KineticaException( "Dynamic table schema is missing required field 'column_headers'." );
            if ( !obj.TryGetValue( "column_datatypes", out object? column_types_0 ) )
                throw new KineticaException( "Dynamic table schema is missing required field 'column_datatypes'." );

            object[] column_headers = (object[]) column_headers_0!;
            object[] column_types   = (object[]) column_types_0!;
            int      num_columns    = column_headers.Length;

            // Extract per-column data arrays (column-major layout)
            object[][] encoded_column_data = ExtractColumnData( obj, num_columns );

            // Validate that all columns have the same row count
            int num_records = encoded_column_data[0].Length;
            foreach ( object[] col in encoded_column_data )
                if ( col.Length != num_records )
                    throw new KineticaException( "Dynamic table has uneven column data lengths" );

            // Build KineticaType and RecordSchema from the dynamic schema
            KineticaType      dynamic_record_type = KineticaType.fromDynamicSchema( dynamic_table_schema_string, column_headers, column_types );
            Avro.RecordSchema record_schema        = (Avro.RecordSchema) dynamic_record_type.getSchema();

            // Transpose column-major → row-major and produce KineticaRecord instances
            TransposeToRecords( records, encoded_column_data, record_schema, num_records, num_columns );
        }

        return records;
    }  // end DecodeDynamicTableRecords

    /// <summary>
    /// Extracts the per-column data arrays out of the decoded master <see cref="Avro.Generic.GenericRecord"/>.
    /// Columns are stored as "column_1", "column_2", … in the record.
    /// </summary>
    private static object[][] ExtractColumnData( Avro.Generic.GenericRecord obj, int numColumns )
    {
        object[][] result = new object[numColumns][];
        for ( int i = 0; i < numColumns; ++i )
        {
            string columnKey = $"column_{i + 1}";
            if ( !obj.TryGetValue( columnKey, out object? column_data_0 ) || column_data_0 == null )
                throw new KineticaException( $"Dynamic table is missing expected column field '{columnKey}'." );
            result[i] = (object[]) column_data_0;
        }
        return result;
    }

    /// <summary>
    /// Transposes column-major <paramref name="columnData"/> into row-major
    /// <see cref="KineticaRecord"/> objects and appends them to <paramref name="records"/>.
    /// </summary>
    private static void TransposeToRecords(
        IList<KineticaRecord> records,
        object[][]            columnData,
        Avro.RecordSchema     schema,
        int                   numRecords,
        int                   numColumns )
    {
        for ( int record_idx = 0; record_idx < numRecords; ++record_idx )
        {
            var record = new KineticaRecord( schema );
            for ( int col_idx = 0; col_idx < numColumns; ++col_idx )
                record.Add( schema.Fields[col_idx].Name, columnData[col_idx][record_idx] );
            records.Add( record );
        }
    }



}  // end class KineticaRecord 
