using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Avro;
using Newtonsoft.Json.Linq;

namespace kinetica
{
    /// <summary>
    /// Describes the schema of a Kinetica table, including its columns and their
    /// data types and properties.  Instances are created via the static factory
    /// methods (<see cref="FromTable"/>, <see cref="FromTypeId"/>,
    /// <see cref="fromDynamicSchema"/>, <see cref="fromClass"/>, <see cref="fromObject"/>)
    /// or by passing a list of <see cref="Column"/> objects to a constructor.
    /// </summary>
    public sealed class KineticaType
    {
        /// <summary>
        /// Describes a single column in a Kinetica table schema: its name, data type,
        /// nullability, and optional column properties (e.g. <c>dict_encoding</c>,
        /// <c>nullable</c>).
        /// </summary>
        public sealed class Column
        {
            /// <summary>
            /// The Avro-mapped column data types supported by Kinetica.
            /// </summary>
            public enum ColumnType
            {
                /// <summary>Binary data (Avro <c>bytes</c>).</summary>
                BYTES = Schema.Type.Bytes,
                /// <summary>64-bit IEEE 754 floating-point (Avro <c>double</c>).</summary>
                DOUBLE = Schema.Type.Double,
                /// <summary>32-bit IEEE 754 floating-point (Avro <c>float</c>).</summary>
                FLOAT = Schema.Type.Float,
                /// <summary>32-bit signed integer (Avro <c>int</c>).</summary>
                INT = Schema.Type.Int,
                /// <summary>64-bit signed integer (Avro <c>long</c>).</summary>
                LONG = Schema.Type.Long,
                /// <summary>Unicode character string (Avro <c>string</c>).</summary>
                STRING = Schema.Type.String,
                /// <summary>Placeholder for unrecognised or unset column types. Not valid for use in a table schema.</summary>
                DEFAULT = Schema.Type.Error
            };

            private string _name;
            private ColumnType _type;
            private bool _isNullable;
            private IList<string> _properties;

            /// <summary>
            /// Creates a <see cref="Column"/> with the specified name, data type, and optional properties.
            /// </summary>
            /// <param name="name">The column name. Must not be null or empty.</param>
            /// <param name="type">The Avro-mapped column type. Must be one of
            /// <see cref="ColumnType.BYTES"/>, <see cref="ColumnType.DOUBLE"/>,
            /// <see cref="ColumnType.FLOAT"/>, <see cref="ColumnType.INT"/>,
            /// <see cref="ColumnType.LONG"/>, or <see cref="ColumnType.STRING"/>.</param>
            /// <param name="properties">Optional Kinetica column properties (e.g.
            /// <c>dict_encoding</c>, <c>nullable</c>). Pass <c>null</c> or omit for
            /// no properties. Individual property strings must not be null or empty.</param>
            /// <exception cref="ArgumentException">
            /// Thrown when <paramref name="name"/> is null or empty, when
            /// <paramref name="type"/> is <see cref="ColumnType.DEFAULT"/> (unsupported),
            /// or when any entry in <paramref name="properties"/> is null or empty.
            /// </exception>
            public Column(string name, ColumnType type, IList<string>? properties = null)
            {
                _name = name;
                _type = type;
                _isNullable = false;
                _properties = properties ?? [];

                Initialize();
            }

            /// <summary>Gets the name of the column.</summary>
            public string Name => _name;

            /// <inheritdoc cref="Name"/>
            [Obsolete("Use the Name property instead.")]
            public string getName() { return _name; }

            /// <summary>Gets the enumeration for the column type.</summary>
            public ColumnType Type => _type;

            /// <inheritdoc cref="Type"/>
            [Obsolete("Use the Type property instead.")]
            public ColumnType getType() { return _type; }

            /// <summary>Indicates whether the column is nullable.</summary>
            public bool IsNullable => _isNullable;

            /// <inheritdoc cref="IsNullable"/>
            [Obsolete("Use the IsNullable property instead.")]
            public bool isNullable() { return _isNullable; }

            /// <summary>Gets the properties for the column.</summary>
            public IReadOnlyList<string> Properties => (IReadOnlyList<string>)_properties;

            /// <inheritdoc cref="Properties"/>
            [Obsolete("Use the Properties property instead.")]
            public IList<string> getProperties() { return _properties; }

            internal void setIsNullable( bool val ) { _isNullable = val; }

            /// <summary>Gets the string format of the data type.</summary>
            public string TypeString => getTypeString();

            /// <inheritdoc cref="TypeString"/>
            [Obsolete("Use the TypeString property instead.")]
            public string getTypeString()
            {
                return _type switch
                {
                    ColumnType.BYTES => "bytes",
                    ColumnType.DOUBLE => "double",
                    ColumnType.FLOAT => "float",
                    ColumnType.INT => "int",
                    ColumnType.LONG => "long",
                    ColumnType.STRING => "string",
                    _ => throw new KineticaException("Unsupported column type: " + _type),
                };
            }  // end getTypeString()

            private void Initialize()
            {
                if (string.IsNullOrEmpty(_name))
                {
                    throw new ArgumentException("Name must not be empty.");
                }

                switch (_type)
                {
                    case ColumnType.BYTES:
                    case ColumnType.DOUBLE:
                    case ColumnType.FLOAT:
                    case ColumnType.INT:
                    case ColumnType.LONG:
                    case ColumnType.STRING:
                        break;

                    default:
                        throw new ArgumentException($"Column {_name} must be of type BYTES, DOUBLE, FLOAT, INT, LONG or STRING.");
                }

                foreach (var it in _properties)
                {
                    if (string.IsNullOrEmpty(it))
                    {
                        throw new ArgumentException("Properties must not be empty.");
                    }

                    if (!_isNullable && (it == ColumnProperty.NULLABLE))
                    {
                        _isNullable = true;
                    }
                }
            }

            /// <summary>
            /// Returns a short diagnostic string in the form <c>name (type)</c>.
            /// </summary>
            /// <returns>A string combining the column name and its <see cref="ColumnType"/>.</returns>
            public override string ToString()
            {
                return $"{_name} ({_type})";
            }
        }  // end class Column

        private class TypeData
        {
            public string? label;
            public IList<Column> columns = [];
            public Dictionary<string, int> columnMap = [];
            public string? schemaString = null;
            public Schema? schema = null;
            public Type? sourceType = null;
        }

        private TypeData _data = new();
        private IDictionary<string, IList<string>> _properties = new Dictionary<string, IList<string>>();
        private string? _typeId = null;

        /// <summary>
        /// Creates a <see cref="KineticaType"/> that matches the schema of an existing
        /// Kinetica table.
        /// </summary>
        /// <param name="kinetica">The connected <see cref="Kinetica"/> client.</param>
        /// <param name="tableName">The fully-qualified name of the table (e.g.
        /// <c>schema.table_name</c>).</param>
        /// <returns>A <see cref="KineticaType"/> populated with the table's column
        /// definitions, type label, schema, and type ID.</returns>
        /// <exception cref="KineticaException">
        /// Thrown when the table does not exist or when the table contains records
        /// of more than one type (heterogeneous table).
        /// </exception>
        public static KineticaType FromTable(Kinetica kinetica, string tableName)
            => fromTable(kinetica, tableName);

        /// <inheritdoc cref="FromTable"/>
        [Obsolete("Use FromTable instead.")]
        public static KineticaType fromTable(Kinetica kinetica, string tableName)
        {
            var response = kinetica.showTable(tableName);
            var typeIdCount = response.type_ids.Count;

            if (typeIdCount == 0)
            {
                throw new KineticaException($"Table {tableName} does not exist.");
            }

            string typeId = response.type_ids[0];
            if (typeIdCount > 1)
            {
                for (int i = 1; i < typeIdCount; ++i)
                {
                    if (response.type_ids[i] != typeId)
                    {
                        throw new KineticaException($"Table {tableName} is not homogeneous.");
                    }
                }
            }

            return new KineticaType(response.type_labels[0], response.type_schemas[0], response.properties[0], typeId );
        }

        /// <summary>
        /// Creates a <see cref="KineticaType"/> from a type ID that already exists in
        /// the Kinetica type catalogue.
        /// </summary>
        /// <param name="kinetica">The connected <see cref="Kinetica"/> client.</param>
        /// <param name="typeId">The server-assigned type ID string.</param>
        /// <returns>A <see cref="KineticaType"/> populated from the server's type
        /// definition for the given ID.</returns>
        /// <exception cref="KineticaException">
        /// Thrown when no type with the given <paramref name="typeId"/> exists in
        /// the Kinetica catalogue.
        /// </exception>
        public static KineticaType FromTypeId(Kinetica kinetica, string typeId)
            => fromTypeID(kinetica, typeId);

        /// <inheritdoc cref="FromTypeId"/>
        [Obsolete("Use FromTypeId instead.")]
        public static KineticaType fromTypeID(Kinetica kinetica, string typeId)
        {
            var response = kinetica.showTypes(typeId, "");

            if (response.type_ids.Count < 1)
            {
                throw new KineticaException($"Type {typeId} does not exist.");
            }

            return new KineticaType(response.labels[0], response.type_schemas[0], response.properties[0]);
        }

        /// <summary>
        /// Async overload of <see cref="FromTable"/>. Cancellable, non-blocking.
        /// </summary>
        /// <param name="kinetica">The connected <see cref="Kinetica"/> client.</param>
        /// <param name="tableName">The fully-qualified name of the table.</param>
        /// <param name="cancellationToken">Token to cancel the operation.</param>
        /// <returns>A <see cref="KineticaType"/> populated with the table's column
        /// definitions, type label, schema, and type ID.</returns>
        /// <exception cref="KineticaException">
        /// Thrown when the table does not exist or contains records of more than
        /// one type.
        /// </exception>
        /// <exception cref="OperationCanceledException">
        /// Thrown when <paramref name="cancellationToken"/> is cancelled before
        /// the server responds.
        /// </exception>
        public static async Task<KineticaType> FromTableAsync(
            Kinetica kinetica,
            string tableName,
            CancellationToken cancellationToken = default)
        {
            var response = await kinetica
                .SubmitRequestAsync<ShowTableResponse>("/show/table", new ShowTableRequest(tableName),
                    cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            var typeIdCount = response.type_ids.Count;
            if (typeIdCount == 0)
                throw new KineticaException($"Table {tableName} does not exist.");

            string typeId = response.type_ids[0];
            if (typeIdCount > 1)
            {
                for (int i = 1; i < typeIdCount; ++i)
                {
                    if (response.type_ids[i] != typeId)
                        throw new KineticaException($"Table {tableName} is not homogeneous.");
                }
            }

            return new KineticaType(response.type_labels[0], response.type_schemas[0], response.properties[0], typeId);
        }

        /// <summary>
        /// Async overload of <see cref="FromTypeId"/>. Cancellable, non-blocking.
        /// </summary>
        /// <param name="kinetica">The connected <see cref="Kinetica"/> client.</param>
        /// <param name="typeId">The server-assigned type ID string.</param>
        /// <param name="cancellationToken">Token to cancel the operation.</param>
        /// <returns>A <see cref="KineticaType"/> populated from the server's type
        /// definition for the given ID.</returns>
        /// <exception cref="KineticaException">
        /// Thrown when no type with the given <paramref name="typeId"/> exists in
        /// the Kinetica catalogue.
        /// </exception>
        /// <exception cref="OperationCanceledException">
        /// Thrown when <paramref name="cancellationToken"/> is cancelled before
        /// the server responds.
        /// </exception>
        public static async Task<KineticaType> FromTypeIDAsync(
            Kinetica kinetica,
            string typeId,
            CancellationToken cancellationToken = default)
        {
            var response = await kinetica
                .SubmitRequestAsync<ShowTypesResponse>("/show/types", new ShowTypesRequest(typeId, ""),
                    cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            if (response.type_ids.Count < 1)
                throw new KineticaException($"Type {typeId} does not exist.");

            return new KineticaType(response.labels[0], response.type_schemas[0], response.properties[0]);
        }


        /// <summary>Create a <see cref="KineticaType"/> from a dynamic schema string.</summary>
        public static KineticaType FromDynamicSchema(string dynamicTableSchemaString, object[] columnHeaders, object[] columnTypes)
            => fromDynamicSchema(dynamicTableSchemaString, columnHeaders, columnTypes);

        /// <summary>Create a <see cref="KineticaType"/> based on information provided in a dynamic schema.</summary>
        [Obsolete("Use FromDynamicSchema instead.")]
        public static KineticaType fromDynamicSchema( string dynamicTableSchemaString,
                                                      object[] columnHeaders, object[] columnTypes )
        {
            // Make sure that the lists of column names and types are of the same length
            if ( columnHeaders.Length != columnTypes.Length )
                throw new KineticaException($"List of column names ({columnHeaders.Length}) and types ({columnTypes.Length}) are not of the same length." );

            // Parse the schema string so that we can later check if a given column is nullable
            JObject dynamicSchemaJson;
            try
            {
                dynamicSchemaJson = JObject.Parse( dynamicTableSchemaString );
            }
            catch ( Exception ex )
            {
                throw new KineticaException( ex.ToString() );
            }

            // Create appropriate columns and column properties
            // ------------------------------------------------
            List<Column> columns = [];
            Dictionary<string, IList<string>> columnProperties = [];
            for ( int i = 0; i < columnHeaders.Length; ++i )
            {
                string column_name = (string)columnHeaders[i];
                string columnTypeString = (string)columnTypes[i];

                // Infer the primitive ColumnType and optional sub-property from the type string
                (Column.ColumnType columnType, string? subProperty) = InferColumnTypeAndProperty( columnTypeString );
                List<string> columnProperty = subProperty != null ? [subProperty] : [];

                // Check if the column is nullable (where the column name is "column_#" as returned by Kinetica)
                if ( IsColumnNullable( $"column_{i + 1}", dynamicSchemaJson ) )
                    columnProperty.Add( ColumnProperty.NULLABLE );

                Column column = new( column_name, columnType, columnProperty );
                columns.Add( column );
                columnProperties.Add( column_name, columnProperty );
            }

            return new KineticaType( "", columns, columnProperties );

            // Static local: checks whether a named field is declared nullable in the dynamic schema JSON.
            // Captures nothing — allocated once, not per-call.
            static bool IsColumnNullable( string columnName, JObject schemaJson )
            {
                JToken? fieldList = schemaJson["fields"];
                if ( fieldList == null ) return false;

                foreach ( var field in fieldList )
                {
                    if ( (string?)field["name"] != columnName ) continue;

                    JToken? fieldType = field["type"];
                    if ( fieldType == null ) return false;

                    var typeElement = fieldType["items"];
                    if ( typeElement == null || typeElement is JValue )
                        return false;  // not an array-wrapped type
                    if ( typeElement is JArray arr && (string?)arr[1] == "null" )
                        return true;
                    return false;
                }

                throw new KineticaException( $"Could not find the field named '{columnName}'" );
            }
        }  // end fromDynamicSchema()

        /// <summary>
        /// Maps a Kinetica dynamic-schema type string to its canonical <see cref="Column.ColumnType"/>
        /// and an optional sub-property string.  Each branch is a single concern; adding new
        /// property strings requires only extending this method, not the caller.
        /// </summary>
        private static (Column.ColumnType columnType, string? subProperty) InferColumnTypeAndProperty(
            string columnTypeString )
        {
            switch ( columnTypeString )
            {
                case "string":                 return (Column.ColumnType.STRING, null);
                case ColumnProperty.CHAR1:
                case ColumnProperty.CHAR2:
                case ColumnProperty.CHAR4:
                case ColumnProperty.CHAR8:
                case ColumnProperty.CHAR16:
                case ColumnProperty.CHAR32:
                case ColumnProperty.CHAR64:
                case ColumnProperty.CHAR128:
                case ColumnProperty.CHAR256:
                case ColumnProperty.DATE:
                case ColumnProperty.DATETIME:
                case ColumnProperty.DECIMAL:
                case ColumnProperty.IPV4:
                case ColumnProperty.TIME:      return (Column.ColumnType.STRING, columnTypeString);

                case "int":                    return (Column.ColumnType.INT,    null);
                case ColumnProperty.INT8:
                case ColumnProperty.INT16:     return (Column.ColumnType.INT,    columnTypeString);

                case "long":                   return (Column.ColumnType.LONG,   null);
                case ColumnProperty.TIMESTAMP: return (Column.ColumnType.LONG,   columnTypeString);

                case "float":                  return (Column.ColumnType.FLOAT,  null);
                case "double":                 return (Column.ColumnType.DOUBLE, null);
                case "bytes":                  return (Column.ColumnType.BYTES,  null);

                default:
                    throw new KineticaException( $"Unknown data type/property: {columnTypeString}" );
            }
        }

        /// <summary>
        /// Create a KineticaType object from properties of a record class and Kinetica column properties.
        /// It ignores any properties inherited from base classes, and also ignores any member fields of
        /// the class.
        /// 
        /// For integer, long, float, and double column types, the user can use the nullable type (e.g. int?)
        /// to declare the column to be nullable.  The <paramref name="properties"/> does not need to contain
        /// the <see cref="ColumnProperty.NULLABLE"/> property.  However, for string type columns, instead of
        /// using nullable type, use the regular string type; additionally, add the
        /// <see cref="ColumnProperty.NULLABLE"/> in <paramref name="properties"/>.
        /// 
        /// </summary>
        /// <param name="recordClass">A class type.</param>
        /// <param name="properties">Properties for the columns.</param>
        /// <returns>A <see cref="KineticaType"/> whose columns mirror the public
        /// properties of <paramref name="recordClass"/>, with types and nullability
        /// inferred from the CLR property types.</returns>
        public static KineticaType fromClass( Type recordClass, IDictionary<string, IList<string>> properties = null )
        {
            return fromClass( recordClass, "", properties );
        }   // end fromClass()


        /// <summary>
        /// Create a KineticaType object from properties of a record class and Kinetica column properties.
        /// It ignores any properties inherited from base classes, and also ignores any member fields of
        /// the class.
        /// </summary>
        /// <param name="recordClass">A class type.</param>
        /// <param name="label">Any label for the type.</param>
        /// <param name="properties">Properties for the columns.</param>
        /// <returns>A <see cref="KineticaType"/> whose columns mirror the public
        /// properties of <paramref name="recordClass"/>, labeled with
        /// <paramref name="label"/>.</returns>
        public static KineticaType fromClass( Type recordClass, string label, IDictionary<string, IList<string>>? properties = null )
        {
            // Get the fields in order (******skipping properties inherited from base classes******)
            // (fields only from this type, i.e. do not include any inherited fields), and public types only
            System.Reflection.PropertyInfo[] typeProperties = recordClass.GetProperties( System.Reflection.BindingFlags.DeclaredOnly |
                                                                                          System.Reflection.BindingFlags.Instance |
                                                                                          System.Reflection.BindingFlags.Public );

            Array.Sort( typeProperties, (p1, p2) => p1.MetadataToken.CompareTo( p2.MetadataToken ) );

            // Need to have a list of columns
            List<Column> columns = [];
            List<string> columnNames = [];

            // Per property, check that it is one of: int, long, float, double, string, bytes
            foreach ( var typeProperty in typeProperties )
            {
                string            columnName = "";
                Column.ColumnType columnType = Column.ColumnType.DEFAULT;
                IList<string>?    columnProperties = null;
                bool              isColumnNullable = false;

                // Get the column name
                columnName = typeProperty.Name;

                Type? propertyType = typeProperty.PropertyType;

                // Check if the field is nullable (declared as T? or Nullable<T>)
                if ( typeProperty.PropertyType.IsGenericType &&
                          ( typeProperty.PropertyType.GetGenericTypeDefinition() == typeof( Nullable<> ) ) )
                {   // the field is a nullable field
                    isColumnNullable = true;
                    // Change the property type to be the underlying type
                    propertyType = Nullable.GetUnderlyingType(propertyType);
                }

                // Check the column data type (must be one of int, long, float, double, string, and bytes)
                if ( propertyType == typeof( System.String ) )
                {
                    columnType = Column.ColumnType.STRING;
                }
                else if ( propertyType == typeof( System.Int32 ) )
                {
                    columnType = Column.ColumnType.INT;
                }
                else if ( propertyType == typeof( System.Int64 ) )
                {
                    columnType = Column.ColumnType.LONG;
                }
                else if ( propertyType == typeof( float ) )
                {
                    columnType = Column.ColumnType.FLOAT;
                }
                else if ( propertyType == typeof( double ) )
                {
                    columnType = Column.ColumnType.DOUBLE;
                }
                else if ( propertyType == typeof( byte ) )
                {
                    columnType = Column.ColumnType.BYTES;
                }
                else
                    throw new KineticaException( "Unsupported data type for " + propertyType?.Name +
                                              ": " + propertyType +
                                              " (must be one of int, long, float, double, string, and byte)" );

                // Extract the given column's properties, if any
                properties?.TryGetValue(columnName, out columnProperties);

                // Keep a list of the column names for checking the properties
                columnNames.Add( columnName );

                // Create the column
                Column column = new( columnName, columnType, columnProperties );
                if ( isColumnNullable ) // Set the appropriate nullable flag for the column
                    column.setIsNullable( true );

                // Save the column
                columns.Add( column );
            }  // end looping over all members of the class type

            // Check for extraneous properties
            if (properties != null)
            {
                IEnumerable<string> propertyKeys = properties.Keys;
                var unknownColumns = propertyKeys.Where(e => !columnNames.Contains(e));
                // Check if any property is provided for wrong/non-existing columns
                if (unknownColumns.Any())
                    throw new KineticaException("Properties specified for unknown columns.");
            }
            else properties = new Dictionary<string, IList<string>>();

            // Create the kinetica type
            KineticaType kType = new(label, columns, properties);

            // Save the class information in the type
            kType.saveSourceType( recordClass );

            return kType;
        } // end fromClass()


        /// <summary>
        /// Create a KineticaType object from properties of a record object and Kinetica column properties.
        /// It ignores any properties inherited from base classes, and also ignores any member fields of
        /// the class.
        /// </summary>
        /// <param name="recordObj">A record object.</param>
        /// <param name="properties">Properties for the columns.</param>
        /// <returns>A <see cref="KineticaType"/> whose columns mirror the public
        /// properties of <paramref name="recordObj"/>'s runtime type.</returns>
        public static KineticaType fromObject( Object recordObj, IDictionary<string, IList<string>> properties = null )
        {
            return fromObject( recordObj, "", properties );
        }  // end fromObject()


        /// <summary>
        /// Create a KineticaType object from properties of a record object and Kinetica column properties.
        /// It ignores any properties inherited from base classes, and also ignores any member fields of
        /// the class.
        /// </summary>
        /// <param name="recordObj">A record object.</param>
        /// <param name="label">Any label for the type.</param>
        /// <param name="properties">Properties for the columns.</param>
        /// <returns>A <see cref="KineticaType"/> whose columns mirror the public
        /// properties of <paramref name="recordObj"/>'s runtime type, labeled
        /// with <paramref name="label"/>.</returns>
        public static KineticaType fromObject(Object recordObj, string label = "", IDictionary<string, IList<string>> properties = null)
        {
            // Create the type schema from the object
            // --------------------------------------
            // Get the class type
            Type object_type = recordObj.GetType();

            return fromClass( object_type, label, properties );
        }  // end fromObject()

        /// <summary>
        /// Create a KineticaType object with the given column information.
        /// </summary>
        /// <param name="columns">A list of Columns with information on all the columns for the type.</param>
        public KineticaType(IList<Column> columns)
        {
            _data.columns = columns;
            Initialize();
            CreateSchema();  // create the schema from columns
        }

        /// <summary>
        /// Create a KineticaType object with the given column and label information.
        /// </summary>
        /// <param name="label">The label for the type.</param>
        /// <param name="columns">A list of Columns with information on all the columns for the type.</param>
        public KineticaType(string label, IList<Column> columns) : this(columns)
        {
            _data.label = label;
        }

        /// <summary>
        /// Create a KineticaType object with the given column, label, and property information.
        /// </summary>
        /// <param name="label">The label for the type.</param>
        /// <param name="columns">A list of Columns with information on all the columns for the type.</param>
        /// <param name="properties">A per-column property information</param>
        public KineticaType( string label, IList<Column> columns, IDictionary<string, IList<string>> properties ) : this( label, columns )
        {
            _properties = properties ?? new Dictionary<string, IList<string>>();
        }

        /// <summary>
        /// Create a KineticaType object using the string-formatted schema for the type.
        /// </summary>
        /// <param name="typeSchema"></param>
        public KineticaType(string typeSchema)
        {
            _data.schemaString = typeSchema;
            CreateSchemaFromString( typeSchema );
            CreateSchema();
        }

        /// <summary>
        /// Create a KineticaType object using the string-formatted schema and properties for its columns.
        /// </summary>
        /// <param name="label">The label for the type.</param>
        /// <param name="typeSchema">The string-formatted schema for the type.</param>
        /// <param name="properties">A per-column based set of properties.</param>
        /// <param name="typeId">An optional ID for this type with which to identify it in the database.</param>
        public KineticaType(string label, string typeSchema, IDictionary<string, IList<string>> properties, string? typeId = null )
        {
            _properties = properties;
            _typeId = typeId;
            _data.label = label;
            _data.schemaString = typeSchema;
            CreateSchemaFromString(typeSchema, properties);
            CreateSchema();
        }

        /// <summary>Gets the label of this type, or <c>null</c> if no label was assigned.</summary>
        public string getLabel() { return _data.label; }

        /// <summary>Gets the list of columns in this type.</summary>
        public IReadOnlyList<Column> Columns => _data.columns.AsReadOnly();

        /// <summary>Gets the number of columns in this type.</summary>
        public int ColumnCount => _data.columns.Count;

        /// <summary>Gets the type ID registered in the database, or null if not yet registered.</summary>
        public string? TypeId => _typeId;

        /// <summary>Gets the Avro schema for this type.</summary>
        public Schema? AvroSchema => _data.schema;

        /// <summary>Gets the string representation of the Avro schema.</summary>
        public string? SchemaString => _data.schemaString;

        /// <summary>Returns true if this type contains a column with the given name.</summary>
        public bool ContainsColumn(string name) => _data.columnMap.ContainsKey(name);

        /// <inheritdoc cref="Columns"/>
        [Obsolete("Use Columns property instead.")]
        public IList<Column> getColumns() { return _data.columns; }

        /// <summary>Gets the column at the specified zero-based index.</summary>
        /// <param name="index">Zero-based column index.</param>
        /// <returns>The <see cref="Column"/> at position <paramref name="index"/>.</returns>
        public Column getColumn(int index) { return _data.columns[index]; }

        /// <summary>Gets the column with the specified name.</summary>
        /// <param name="name">The column name (case-sensitive).</param>
        /// <returns>The matching <see cref="Column"/>.</returns>
        public Column getColumn(string name) { return _data.columns[getColumnIndex(name)]; }

        /// <inheritdoc cref="ColumnCount"/>
        [Obsolete("Use ColumnCount property instead.")]
        public int getColumnCount() { return _data.columns.Count; }

        /// <summary>Returns the zero-based index of the column with the given name.</summary>
        /// <param name="name">The column name (case-sensitive).</param>
        /// <returns>Zero-based column index.</returns>
        public int getColumnIndex(string name) { return _data.columnMap[name]; }

        /// <inheritdoc cref="ContainsColumn"/>
        [Obsolete("Use ContainsColumn instead.")]
        public bool hasColumn(string name) { return _data.columnMap.ContainsKey(name); }

        /// <inheritdoc cref="AvroSchema"/>
        [Obsolete("Use AvroSchema property instead.")]
        public Schema getSchema() { return _data.schema; }

        /// <summary>Returns the CLR <see cref="System.Type"/> registered as the source type for
        /// this schema, or <c>null</c> if no source type has been registered.</summary>
        public Type? getSourceType() { return _data.sourceType; }

        /// <inheritdoc cref="SchemaString"/>
        [Obsolete("Use SchemaString property instead.")]
        public string getSchemaString() { return _data.schemaString; }

        /// <inheritdoc cref="TypeId"/>
        [Obsolete("Use TypeId property instead.")]
        public string getTypeID() { return _typeId; }

        /// <summary>
        /// Saves the given type as this KineticaType's source type.
        /// </summary>
        /// <param name="sourceType">The type that works as the source. </param>
        public void saveSourceType( Type sourceType )
        {
            this._data.sourceType = sourceType;
        }  // end saveSourceType


        /// <summary>
        /// Given a handle to the server, creates a type in the database based
        /// on this data type.
        /// </summary>
        /// <param name="kinetica">The handle to the database server.</param>
        /// <returns>The ID with which the database identifies this type.</returns>
        public string create(Kinetica kinetica)
        {
            // Save the association between this KineticaType's source and itself in the Kinetica object
            // for future reference (it helps with encoding and decoding records)
            if ( this._data.sourceType != null )
                kinetica.SetKineticaSourceClassToTypeMapping( this._data.sourceType, this );

            // Register the type with Kinetica
            CreateTypeResponse response = kinetica.createType( _data.schemaString, _data.label, _properties);
            return response.type_id;
        }  // end create()

        private KineticaType() { }

        /// <summary>
        /// Initializes the type based on the columns.  Verifies that the columns
        /// are valid.
        /// </summary>
        private void Initialize()
        {
            int columnCount = _data.columns.Count;

            if (columnCount == 0)
            {
                throw new ArgumentException("At least one column must be specified.");
            }

            for (int i = 0; i < columnCount; ++i)
            {
                string columnName = _data.columns[i].Name;

                if (_data.columnMap.ContainsKey(columnName))
                {
                    throw new ArgumentException("Duplicate column name " + columnName + " specified.");
                }

                _data.columnMap[columnName] = i;
            }
        }  // end Initialize()


        /// <summary>
        /// Creates a schema object from a string.
        /// </summary>
        /// <param name="typeSchema">The schema in a string format.</param>
        /// <param name="properties">Properties for the columns.</param>
        private void CreateSchemaFromString( string typeSchema,
                                             IDictionary<string, IList<string>> properties = null)
        {
            // Create the avro schema from the string and save it
            try
            {
                _data.schema = RecordSchema.Parse(typeSchema);
            }
            catch (Exception ex)
            {
                throw new KineticaException(ex.ToString());
            }

            var root = JObject.Parse(typeSchema);

            var rootType = root["type"];
            if ((null == rootType) || !rootType.ToString().Contains("record"))
            {
                throw new ArgumentException("Schema must be of type record.");
            }

            var fields = root["fields"];
            if ((null == fields) || !fields.HasValues)
            {
                throw new ArgumentException("Schema has no fields.");
            }

            foreach (var field in fields)
            {
                //if (!field->first.empty() || field->second.empty())
                //{
                //    throw std::invalid_argument("Schema has invalid field.");
                //}

                // Do NOT use ToString 'cause it includes the double quotes (turns it into a JSON representation)
                string? fieldName = (string?)field["name"];
                if (string.IsNullOrEmpty(fieldName))
                {
                    throw new ArgumentException("Schema has unnamed field.");
                }

                if (_data.columnMap.ContainsKey(fieldName))
                {
                    throw new ArgumentException($"Duplicate field name {fieldName}.");
                }

                var fieldType = field["type"];
                if (null == fieldType)
                {
                    throw new ArgumentException($"Field {fieldName} has no type.");
                }

                // Flag for nullability
                bool isColumnNullable = false;

                if (fieldType.HasValues) // If it has children
                {
                    var fieldTypeArray = fieldType;

                    foreach (var fieldTypeElement in fieldTypeArray.Children())
                    {
                        bool valid = false;
                        //if (fieldTypeElement->first.empty())
                        {
                            var fieldTypeElementString = fieldTypeElement.ToString();

                            if (!string.IsNullOrEmpty(fieldTypeElementString))
                            {
                                if (fieldTypeElementString == "null" || fieldTypeElementString == "\"null\"")
                                {
                                    isColumnNullable = true;
                                    valid = true;
                                }
                                else //if (fieldType->empty())
                                {
                                    fieldType = fieldTypeElement; // fieldTypeElementString;
                                    valid = true;
                                }
                            }
                        }

                        if (!valid)
                        {
                            throw new ArgumentException("Field {fieldName} has invalid type.");
                        }
                    }
                }

                Column.ColumnType columnType;

                if (fieldType.ToString().Equals("bytes") || fieldType.ToString().Equals("\"bytes\""))
                {
                    columnType = Column.ColumnType.BYTES;
                }
                else if (fieldType.ToString().Equals("double") || fieldType.ToString().Equals("\"double\""))
                {
                    columnType = Column.ColumnType.DOUBLE;
                }
                else if (fieldType.ToString().Equals("float") || fieldType.ToString().Equals("\"float\""))
                {
                    columnType = Column.ColumnType.FLOAT;
                }
                else if (fieldType.ToString().Equals("int") || fieldType.ToString().Equals("\"int\""))
                {
                    columnType = Column.ColumnType.INT;
                }
                else if (fieldType.ToString().Equals("long") || fieldType.ToString().Equals("\"long\""))
                {
                    columnType = Column.ColumnType.LONG;
                }
                else if (fieldType.ToString().Equals("string") || fieldType.ToString().Equals("\"string\""))
                {
                    columnType = Column.ColumnType.STRING;
                }
                else
                {
                    throw new ArgumentException("Field {fieldName} must be of type bytes, double, float, int, long or string.");
                }

                IList<string>? columnProperties = null;
                properties?.TryGetValue(fieldName, out columnProperties);
                // Check the column properties for nullability
                if ( ( null != columnProperties ) &&
                     ( columnProperties.Contains( ColumnProperty.NULLABLE ) ) )
                    isColumnNullable = true;

                // Create the column to be added
                Column column = new( fieldName, columnType, columnProperties );

                column.setIsNullable( isColumnNullable );

                _data.columns.Add( column );

                _data.columnMap[fieldName] = _data.columns.Count - 1;
            }
        }  // end CreateSchemaFromString()

        /// <summary>
        /// Create an avro schema from either the columns or the schema string.
        /// </summary>
        private void CreateSchema()
        {
            // First, check if the schema has already been created
            if (_data.schema != null)
            {
                // nothing to do
                return;
            }

            // Check if the schema string exists, if so, create the schema from that
            if (_data.schemaString != null)
            {
                try
                {
                    _data.schema = RecordSchema.Parse(_data.schemaString);
                    return;
                }
                catch (Exception ex)
                {
                    throw new KineticaException(ex.ToString());
                }
            }  // done creating the schema from the schema string

            // Since the shortcuts didn't apply, create a JSON object from the columns
            // and then create the schema and the schema string off it
            // --------------------------------------------------------------------------
            // Create the json string for the type
            string schemaString = "";
            // Create the json string opening with empty fields (with a generic 'type_name' (because the
            // server always replaces the name with this string anyway) )
            string schemaOpening = "{'type':'record','name':'type_name','fields':[";
            // Create the json string closing
            string schemaClosing = "]}";

            schemaString += schemaOpening;

            // Create the json substrings for the columns
            foreach (var column in _data.columns)
            {
                // Add the name
                string fieldName = ("'name':'" + column.Name + "'");

                // Add the type
                string fieldType = "";
                if (column.IsNullable)
                {  // the column is nullable, so we need a union
                    fieldType = ("['" + column.TypeString + "','null']");
                }
                else  // regular type, no union needed
                {
                    fieldType = ( "'" + column.TypeString + "'" );
                }
                fieldType = ("'type':" + fieldType);

                // Put the field together
                string field = ("{" + fieldName + "," + fieldType + "},");
                schemaString += field;
            }  // end looping over the fields

            // Trim the trailing comma from the fields
            char[] comma = [','];
            schemaString = schemaString.TrimEnd(comma);
            // Add the ending of the json string
            schemaString += schemaClosing;

            // Create the RecordSchema from the JSON string
            try
            {
                _data.schema = RecordSchema.Parse(schemaString);
            }
            catch (Exception ex)
            {
                throw new KineticaException(ex.ToString());
            }

            // Save the schema string
            _data.schemaString = _data.schema.ToString();
            return;
        }  // end CreateSchema()
    }  // end class KineticaType
}  // end namespace kinetica
