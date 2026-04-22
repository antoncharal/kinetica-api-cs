using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace kinetica.Internal
{
    /// <summary>
    /// Maintains the three thread-safe lookup tables that map Avro type IDs, type labels,
    /// and CLR types to <see cref="KineticaType"/> descriptors.
    /// <para>
    /// Extracted from <see cref="Kinetica"/> to satisfy the Single Responsibility Principle:
    /// the registry owns the type-management state while <see cref="Kinetica"/> owns
    /// server communication and Avro encoding.
    /// </para>
    /// </summary>
    internal sealed class KineticaTypeRegistry
    {
        // -----------------------------------------------------------------------
        // State
        // -----------------------------------------------------------------------

        /// <summary>Avro type-ID  →  KineticaType descriptor.</summary>
        private readonly ConcurrentDictionary<string, KineticaType> _knownTypes = new();

        /// <summary>Type label (human-readable name)  →  Avro type-ID.</summary>
        private readonly ConcurrentDictionary<string, string> _typeNameLookup = new();

        /// <summary>CLR Type  →  KineticaType descriptor (registered by the caller).</summary>
        private readonly ConcurrentDictionary<Type, KineticaType> _kineticaTypeLookup = new();

        // -----------------------------------------------------------------------
        // Mutation helpers
        // -----------------------------------------------------------------------

        /// <summary>
        /// Registers a <paramref name="ktype"/> by its Avro type ID.
        /// If a mapping for that ID already exists it is left unchanged
        /// (<see cref="ConcurrentDictionary{TKey,TValue}.TryAdd"/> semantics).
        /// </summary>
        internal bool TryAddByTypeId( string typeId, KineticaType ktype ) =>
            _knownTypes.TryAdd( typeId, ktype );

        /// <summary>
        /// Ensures a <see cref="KineticaType"/> is registered for <paramref name="typeId"/>
        /// (adds if absent) and records the label → ID mapping.
        /// Intended for use by <see cref="Kinetica"/>'s decode-path.
        /// </summary>
        internal void RegisterIfAbsent(
            string typeId,
            string label,
            string schemaString,
            IDictionary<string, IList<string>> properties )
        {
            _knownTypes.GetOrAdd( typeId, _ => new KineticaType( label, schemaString, properties ) );
            _typeNameLookup[label] = typeId;
        }

        /// <summary>
        /// Associates a CLR <paramref name="objectType"/> with a
        /// <paramref name="kineticaType"/> for subsequent lookup.
        /// </summary>
        internal void MapObjectTypeToKineticaType( Type objectType, KineticaType kineticaType )
            => _kineticaTypeLookup[objectType] = kineticaType;

        // -----------------------------------------------------------------------
        // Query helpers
        // -----------------------------------------------------------------------

        /// <summary>
        /// Looks up a <see cref="KineticaType"/> by its Avro type ID.
        /// Returns <c>false</c> when no mapping exists.
        /// </summary>
        internal bool TryGetByTypeId( string typeId, out KineticaType? ktype ) =>
            _knownTypes.TryGetValue( typeId, out ktype );

        /// <summary>
        /// Looks up a <see cref="KineticaType"/> by its human-readable label.
        /// Returns <c>null</c> when no mapping exists.
        /// </summary>
        internal KineticaType? FindByLabel( string label )
        {
            if ( !_typeNameLookup.TryGetValue( label, out string? typeId ) )
                return null;
            _knownTypes.TryGetValue( typeId, out KineticaType? ktype );
            return ktype;
        }

        /// <summary>
        /// Looks up the <see cref="KineticaType"/> associated with a CLR type.
        /// Returns <c>null</c> when no mapping has been registered.
        /// </summary>
        internal KineticaType? FindByObjectType( Type objectType )
        {
            _kineticaTypeLookup.TryGetValue( objectType, out KineticaType? ktype );
            return ktype;
        }
    }
}
