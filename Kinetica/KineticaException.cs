using System;
using System.Runtime.Serialization;


namespace kinetica
{
    /// <summary>
    /// Exception raised by the Kinetica C# client for both server-side errors
    /// (where the server returns a non-success HTTP status or an error body) and
    /// client-side failures (serialization errors, network disconnects, etc.).
    /// </summary>
    /// <remarks>
    /// When the error originates from a server response the <see cref="StatusCode"/>
    /// property contains the HTTP status code returned by Kinetica.  For locally
    /// detected failures (e.g. type-registration errors, argument validation)
    /// <see cref="StatusCode"/> is <c>null</c>.
    /// </remarks>
    [Serializable()]
    public class KineticaException : System.Exception
    {
        /// <summary>
        /// HTTP status code returned by the Kinetica server, or <c>null</c>
        /// for client-side failures (serialization, network disconnect, etc.).
        /// </summary>
        public int? StatusCode { get; }

        /// <summary>
        /// Initialises a <see cref="KineticaException"/> with no message, no inner
        /// exception, and no status code.  Prefer one of the overloads that carry a
        /// descriptive message.
        /// </summary>
        public KineticaException() { }

        /// <summary>
        /// Initialises a <see cref="KineticaException"/> with the specified error message.
        /// </summary>
        /// <param name="msg">A human-readable description of the error.</param>
        public KineticaException(string msg) : base ( msg ) { }

        /// <summary>
        /// Initialises a <see cref="KineticaException"/> with the specified error message
        /// and the exception that caused this one.
        /// </summary>
        /// <param name="msg">A human-readable description of the error.</param>
        /// <param name="innerException">The exception that is the cause of this exception.</param>
        public KineticaException( string msg, Exception innerException ) :
            base( msg, innerException ) { }

        /// <summary>
        /// Initialises a <see cref="KineticaException"/> with an HTTP status code and no inner exception.
        /// </summary>
        /// <param name="msg">A human-readable description of the error.</param>
        /// <param name="statusCode">The HTTP status code returned by the Kinetica server,
        /// or <c>null</c> for client-side failures.</param>
        public KineticaException(string msg, int? statusCode) : base(msg)
        {
            StatusCode = statusCode;
        }

        /// <summary>
        /// Initialises a <see cref="KineticaException"/> with an HTTP status code and
        /// the exception that caused this one.
        /// </summary>
        /// <param name="msg">A human-readable description of the error.</param>
        /// <param name="statusCode">The HTTP status code returned by the Kinetica server,
        /// or <c>null</c> for client-side failures.</param>
        /// <param name="innerException">The exception that is the cause of this exception,
        /// or <c>null</c> if no inner exception is available.</param>
        public KineticaException(string msg, int? statusCode, Exception? innerException)
            : base(msg, innerException)
        {
            StatusCode = statusCode;
        }

        /// <summary>
        /// Deserialisation constructor required by <see cref="ISerializable"/>.
        /// Restores a <see cref="KineticaException"/> from serialised state.
        /// </summary>
        /// <param name="info">The <see cref="SerializationInfo"/> that holds the
        /// serialised object data.</param>
        /// <param name="context">The <see cref="StreamingContext"/> that contains
        /// contextual information about the source or destination.</param>
        protected KineticaException( SerializationInfo info, StreamingContext context )
            : base ( info, context )
        {
            StatusCode = info.GetBoolean("HasStatusCode") ? info.GetInt32("StatusCode") : null;
        }

        /// <summary>
        /// Populates a <see cref="SerializationInfo"/> with the data needed to
        /// serialise this exception, including <see cref="StatusCode"/>.
        /// </summary>
        /// <param name="info">The <see cref="SerializationInfo"/> to populate.</param>
        /// <param name="context">The <see cref="StreamingContext"/> for the serialisation.</param>
        public override void GetObjectData( SerializationInfo info, StreamingContext context )
        {
            base.GetObjectData( info, context );
            info.AddValue("HasStatusCode", StatusCode.HasValue);
            info.AddValue("StatusCode", StatusCode ?? 0);
        }

        /// <inheritdoc cref="Exception.Message"/>
        /// <remarks>Prefer reading <see cref="Exception.Message"/> directly. This method exists only for backwards compatibility.</remarks>
        [Obsolete("Use the Message property instead.")]
        public string what() => Message;

        /// <inheritdoc />
        public override string ToString() => $"KineticaException: {Message}";
    }
}
