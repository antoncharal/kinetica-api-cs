using System;
using System.Runtime.Serialization;


namespace kinetica
{
    [Serializable()]
    public class KineticaException : System.Exception
    {
        /// <summary>
        /// HTTP status code returned by the Kinetica server, or <c>null</c>
        /// for client-side failures (serialization, network disconnect, etc.).
        /// </summary>
        public int? StatusCode { get; }

        public KineticaException() { }

        public KineticaException(string msg) : base ( msg ) { }

        public KineticaException( string msg, Exception innerException ) :
            base( msg, innerException ) { }

        public KineticaException(string msg, int? statusCode, Exception? innerException)
            : base(msg, innerException)
        {
            StatusCode = statusCode;
        }

        protected KineticaException( SerializationInfo info, StreamingContext context )
            : base ( info, context )
        {
            StatusCode = info.GetBoolean("HasStatusCode") ? info.GetInt32("StatusCode") : null;
        }

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
