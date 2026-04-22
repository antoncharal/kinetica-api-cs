using System;
using System.Runtime.Serialization;


namespace kinetica
{
    [Serializable()]
    public class KineticaException : System.Exception
    {
        private string message;

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

        public string what() { return message; }

        public override string ToString() { return "KineticaException: " + message; }
    }
}
