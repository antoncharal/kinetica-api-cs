using System;
using System.Text;

namespace kinetica.Internal
{
    /// <summary>
    /// Constructs the HTTP <c>Authorization</c> header value for Kinetica requests.
    /// <para>
    /// Extracted from <see cref="Kinetica"/> to satisfy the Single Responsibility Principle
    /// and to enable isolated testing without a live server connection.
    /// </para>
    /// </summary>
    internal static class AuthorizationHeaderBuilder
    {
        /// <summary>
        /// Returns the appropriate <c>Authorization</c> header value, or <c>null</c> when
        /// no credentials are present.
        /// <list type="bullet">
        ///   <item>OAuth token present  → <c>Bearer &lt;token&gt;</c></item>
        ///   <item>Username/password    → <c>Basic &lt;base64(user:pass)&gt;</c> (ISO-8859-1)</item>
        ///   <item>Neither              → <c>null</c></item>
        /// </list>
        /// </summary>
        internal static string? Create( string? username, string? password, string? oauthToken )
        {
            if ( oauthToken is { Length: > 0 } )
                return "Bearer " + oauthToken;

            if ( ( username is { Length: > 0 } ) || ( password is { Length: > 0 } ) )
            {
                // RFC 7617 §2 specifies ISO-8859-1 for the user-info encoding.
                byte[] credentialBytes = Encoding.GetEncoding( "ISO-8859-1" )
                                                  .GetBytes( ( username ?? string.Empty )
                                                             + ":"
                                                             + ( password ?? string.Empty ) );
                return "Basic " + Convert.ToBase64String( credentialBytes );
            }

            return null;
        }
    }
}
