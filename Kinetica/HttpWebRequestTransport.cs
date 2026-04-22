using System.IO;
using System.Net;

namespace kinetica
{
    /// <summary>
    /// Default <see cref="IHttpTransport"/> implementation backed by the
    /// legacy <see cref="HttpWebRequest"/> API.  Behaviour is identical to the
    /// hard-coded HTTP logic that previously lived in
    /// <c>Kinetica.SubmitRequestRaw</c>.
    /// </summary>
    internal sealed class HttpWebRequestTransport : IHttpTransport
    {
        public byte[] Post(string url, byte[] body, string contentType, string? authorization)
        {
            var request = (HttpWebRequest)WebRequest.Create(url);
            request.Method = "POST";
            request.ContentType = contentType;
            request.ContentLength = body.Length;

            if (authorization != null)
                request.Headers.Add("Authorization", authorization);

            using (var dataStream = request.GetRequestStream())
                dataStream.Write(body, 0, body.Length);

            // GetResponse() throws WebException for 4xx / 5xx — the caller
            // (SubmitRequestRaw) catches WebException and decodes the error body.
            using var response = (HttpWebResponse)request.GetResponse();
            using var responseStream = response.GetResponseStream();
            using var ms = new MemoryStream();
            responseStream.CopyTo(ms);
            return ms.ToArray();
        }
    }
}
