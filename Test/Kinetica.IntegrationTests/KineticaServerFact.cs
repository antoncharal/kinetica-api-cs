using System;
using Xunit;

namespace Kinetica.IntegrationTests;

/// <summary>
/// Marks a test as requiring a live Kinetica server.
/// Skipped automatically when the <c>KINETICA_URL</c> environment variable is not set.
/// </summary>
public sealed class KineticaServerFact : FactAttribute
{
    public KineticaServerFact()
    {
        if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("KINETICA_URL")))
            Skip = "KINETICA_URL not set — integration test skipped.";
    }
}
