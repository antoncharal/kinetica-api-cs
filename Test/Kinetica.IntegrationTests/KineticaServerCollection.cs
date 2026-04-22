using Kinetica.IntegrationTests.Fixtures;
using Xunit;

namespace Kinetica.IntegrationTests;

/// <summary>
/// xUnit collection that shares a single <see cref="KineticaServerFixture"/>
/// across all integration test classes, so the scratch schema is created once
/// and dropped once.
/// </summary>
[CollectionDefinition(Name)]
public sealed class KineticaServerCollection : ICollectionFixture<KineticaServerFixture>
{
    public const string Name = "KineticaServer";
}
