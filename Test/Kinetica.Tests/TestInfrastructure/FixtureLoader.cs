using System;
using System.IO;

namespace Kinetica.Tests.TestInfrastructure
{
    internal static class FixtureLoader
    {
        private static readonly string Root = Path.Combine(
            AppContext.BaseDirectory, "Fixtures");

        public static byte[] LoadBytes(string relativePath)
            => File.ReadAllBytes(Path.Combine(Root, relativePath));

        public static string LoadText(string relativePath)
            => File.ReadAllText(Path.Combine(Root, relativePath));
    }
}
