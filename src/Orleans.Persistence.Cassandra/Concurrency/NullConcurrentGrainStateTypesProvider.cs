using System;
using System.Collections.Generic;

namespace Orleans.Persistence.Cassandra.Concurrency
{
    public sealed class NullConcurrentGrainStateTypesProvider : IConcurrentGrainStateTypesProvider
    {
        public IReadOnlyCollection<Type> GetGrainStateTypes() => Array.Empty<Type>();
    }
}