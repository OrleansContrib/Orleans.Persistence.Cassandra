using System;
using System.Collections.Generic;

using Orleans.Persistence.Cassandra.Concurrency;

namespace Orleans.Persistence.Cassandra
{
    public sealed class NullConcurrentGrainStateTypesProvider : IConcurrentGrainStateTypesProvider
    {
        public IReadOnlyCollection<Type> GetGrainStateTypes() => Array.Empty<Type>();
    }
}