using System;
using System.Collections.Generic;

namespace Orleans.Persistence.Cassandra.Concurrency
{
    public interface IConcurrentGrainStateTypesProvider
    {
        IReadOnlyCollection<Type> GetGrainStateTypes();
    }
}