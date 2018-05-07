using System;

namespace Orleans.Persistence.Cassandra.Storage
{
    public sealed class CassandraConcurrencyException : Exception
    {
        public CassandraConcurrencyException(string grainId, string stateEtag, string currentEtag)
            : base($"State of grain with id '{grainId}' cannot be updated due to concurrency. State Etag = '{stateEtag}', current Etag = '{currentEtag}'")
        {
        }
    }
}