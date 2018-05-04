using Cassandra.Mapping;

using Orleans.Persistence.Cassandra.Models;

namespace Orleans.Persistence.Cassandra.Storage
{
    internal class EntityMappings : Mappings
    {
        public EntityMappings(string tableName)
        {
            For<GrainState>()
                .TableName(tableName)
                .PartitionKey(x => x.Id, x => x.GrainType);
        }
    }
}