using Cassandra.Mapping;

using Orleans.Persistence.Cassandra.Models;

namespace Orleans.Persistence.Cassandra.Storage
{
    internal sealed class EntityMappings : Mappings
    {
        public EntityMappings(string keyspaceName, string tableName)
        {
            For<CassandraSystemTables>()
                .KeyspaceName("system_schema")
                .TableName("tables")
                .Column(t => t.KeyspaceName, map => map.WithName("keyspace_name"))
                .Column(t => t.TableName, map => map.WithName("table_name"))
                .PartitionKey(t => t.KeyspaceName)
                .ClusteringKey(t => t.TableName);
            For<CassandraGrainState>()
                .KeyspaceName(keyspaceName)
                .TableName(tableName)
                .PartitionKey(t => t.Id, t => t.GrainType);
        }
    }
}