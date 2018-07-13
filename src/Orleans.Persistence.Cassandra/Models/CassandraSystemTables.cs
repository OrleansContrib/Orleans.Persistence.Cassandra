namespace Orleans.Persistence.Cassandra.Models
{
    internal sealed class CassandraSystemTables
    {
        public string KeyspaceName { get; set; }
        public string TableName { get; set; }
    }
}