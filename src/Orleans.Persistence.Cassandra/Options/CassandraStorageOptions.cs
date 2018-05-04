using System.Collections.Generic;

namespace Orleans.Persistence.Cassandra.Options
{
    public class CassandraStorageOptions
    {
        public const int DefaultInitStage = ServiceLifecycleStage.ApplicationServices;

        /// <summary>
        /// Stage of silo lifecycle where storage should be initialized.  Storage must be initialzed prior to use.
        /// </summary>
        public int InitStage { get; set; } = DefaultInitStage;

        public IEnumerable<string> ContactPoints { get; set; }
        public string Keyspace { get; set; } = "orleans";
        public string TableName { get; set; } = "grain_state";
        public int ReplicationFactor { get; set; } = 3;
    }
}