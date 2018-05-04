namespace Orleans.Persistence.Cassandra.Models
{
    internal sealed class GrainState
    {
        public string Id { get; set; }
        public string GrainType { get; set; }
        public string State { get; set; }
        public string ETag { get; set; }
    }
}