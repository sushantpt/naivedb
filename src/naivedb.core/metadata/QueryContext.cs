namespace naivedb.core.metadata
{
    /*
     * query execution metadata
     */ 
    public class QueryContext
    {
        public string Query { get; }
        public DateTime StartedAt { get; }

        public QueryContext(string query)
        {
            Query = query;
            StartedAt = DateTime.UtcNow;
        }
    }
}