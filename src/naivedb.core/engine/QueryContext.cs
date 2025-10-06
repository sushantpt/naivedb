namespace naivedb.core.engine
{
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