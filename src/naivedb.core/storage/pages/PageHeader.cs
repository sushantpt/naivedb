namespace naivedb.core.storage.pages
{
    /// <summary>
    /// Represents metadata for a page.
    /// </summary>
    public class PageHeader
    {
        public string TableName { get; set; } = string.Empty;
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
        public DateTime LastUpdated { get; set; } = DateTime.UtcNow;
        public int PageNumber { get; set; }
        public int RecordCount { get; set; } 
        public int OperationCount { get; set; }
        public string LastOperation { get; set; } = "none";
        public string Version { get; set; } = "1.0.0";
        public string Serializer { get; set; } = "msgpack";
    }
}