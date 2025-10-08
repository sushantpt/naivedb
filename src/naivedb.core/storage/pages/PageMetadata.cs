namespace naivedb.core.storage.pages
{
    /// <summary>
    /// Represents metadata for a page.
    /// </summary>
    public class PageMetadata
    {
        /// <summary>
        /// The name of the table that the page belongs to.
        /// </summary>
        public string TableName { get; set; } = string.Empty;

        /// <summary>
        /// Creation date of the page.
        /// </summary>
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
        
        /// <summary>
        /// Last update date of the page.
        /// </summary>
        public DateTime LastUpdated { get; set; } = DateTime.UtcNow;
        
        /// <summary>
        /// Count of records in the page.
        /// </summary>
        public int RecordCount { get; set; }

        /// <summary>
        /// The total number of operations performed on the page.
        /// </summary>
        public int OperationCount { get; set; }
        
        /// <summary>
        /// Last operation performed on the page.
        /// </summary>
        public string LastOperation { get; set; } = "none";
        
        /// <summary>
        /// Page version.
        /// </summary>
        public string Version { get; set; } = "1.0.0";
    }
    
    /// <summary>
    /// Represents metadata for a page footer.
    /// </summary>
    public class PageFooter
    {
        /// <summary>
        /// Used to verify the integrity of the serialized page data.
        /// </summary>
        public string Checksum { get; set; } = string.Empty;
        
        /// <summary>
        /// Size of the page in bytes.
        /// </summary>
        public long PageSizeBytes { get; set; }
        
        /// <summary>
        /// Date and time when the page was written to disk.
        /// </summary>
        public DateTime WrittenAt { get; set; } = DateTime.UtcNow;
    }

    /// <summary>
    /// Represents an actual table page.
    /// </summary>
    public class TablePage
    {
        /// <summary>
        /// Page header containing metadata.
        /// </summary>
        public PageMetadata Header { get; set; } = new();
        
        /// <summary>
        /// Page body, containing the records/tuples/rows.
        /// </summary>
        public List<Row> Body { get; set; } = new();
        
        /// <summary>
        /// Page footer containing metadata.
        /// </summary>
        public PageFooter Footer { get; set; } = new();
    }
}