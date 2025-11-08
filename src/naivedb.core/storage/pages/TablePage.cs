namespace naivedb.core.storage.pages
{
    /// <summary>
    /// Represents an actual table page.
    /// </summary>
    public class TablePage
    {
        public PageHeader Header { get; set; } = new();
        public List<Row> Body { get; set; } = new();
        public PageFooter Footer { get; set; } = new();
        
        public int? NextPageNumber { get; set; }
        public int? PrevPageNumber { get; set; }
    }
}