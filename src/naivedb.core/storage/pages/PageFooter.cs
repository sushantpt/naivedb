namespace naivedb.core.storage.pages
{
    /// <summary>
    /// Represents metadata for a page footer.
    /// </summary>
    public class PageFooter
    {
        public string Checksum { get; set; } = string.Empty;
        public long PageSizeBytes { get; set; }
        public DateTime WrittenAt { get; set; } = DateTime.UtcNow;
    }
}