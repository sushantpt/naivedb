using naivedb.core.storage.pages;

namespace naivedb.core.storage
{
    /// <summary>
    /// Represents an abstraction for a storage mechanism
    /// </summary>
    public interface IStorage
    {
        Task AppendAsync(Row row);
        Task BulkAppendAsync(IEnumerable<Row> rows);
        // todo -> readall mustnt be preferred, predicated search should be implemented when indexing is added; upcoming version
        IAsyncEnumerable<Row> ReadAllAsync();
        Task SaveAllAsync(List<Row> records, string lastOperation = "update");
        Task<PageHeader> GetMetadataAsync();
    }
}