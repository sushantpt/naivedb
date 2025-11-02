namespace naivedb.core.indexing
{
    /// <summary>
    /// contract for managing and accessing indexing (any data structure)
    /// </summary>
    public interface IIndex
    {
        string Name { get; }
        Type KeyType { get; }
        Task LoadAsync();
        Task SaveAsync();
        Task<object>? GetAsync(object key);
        Task InsertAsync(object key, RowPointer ptr);
        Task DeleteAsync(object key);
        Task RebuildAsync(Func<IAsyncEnumerable<(object key, RowPointer ptr)>> scanPredicate);
    }
}