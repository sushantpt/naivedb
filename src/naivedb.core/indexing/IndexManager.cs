namespace naivedb.core.indexing
{
    public class IndexManager
    {
        private readonly Dictionary<string, IIndex> _indexes = new();
        
        public void Register(IIndex index)
        {
            var add = _indexes.TryAdd(index.Name, index);
            if (!add)
                throw new Exception($"Index {index.Name} already exists.");
        }

        public IIndex? GetIndex(string name)
        {
            return _indexes.GetValueOrDefault(name);
        }
        
        public async Task RebuildAsync(Func<string, IAsyncEnumerable<(object key, RowPointer ptr)>> scanPredicate)
        {
            foreach (var idx in _indexes.Values)
            {
                await idx.RebuildAsync(() => scanPredicate(idx.Name));
            }
        }
    }
}