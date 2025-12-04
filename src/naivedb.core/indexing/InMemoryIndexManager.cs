using System.Collections.Concurrent;

namespace naivedb.core.indexing
{
    public class InMemoryIndexManager
    {
        private static readonly Lazy<InMemoryIndexManager> _instance = new(() => new InMemoryIndexManager());
        public static InMemoryIndexManager Instance => _instance.Value;
        
        private readonly ConcurrentDictionary<long, RowPointer> _cache = [];

        public void AddOrUpdateIndex(long key, RowPointer ptr)
        {
            var add = _cache.TryAdd(key, ptr);
            if (!add)
                _cache[key] = ptr;
        }

        public bool TryGetIndex(long key, out RowPointer? ptr)
        {
            return _cache.TryGetValue(key, out ptr);
        }

        public bool RemoveIndex(long key)
        {
            return _cache.TryRemove(key, out _);
        }

        public void Clear()
        {
            _cache.Clear();
        }

        // test case only, dict may be large
        public IEnumerable<KeyValuePair<long, RowPointer>> GetAllIndexes()
        {
            return _cache;
        }

        public int Count => _cache.Count;
        
    }
}