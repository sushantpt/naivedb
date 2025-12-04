using naivedb.core.engine.bpt;
using naivedb.core.serialization;
using naivedb.core.storage;

namespace naivedb.core.indexing.bpt
{
    /// <summary>
    /// use for building secondary indexes using b+ tree
    /// </summary>
    public class BPlusTreeIndex<TKey> : IIndex where TKey : IComparable<TKey>
    {
        private readonly string _name;
        private readonly string _path;
        private readonly MessagePackDataSerializer _serializer = new();
        private readonly BPlusTree<TKey, RowPointer> _bPlusTree;
        private readonly PagedFileStorageUsingBPT _storage;
        
        public BPlusTreeIndex(string name, string basePath, PagedFileStorageUsingBPT storage, int order = 32)
        {
            _name = name;
            _path = Path.Combine(basePath, name);
            Directory.CreateDirectory(_path);
            _storage = storage;
            _bPlusTree = new BPlusTree<TKey, RowPointer>(_path, order);
        }
        
        public string Name => _name;
        public Type KeyType => typeof(TKey);
        
        public async Task LoadAsync()
        {
            var root = await _bPlusTree.LoadAsync();
            if (root == null)
                await _bPlusTree.SaveAsync();
        }

        public async Task SaveAsync()
        {
            await _bPlusTree.SaveAsync();
        }

        public async Task<object?> GetAsync(object key)
        {
            if (key is not TKey typedKey)
                throw new ArgumentException($"Invalid key type. Expected {typeof(TKey)}");

            return await _bPlusTree.GetAsync(typedKey);
        }

        public async Task InsertAsync(object key, RowPointer ptr)
        {
            if (key is not TKey typedKey)
                throw new ArgumentException($"Invalid key type. Expected {typeof(TKey)}");

            await _bPlusTree.Add(typedKey, ptr);
        }

        public Task DeleteAsync(object key)
        {
            throw new NotImplementedException();
        }

        public async Task RebuildAsync(Func<IAsyncEnumerable<(object key, RowPointer ptr)>> scanPredicate)
        {
            Directory.Delete(_path, true);
            Directory.CreateDirectory(_path);
            
            await foreach (var (keyObj, ptr) in scanPredicate())
            {
                if (keyObj is TKey key)
                    await _bPlusTree.Add(key, ptr);
                else
                    throw new Exception($"Invalid key type for index {Name}");
            }

            await _bPlusTree.SaveAsync();
        }
    }
}