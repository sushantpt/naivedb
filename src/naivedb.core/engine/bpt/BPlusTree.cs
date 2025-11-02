using naivedb.core.serialization;
using naivedb.core.storage.pages;

namespace naivedb.core.engine.bpt
{
    /*
     * https://www.youtube.com/watch?v=aZjYr87r1b8
     */
    public class BPlusTree<TKey, TValue> where TKey : IComparable<TKey>
    {
        private readonly string _path; // actual path to save this b+ tree. eg: for a table called "users" path will be: /../users/<.data or .index>
        private readonly MessagePackDataSerializer _serializer = new();
        private BPlusNode<TKey, TValue> _root;
        private readonly int _order; // keys per node
        private readonly int _minKeys;
        
        public BPlusTree(string path, int order = 32)
        {
            if(order < 3)
                throw new ArgumentException("Order must be greater than 3.", nameof(order));
            
            _path = path;
            _order = order;
            _minKeys = Math.Max(1, order / 2);
            Directory.CreateDirectory(path);
            _root = new BPlusNode<TKey, TValue>(true); // root is always leaf initially
        }
        
        # region public apis
        
        public async Task Add(TKey key, TValue value)
        {
            var leaf = await FindLeafAsync(_root, key);
            await InsertInLeafAsync(leaf, key, value);
        }
        
        public async Task<TValue?> GetAsync(TKey key)
        {
            var leaf = await FindLeafAsync(_root, key);
            int idx = leaf.Keys.BinarySearch(key);
            if (idx >= 0) 
                return leaf.Values[idx];
            return default;
        }
        
        public async Task SaveAsync()
        {
            await SaveNodeRecursiveAsync(_root);
            var meta = new
            {
                RootId = _root.NodeId,
                Order = _order
            };
            var metaBytes = _serializer.Serialize(meta);
            await File.WriteAllBytesAsync(Path.Combine(_path, "tree.meta"), metaBytes);
        }
        
        public async Task<BPlusNode<TKey, TValue>?> LoadAsync()
        {
            string metaPath = Path.Combine(_path, "tree.meta");
            if(!File.Exists(metaPath))
                return null;
            var bytes = await File.ReadAllBytesAsync(metaPath);
            var meta = _serializer.Deserialize<dynamic>(bytes);
            string rootId = meta!["RootId"];
            _root = await LoadNodeAsync(rootId);
            return _root;
        }
        
        #endregion

        #region b+ tree internals and helpers
        
        private async Task<BPlusNode<TKey, TValue>> FindLeafAsync(BPlusNode<TKey, TValue> node, TKey key)
        {
            if(node.IsLeaf) 
                return node;
            int idx = node.Keys.FindIndex(k => key.CompareTo(k) < 0);
            int childIdx = idx < 0 ? node.ChildIds.Count - 1 : idx;
            string childId = node.ChildIds[childIdx];
            var childNode = await LoadNodeAsync(childId);
            return await FindLeafAsync(childNode, key);
        }

        private async Task InsertInLeafAsync(BPlusNode<TKey, TValue> leaf, TKey key, TValue value)
        {
            int idx = leaf.Keys.BinarySearch(key);
            if(idx >= 0)
                leaf.Values[idx] = value;
            else
            {
                idx = ~idx;
                leaf.Keys.Insert(idx, key);
                leaf.Values.Insert(idx, value);
            }
            
            if(leaf.IsFull(_order))
                await SplitLeafAsync(leaf);
            else
                await SaveNodeAsync(leaf);
        }

        private async Task SplitLeafAsync(BPlusNode<TKey, TValue> leaf)
        {
            int mid = leaf.Keys.Count / 2;
            var right = new BPlusNode<TKey, TValue>(true)
            {
                Keys = leaf.Keys.Skip(mid).ToList(),
                Values = leaf.Values.Skip(mid).ToList(),
                NextPageNumber = leaf.NextPageNumber,
                PrevPageNumber = leaf.Keys.Count > 0 ? leaf.NodeId.GetHashCode() : null
            };
            
            leaf.Keys = leaf.Keys.Take(mid).ToList();
            leaf.Values = leaf.Values.Take(mid).ToList();
            leaf.NextPageNumber = right.NodeId.GetHashCode();
            
            await SaveNodeAsync(leaf);
            await SaveNodeAsync(right);
            await InsertIntoParentAsync(leaf, right.Keys.First(), right);
        }

        private async Task InsertIntoParentAsync(BPlusNode<TKey, TValue> left, TKey key, BPlusNode<TKey, TValue> right)
        {
            if (left.Parent == null)
            {
                var newRoot = new BPlusNode<TKey, TValue>(false)
                {
                    Keys = [key],
                    ChildIds = [left.NodeId, right.NodeId],
                };
                left.Parent = newRoot;
                right.Parent = newRoot;
                _root = newRoot;
                await SaveNodeAsync(_root);
                return;
            }
            
            var parent = left.Parent;
            int idx = parent.Keys.BinarySearch(key);
            if(idx >= 0)
                idx++;
            else
                idx = ~idx; // bs returns idx of the key if found, else idx of the next key. so bitwise invert to get the idx of the key before the key to be inserted
            parent.Keys.Insert(idx, key);
            parent.ChildIds.Insert(idx + 1, right.NodeId);
            right.Parent = parent;
            if (parent.IsFull(_order))
            {
                await SplitParentAsync(parent);
            }
            
            await SaveNodeAsync(parent);
        }
        
        private async Task RebalanceAfterDeleteAsync(BPlusNode<TKey, TValue> leaf)
        {
            // todo: merge or redistribute keys if underflow occurs; better if done in the background
        }
        
        private async Task SplitParentAsync(BPlusNode<TKey, TValue> node)
        {
            int mid = node.Keys.Count / 2;
            var right = new BPlusNode<TKey, TValue>(false)
            {
                Keys = node.Keys.Skip(mid + 1).ToList(),
                ChildIds = node.ChildIds.Skip(mid + 1).ToList(),
            };
            var upKey = node.Keys[mid];
            node.Keys = node.Keys.Take(mid + 1).ToList();
            node.ChildIds = node.ChildIds.Take(mid + 1).ToList();
            
            await SaveNodeAsync(node);
            await SaveNodeAsync(right);
            await InsertIntoParentAsync(node, upKey, right);
        }
        
        private async Task SaveNodeAsync(BPlusNode<TKey, TValue> node)
        {
            string nodePath = Path.Combine(_path, $"node_{node.NodeId}.dbp");
            var nodePage = new NodePage<TKey, TValue>
            {
                Header = new PageHeader { PageNumber = 0, TableName = _path },
                Node = node,
                Footer = new PageFooter()
            };
            var bytes = _serializer.Serialize(nodePage);
            await File.WriteAllBytesAsync(nodePath, bytes);
        }
        
        private async Task SaveNodeRecursiveAsync(BPlusNode<TKey, TValue> node)
        {
            await SaveNodeAsync(node);
            if (!node.IsLeaf)
            {
                foreach (var childId in node.ChildIds)
                {
                    var childNode = await LoadNodeAsync(childId);
                    await SaveNodeRecursiveAsync(childNode);
                }
            }
        }
        
        private async Task<BPlusNode<TKey, TValue>> LoadNodeAsync(string nodeId)
        {
            string nodePath = Path.Combine(_path, $"node_{nodeId}.dbp");
            var bytes = await File.ReadAllBytesAsync(nodePath);
            var nodePage = _serializer.Deserialize<NodePage<TKey, TValue>>(bytes)!;
            return nodePage.Node;
        }

        #endregion
    }
}