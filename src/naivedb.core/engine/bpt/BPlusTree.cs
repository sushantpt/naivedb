using naivedb.core.serialization;
using naivedb.core.storage.pages;
using naivedb.core.utils;

namespace naivedb.core.engine.bpt
{
    /*
     * https://www.youtube.com/watch?v=aZjYr87r1b8
     */
    public class BPlusTree<TKey, TValue> where TKey : IComparable<TKey>
    {
        private readonly string _path; // actual path to save this b+ tree. eg: for a table called "users" path will be: /../users/<.data or .index>
        private readonly MessagePackDataSerializer _serializer = new();
        private BPlusNode<TKey, TValue> _rootNode;
        private readonly int _order; // keys per node
        private readonly int _minKeys; // minimum keys to split a node and/or merge a node if underflow occurs
        
        public BPlusTree(string path, int order = 32)
        {
            if(order < 3)
                throw new ArgumentException("Order must be greater than 3.", nameof(order));
            
            _path = path;
            _order = order;
            _minKeys = Math.Max(1, order / 2);
            Directory.CreateDirectory(path);
            _rootNode = new BPlusNode<TKey, TValue>(true); // root is always leaf initially
        }
        
        # region public apis
        
        public async Task Add(TKey key, TValue value)
        {
            var leaf = await FindLeafAsync(_rootNode, key);
            await InsertInLeafAsync(leaf, key, value);
        }
        
        public async Task<TValue?> GetAsync(TKey key)
        {
            var leaf = await FindLeafAsync(_rootNode, key);
            int idx = leaf.Keys.BinarySearch(key);
            if (idx >= 0 && !leaf.IsDeleted[idx]) 
                return leaf.Values[idx];
            return default;
        }
        
        public async Task SaveAsync()
        {
            await SaveNodeRecursiveAsync(_rootNode);
            var meta = new
            {
                RootId = _rootNode.NodeId,
                Order = _order // keys per node
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
            _rootNode = await LoadNodeAsync(rootId);
            return _rootNode;
        }
        
        #endregion

        #region b+ tree internals and helpers
        
        private async Task<BPlusNode<TKey, TValue>> FindLeafAsync(BPlusNode<TKey, TValue> rootNode, TKey key)
        {
            if(rootNode.IsLeaf || rootNode.ChildIds.Count == 0)
                return rootNode;
            int idx = rootNode.Keys.BinarySearch(key); // keys are sorted, so bs returns idx of the key if found, else idx of the next key (so bitwise invert to get the idx of the key before the key to be inserted)
            if (idx < 0)
                idx = ~idx;
            string childId = rootNode.ChildIds[idx];
            var childNode = await LoadNodeAsync(childId);
            return await FindLeafAsync(childNode, key);
        }

        private async Task InsertInLeafAsync(BPlusNode<TKey, TValue> leaf, TKey key, TValue value)
        {
            int idx = leaf.Keys.BinarySearch(key); // bs returns idx of the key if found, else idx of the next key (bitwise invert to get the idx of the key before the key to be inserted). 
            if (idx >= 0)
            {
                leaf.Values[idx] = value;
                leaf.IsDeleted[idx] = false;
            }
            else
            {
                idx = ~idx; // eg: if idx returned -4 (bs didnt found and returned next negative element), bitwise invert to get 3 i.e. index to insert at
                leaf.Keys.Insert(idx, key);
                leaf.Values.Insert(idx, value);
                leaf.IsDeleted.Insert(idx, false);
            }
            leaf.NodeIsDirty = true;
            
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
                NextNodeId = leaf.NextNodeId,
            };
            
            leaf.Keys = leaf.Keys.Take(mid).ToList();
            leaf.Values = leaf.Values.Take(mid).ToList();
            leaf.NextNodeId = right.NodeId;
            
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
                _rootNode = newRoot;
                await SaveNodeAsync(_rootNode);
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

        private async Task DeleteAsync(TKey key)
        {
            var leaf = await FindLeafAsync(_rootNode, key);
            int idx = leaf.Keys.BinarySearch(key);
            if (idx >= 0)
            {
                leaf.Tombstone(idx);
                await SaveNodeAsync(leaf);
            }
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
            string tmpPath = nodePath + ".tmp";
            
            var nodePage = new NodePageForIndex<TKey, TValue>
            {
                Header = new PageHeader { PageNumber = 0, TableName = _path },
                Node = node,
                Footer = new PageFooter()
            };
            var dataBytes = _serializer.Serialize(node);
            nodePage.Footer.Checksum = ChecksumUtils.ComputeCrc32C(dataBytes); // compute checksum before serializing and only of node i.e. the data
            
            var finalBytes = _serializer.Serialize(nodePage);
            await File.WriteAllBytesAsync(tmpPath, finalBytes);
            File.Move(tmpPath, nodePath, overwrite: true);
            
            node.NodeIsDirty = false; // finally, saved to disk and now in-mem node isnt dirty
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
            if (!File.Exists(nodePath))
                throw new FileNotFoundException($"Node {nodeId} not found. File lookup path: {nodePath}");
            
            var fileBytes = await File.ReadAllBytesAsync(nodePath);
            var nodePage = _serializer.Deserialize<NodePageForIndex<TKey, TValue>>(fileBytes) ?? throw new IOException($"Invalid node page {nodeId}!");
            
            var dataBytes = _serializer.Serialize(nodePage.Node);
            var computedChecksum = ChecksumUtils.ComputeCrc32C(dataBytes);
            if (nodePage.Footer.Checksum != computedChecksum)
                throw new IOException($"Checksum mismatch for node {nodeId}!");
            return nodePage.Node;
        }

        #endregion
    }
}