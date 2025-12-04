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
            _rootNode = new BPlusNode<TKey, TValue>(true) // root is always leaf initially
            {
                Keys = [],
                Values = [],
                IsDeleted = [],
                ChildIds = [],
            };
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

        public BPlusNode<TKey, TValue>? Load()
        {
            string metaPath = Path.Combine(_path, "tree.meta");
            if (!File.Exists(metaPath))
                return null;
            var bytes = File.ReadAllBytes(metaPath);
            var meta = _serializer.Deserialize<dynamic>(bytes);
            string rootId = meta!["RootId"];
            _rootNode = LoadNodeAsync(rootId).GetAwaiter().GetResult();
            return _rootNode;
        }
        
        public async Task DeleteAsync(TKey key)
        {
            var leaf = await FindLeafAsync(_rootNode, key);
            int idx = leaf.Keys.BinarySearch(key);
            if (idx >= 0)
            {
                leaf.Tombstone(idx);
                await SaveNodeAsync(leaf);
            }
        }
        
        #endregion

        #region b+ tree internals and helpers
        
        private async Task<BPlusNode<TKey, TValue>> FindLeafAsync(BPlusNode<TKey, TValue> node, TKey key)
        {
            // base case: if at a leaf or have no children, return current node
            if (node.IsLeaf || node.ChildIds.Count == 0)
                return node;

            // find appropriate child to traverse to
            int idx = FindChildIndex(node, key);
            
            // load child node and continue search
            var childNode = await LoadNodeAsync(node.ChildIds[idx]);
            return await FindLeafAsync(childNode, key);
        }

        private int FindChildIndex(BPlusNode<TKey, TValue> node, TKey key)
        {
            // binary search to find the right child index
            int low = 0;
            int high = node.Keys.Count - 1;
            int idx = 0;

            while (low <= high)
            {
                int mid = (low + high) / 2;
                int comparison = key.CompareTo(node.Keys[mid]);
                
                if (comparison < 0)
                {
                    high = mid - 1;
                    idx = mid;
                }
                else if (comparison > 0)
                {
                    low = mid + 1;
                    idx = mid + 1;
                }
                else
                {
                    // key matches exactly, go to the right child
                    idx = mid + 1;
                    break;
                }
            }
            
            return Math.Clamp(idx, 0, node.ChildIds.Count - 1);
        }

        private async Task InsertInLeafAsync(BPlusNode<TKey, TValue> leaf, TKey key, TValue value)
        {
            while (leaf.IsDeleted.Count < leaf.Keys.Count)
                leaf.IsDeleted.Add(false);
            
            // find insertion position using binary search
            int idx = leaf.Keys.BinarySearch(key);
            if (idx >= 0)
            {
                leaf.Values[idx] = value;
                leaf.IsDeleted[idx] = false;
            }
            else
            {
                idx = ~idx;
                idx = Math.Clamp(idx, 0, leaf.Keys.Count);
                // insert into all three lists at the same position
                leaf.Keys.Insert(idx, key);
                leaf.Values.Insert(idx, value);
                leaf.IsDeleted.Insert(idx, false);
            }
            
            leaf.NodeIsDirty = true;
            
            // check if we need to split the leaf
            if (leaf.Keys.Count > _order)
            {
                await SplitLeafAsync(leaf);
            }
            else
            {
                await SaveNodeAsync(leaf);
            }
        }

        private async Task SplitLeafAsync(BPlusNode<TKey, TValue> leaf)
        {
            int splitIndex = leaf.Keys.Count / 2;
            var rightLeaf = new BPlusNode<TKey, TValue>(true)
            {
                Keys = new List<TKey>(),
                Values = new List<TValue>(),
                IsDeleted = new List<bool>(),
                NextNodeId = leaf.NextNodeId
            };

            // move half the entries to the right leaf
            for (int i = splitIndex; i < leaf.Keys.Count; i++)
            {
                rightLeaf.Keys.Add(leaf.Keys[i]);
                rightLeaf.Values.Add(leaf.Values[i]);
                rightLeaf.IsDeleted.Add(leaf.IsDeleted[i]);
            }

            // remove moved entries from original leaf
            leaf.Keys.RemoveRange(splitIndex, leaf.Keys.Count - splitIndex);
            leaf.Values.RemoveRange(splitIndex, leaf.Values.Count - splitIndex);
            leaf.IsDeleted.RemoveRange(splitIndex, leaf.IsDeleted.Count - splitIndex);
            
            // update leaf pointers
            leaf.NextNodeId = rightLeaf.NodeId;

            // save both nodes
            await SaveNodeAsync(leaf);
            await SaveNodeAsync(rightLeaf);

            // promote the first key of right leaf to parent
            if (rightLeaf.Keys.Count > 0)
            {
                await InsertIntoParentAsync(leaf, rightLeaf.Keys[0], rightLeaf);
            }
        }

        private async Task InsertIntoParentAsync(BPlusNode<TKey, TValue> leftChild, TKey key, BPlusNode<TKey, TValue> rightChild)
        {
            var parent = leftChild.Parent;

            if (parent == null)
            {
                await CreateNewRoot(leftChild, key, rightChild);
                return;
            }

            // get insertion position in parent
            int idx = parent.Keys.BinarySearch(key);
            if (idx < 0) 
                idx = ~idx;
            
            idx = Math.Clamp(idx, 0, parent.Keys.Count);
            parent.Keys.Insert(idx, key);
            
            // find position for child reference (always idx + 1 for right child)
            int childIdx = idx + 1;
            childIdx = Math.Clamp(childIdx, 0, parent.ChildIds.Count);
            parent.ChildIds.Insert(childIdx, rightChild.NodeId);
            
            rightChild.Parent = parent;

            // check if parent needs splitting
            if (parent.Keys.Count > _order)
            {
                await SplitInternalNodeAsync(parent);
            }
            else
            {
                await SaveNodeAsync(parent);
            }
        }

        private async Task CreateNewRoot(BPlusNode<TKey, TValue> leftChild, TKey key, BPlusNode<TKey, TValue> rightChild)
        {
            var newRoot = new BPlusNode<TKey, TValue>(false)
            {
                Keys = [key],
                ChildIds = [leftChild.NodeId, rightChild.NodeId]
            };

            leftChild.Parent = newRoot;
            rightChild.Parent = newRoot;
            _rootNode = newRoot;
            await SaveNodeAsync(_rootNode);
        }

        private async Task SplitInternalNodeAsync(BPlusNode<TKey, TValue> node)
        {
            int splitIndex = node.Keys.Count / 2;
            TKey promoteKey = node.Keys[splitIndex];
            
            var rightNode = new BPlusNode<TKey, TValue>(false)
            {
                Keys = new List<TKey>(),
                ChildIds = new List<string>()
            };

            // move keys and children to right node (skip the promoted key)
            for (int i = splitIndex + 1; i < node.Keys.Count; i++)
            {
                rightNode.Keys.Add(node.Keys[i]);
            }
            for (int i = splitIndex + 1; i < node.ChildIds.Count; i++)
            {
                rightNode.ChildIds.Add(node.ChildIds[i]);
            }

            // remove moved entries from original node
            node.Keys.RemoveRange(splitIndex, node.Keys.Count - splitIndex);
            node.ChildIds.RemoveRange(splitIndex + 1, node.ChildIds.Count - (splitIndex + 1));

            // save both nodes
            await SaveNodeAsync(node);
            await SaveNodeAsync(rightNode);

            // promote middle key to parent
            await InsertIntoParentAsync(node, promoteKey, rightNode);
        }
        
        private async Task RebalanceAfterDeleteAsync(BPlusNode<TKey, TValue> leaf)
        {
            // todo: merge or redistribute keys if underflow occurs; better if done in the background
            throw new NotImplementedException();
        }
        
        private async Task SaveNodeAsync(BPlusNode<TKey, TValue> node)
        {
            if (node.IsLeaf)
            {
                while (node.IsDeleted.Count < node.Keys.Count)
                    node.IsDeleted.Add(false);
                if (node.IsDeleted.Count > node.Keys.Count)
                    node.IsDeleted = node.IsDeleted.Take(node.Keys.Count).ToList();
            }
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
            var node = nodePage.Node;
            if (node.IsLeaf)
            {
                while (node.IsDeleted.Count < node.Keys.Count)
                    node.IsDeleted.Add(false);
                if (node.IsDeleted.Count > node.Keys.Count)
                    node.IsDeleted = node.IsDeleted.Take(node.Keys.Count).ToList();
            }
            return nodePage.Node;
        }

        public async IAsyncEnumerable<object> TraverseRangeAsync(long startKey, long endKey)
        {
            var startTKey = (TKey)Convert.ChangeType(startKey, typeof(TKey));
            var endTKey = (TKey)Convert.ChangeType(endKey, typeof(TKey));
            if (startTKey.CompareTo(endTKey) == 0)
            {
                var val = await GetAsync(startTKey);
                if (val != null)
                    yield return val!;
                yield break;
            }

            if (startTKey.CompareTo(endTKey) > 0)
            {
                startTKey = (TKey)Convert.ChangeType(startKey, typeof(TKey));
                endTKey = (TKey)Convert.ChangeType(endKey, typeof(TKey));
            }

            var leaf = await FindLeafAsync(_rootNode, startTKey);
            while (leaf != null)
            {
                for (int i = 0; i < leaf.Keys.Count; i++)
                {
                    var currentKey = leaf.Keys[i];
                    if (currentKey.CompareTo(startTKey) >= 0 && currentKey.CompareTo(endTKey) <= 0 && !leaf.IsDeleted[i])
                    {
                        yield return leaf.Values[i]!;
                    }
                    else if (currentKey.CompareTo(endTKey) > 0)
                    {
                        yield break;
                    }
                }

                if (string.IsNullOrEmpty(leaf.NextNodeId))
                    yield break;

                leaf = await LoadNodeAsync(leaf.NextNodeId);
            }
        }

        #endregion
    }
}