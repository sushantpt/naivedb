using System.Text.Json.Serialization;
using MessagePack;

namespace naivedb.core.engine.bpt
{
    /*
     * leaf nodes store data itself or rowpointers for secondary indexes
     * internal nodes store keys and child ids
     */
    public class BPlusNode<TKey, TValue> where TKey : IComparable<TKey>
    {
        public string NodeId { get; set; } = Guid.CreateVersion7().ToString("N"); // unique id for each node
        public bool IsLeaf { get; set; } // true: points toward actual data; false: stores child ids
        public List<TKey> Keys { get; set; } = [];
        public List<TValue> Values { get; set; } = []; // only for leaf nodes i.e. to store actual data or rowpointers or page numbers
        public List<string> ChildIds { get; set; } = []; // only for internal nodes i.e. to act like a pointer to child nodes
        public bool NodeIsDirty { get; set; } = false; // dirty nodes to be flushed/reshuffled in the background asynchronously 
        public List<bool> IsDeleted { get; set; } = []; // tombstone for leafnodes
        
        /*
         * connect to other pages, sorta linked list 
         */
        public string? NextNodeId { get; set; }
        
        /*
         * ignore because we dont need to serialize this; reason is that we dont want to serialize the entire tree (so its bit faster)
         */
        [JsonIgnore] [IgnoreMember]
        public BPlusNode<TKey, TValue>? Parent { get; set; } = null;
        
        public BPlusNode(bool isLeaf)
        {
            IsLeaf = isLeaf;
        }
        
        public BPlusNode(){ }
        
        public bool IsFull(int order) => Keys.Count >= order; // check if node is full; if so, split
        public bool IsUnderflow(int order) => Keys.Count <= Math.Max(1, (order + 1) / 2); // check if node is underflow; if so, merge
        
        public void Tombstone(int index)
        {
            if (!IsLeaf) 
                throw new InvalidOperationException("Only leaf nodes can delete keys.");
            IsDeleted[index] = true;
            NodeIsDirty = true;
        }
        
        public IEnumerable<(TKey Key, TValue Value)> GetActiveKeys()
        {
            for (int i = 0; i < Keys.Count; i++)
            {
                if (!IsDeleted[i])
                    yield return (Keys[i], Values[i]);
            }
        }
    }
}