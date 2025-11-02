using System.Text.Json.Serialization;

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
        
        /*
         * connect to other pages, sorta doubly linked list 
         */
        public int? PrevPageNumber { get; set; } = null;
        public int? NextPageNumber { get; set; } = null;
        
        /*
         * ignore because we dont need to serialize this; reason is that we dont want to serialize the entire tree (so its bit faster)
         */
        [JsonIgnore]
        public BPlusNode<TKey, TValue>? Parent { get; set; } = null;
        [JsonIgnore]
        public List<BPlusTree<TKey, TValue>>? Children { get; set; } = [];
        
        public BPlusNode(bool isLeaf)
        {
            IsLeaf = isLeaf;
        }
        
        public BPlusNode(){ }
        
        public bool IsFull(int order) => Keys.Count >= order; // check if node is full; if so, split
        public bool IsUnderflow(int order) => Keys.Count <= Math.Max(1, (order + 1) / 2); // check if node is underflow; if so, merge
    }
}