using naivedb.core.engine.bpt;

namespace naivedb.core.storage.pages
{
    public class NodePage<TKey, TValue> where TKey : IComparable<TKey>
    {
        public PageHeader Header { get; set; } = new();
        public BPlusNode<TKey, TValue> Node { get; set; } = new(true);
        public PageFooter Footer { get; set; } = new();
    }
}