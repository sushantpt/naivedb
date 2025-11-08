namespace naivedb.core.indexing
{
    /*
     * RowPointer is used to point data/row in b+ tree. eg: each index (primary or secondary) is also a b+ tree. leaf nodes store RowPointer to the row in the data b+ tree.
     * leaf node id of data in b+ tree
     * index of the row in the leaf node
     * 
     * NodeId indicates b+ tree node id
     * SlotIndex indicates specific position of the row in the leaf node
     */
    public record RowPointer(string NodeId, int SlotIndex)
    {
        public override string ToString()
        {
            return $"(Nid:{NodeId}, Sidx:{SlotIndex})";
        }
    }
}