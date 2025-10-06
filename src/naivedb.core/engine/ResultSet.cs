using naivedb.core.storage;

namespace naivedb.core.engine
{
    /// <summary>
    /// Represents a set of results, consisting of multiple records.
    /// </summary>
    public class ResultSet
    {
        /// <summary>
        /// Gets the collection of records contained within the result set.
        /// </summary>
        public List<Record> Records { get; }
        
        /// <summary>
        /// Gets the first record if one exists, otherwise null.
        /// </summary>
        public Record? Single => Records.FirstOrDefault();

        /// <summary>
        /// Gets the total number of records.
        /// </summary>
        public int Count => Records.Count;

        public ResultSet(IEnumerable<Record> records)
        {
            Records = records.ToList();
        }
    }
}