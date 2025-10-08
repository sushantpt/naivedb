namespace naivedb.core.storage.pages
{
    /// <summary>
    /// Represents a data (row/tuple) record stored as a dictionary with case-insensitive keys.
    /// </summary>
    public class Row : Dictionary<string, object?>
    {
        public Row() : base(StringComparer.OrdinalIgnoreCase)
        {
            
        }

        public string? Key
        {
            get => this.TryGetValue("key", out var value) ? value?.ToString() : null;
            set => this["key"] = value;
        }
        
        /// <summary>
        /// Creates a new instance of the <see cref="Row"/> class from a set of key-value pairs.
        /// </summary>
        /// <param name="values">An array of tuples where each tuple contains a key and corresponding value to be added to the record.</param>
        /// <returns>A new <see cref="Row"/> instance containing the specified key-value pairs.</returns>
        public static Row From(params (string key, object value)[] values)
        {
            var record = new Row();
            foreach ((string key, object value) in values)
                record.Add(key, value);
            return record;
        }
    }
}