using naivedb.core.configs;
using naivedb.core.storage;

namespace naivedb.core.engine
{
    /// <summary>
    /// Provides functionality for managing and manipulating a collection of records in a database.
    /// Each table is stored as a page-based JSON file (header, body, footer).
    /// </summary>
    public class Database : IDatabase
    {
        private readonly string _dataDirectory;
        private readonly Dictionary<string, FileStorage> _tables = new();
        private readonly DbOptions _options;

        public Database(string dataDirectory, DbOptions options)
        {
            _dataDirectory = dataDirectory;
            _options = options;
            if (!Directory.Exists(_dataDirectory))
                Directory.CreateDirectory(_dataDirectory);
        }

        public void Create(string tableName, Record record)
        {
            var storage = GetTableStorage(tableName);
            storage.Append(record);
        }

        public void Update(string tableName, Record record)
        {
            var storage = GetTableStorage(tableName);
            var records = storage.ReadAll().ToList();
            var index = records.FindIndex(r => r.Key == record.Key);
            if (index >= 0)
                records[index] = record;
            storage.SaveAll(records, "update");
        }

        public void Delete(string tableName, string key)
        {
            var storage = GetTableStorage(tableName);
            var records = storage.ReadAll().ToList();

            var index = records.FindIndex(r => r.Key == key);
            if (index == -1)
                throw new Exception($"Record with key {key} not found.");

            records.RemoveAt(index);
            storage.SaveAll(records, lastOperation: "delete");
        }

        public ResultSet ReadAll(string tableName)
        {
            var storage = GetTableStorage(tableName);
            var records = storage.ReadAll();
            return new ResultSet(records);
        }

        public PageMetadata GetTableMetadata(string tableName)
        {
            var storage = GetTableStorage(tableName);
            return storage.GetMetadata();
        }

        private FileStorage GetTableStorage(string table)
        {
            if (!_tables.ContainsKey(table))
                _tables[table] = new FileStorage(_dataDirectory, table);
            return _tables[table];
        }
    }
}
