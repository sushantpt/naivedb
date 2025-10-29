using naivedb.core.configs;
using naivedb.core.serialization;
using naivedb.core.storage;
using naivedb.core.storage.pages;

namespace naivedb.core.engine
{
    public class Database
    {
        private readonly string _databaseDirectory;
        private readonly Dictionary<string, PagedFileStorage> _tables = new();
        private readonly DbOptions _options;
        private readonly IDataSerializer _serializer = new MessagePackDataSerializer();

        public Database(string databaseDirectory, DbOptions options)
        {
            _databaseDirectory = databaseDirectory;
            _options = options;
            Directory.CreateDirectory(_databaseDirectory);
        }

        public async Task CreateRecordAsync(string tableName, Row row)
        {
            var storage = GetTableStorage(tableName);
            await storage.AppendAsync(row);
        }

        public async Task UpdateRecordAsync(string tableName, Row row)
        {
            var storage = GetTableStorage(tableName);
            var records = new List<Row>();
            await foreach (var record in storage.ReadAllAsync())
                records.Add(record);
            
            var index = records.FindIndex(r => r.Key == row.Key);
            if (index >= 0)
                records[index] = row;
            await storage.SaveAllAsync(records, "update");
        }

        public async Task DeleteRecordAsync(string tableName, string key)
        {
            var storage = GetTableStorage(tableName);
            var records = new List<Row>();
            await foreach (var record in storage.ReadAllAsync())
                records.Add(record);

            var index = records.FindIndex(r => r.Key == key);
            if (index == -1)
                throw new Exception($"Record with key {key} not found.");

            records.RemoveAt(index);
            await storage.SaveAllAsync(records, lastOperation: "delete");
        }

        public async Task<ResultSet> ReadAllRecordAsync(string tableName)
        {
            var storage = GetTableStorage(tableName);
            var records = new List<Row>();
            await foreach (var record in storage.ReadAllAsync())
                records.Add(record);
            return new ResultSet(records);
        }

        public async Task<PageHeader> GetTableMetadataAsync(string tableName)
        {
            var storage = GetTableStorage(tableName);
            return await storage.GetMetadataAsync();
        }

        private PagedFileStorage GetTableStorage(string table)
        {
            if (!_tables.ContainsKey(table))
                //_tables[table] = new JsonFileStorage(_dataDirectory, table);
                _tables[table] = new PagedFileStorage(_databaseDirectory, table, _options);
            return _tables[table];
        }
        
        public string GetDatabasePath(string dbName)
        {
            var p = Path.Combine(_options.DataPath, dbName);
            return p;
        }

        public bool DatabaseExists(string dbName)
        {
            return Directory.Exists(GetDatabasePath(dbName));
        }

        public void CreateDatabase(string dbName)
        {
            Directory.CreateDirectory(GetDatabasePath(dbName));
        }
        
        public async Task<bool> ConnectDatabase(DbInfo dbInfo)
        {
            var dbInfoFilePath = Path.Combine(_options.DataPath, _options.DbInfoFile);
            await dbInfo.SaveAsync(dbInfoFilePath);
            return true;
        }

        public async Task<bool> DisconnectDatabase(DbInfo dbInfo)
        {
            var dbExists = DatabaseExists(dbInfo.CurrentDatabase ?? string.Empty);
            if (!dbExists)
            {
                return false;
            }
            var dbInfoFilePath = Path.Combine(_options.DataPath, _options.DbInfoFile);
            dbInfo.CurrentDatabase = string.Empty;
            await dbInfo.SaveAsync(dbInfoFilePath);
            return true;
        }

        public bool DropDatabase(string dbName)
        {
            var path = Path.Combine(_options.DataPath, dbName);
            if (!Directory.Exists(path))
            {
                return false;
            }
            Directory.Delete(path, true);
            return true;
        }

        public List<string?> ListDatabasesAsync()
        {
            var dbs = Directory.GetDirectories(_options.DataPath)
                .Select(Path.GetFileName)
                .ToList();
            return dbs;
        }
        
        public async Task<string?> GetCurrentDatabase()
        {
            var dbInfoFilePath = Path.Combine(GetDatabasePath(""), _options.DbInfoFile);
            if (File.Exists(dbInfoFilePath))
            {
                byte[] fileBytes = await File.ReadAllBytesAsync(dbInfoFilePath);
                var dbInfo = _serializer.Deserialize<DbInfo>(fileBytes);
                return dbInfo?.CurrentDatabase;
            }
            return null;
        }
    }
}
