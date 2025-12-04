using naivedb.core.configs;
using naivedb.core.serialization;
using naivedb.core.storage;
using naivedb.core.storage.pages;

namespace naivedb.core.engine
{
    public class Database
    {
        private readonly string _databaseDirectory;
        private readonly Dictionary<string, PagedFileStorageUsingBPT> _tables = [];
        private readonly DbOptions _options;
        private readonly MessagePackDataSerializer _serializer = new();

        public Database(string? databaseDirectory = null, DbOptions? options = null!)
        {
            options ??= new DbOptions();
            _options = options;
            _databaseDirectory = string.IsNullOrEmpty(databaseDirectory) ? _options.DataPath : Path.Combine(_options.DataPath, databaseDirectory); 
            Directory.CreateDirectory(_databaseDirectory);
        }

        public async Task CreateRecordAsync(string tableName, Row row)
        {
            /*
             * creating a new record:
             *  - check if table exists, if not create table
             *  - append to table
             *      - save to disk in row format
             *      - create index based on key i.e _id_ and build bpt
             *      - update in-mem index
             */
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

        public async Task DeleteRecordAsync(string tableName, long key)
        {
            var storage = GetTableStorage(tableName);
            await storage.DeleteAsync(key);
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

        private PagedFileStorageUsingBPT GetTableStorage(string table)
        {
            if (!_tables.ContainsKey(table))
                _tables[table] = new PagedFileStorageUsingBPT(_databaseDirectory, table, _options);
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

        public List<(string? name, string? date)> ListDatabasesAsync()
        {
            var data = Directory.GetDirectories(_options.DataPath)
                .Select(x => (
                    name: Path.GetFileName(x),
                    date: Directory.GetCreationTimeUtc(x).ToString("g")))
                .ToList();
            return data!;
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
        
        public async Task<Row?> GetRecordByKeyAsync(string tableName, long key)
        {
            var storage = GetTableStorage(tableName);
            return await storage.GetAsync(key);
        }
    }
}
