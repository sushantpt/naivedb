using naivedb.core.configs;
using naivedb.core.engine;
using naivedb.core.logger;
using naivedb.core.storage.pages;

namespace naivedb.facade.services
{
    public class DatabaseService
    {
        private readonly DbOptions _options;
        private readonly bool _isInit = false;
        private readonly Database _db;

        private DatabaseService(DbOptions options, string root, bool isInit = false)
        {
            _options = options;
            _isInit = isInit;
            _db = new Database(root, options);
        }
        
        public static DatabaseService Init(DbOptions options, string root)
        {
            return new DatabaseService(options, root, true);
        }

        public string GetDatabasePath(string dbName)
        {
            if (!_isInit) throw new InvalidOperationException("DatabaseService not initialized.");
            return _db.GetDatabasePath(dbName);
        }

        public async Task<PageHeader> GetTableMetadata(string tableName)
        {
            if (!_isInit) throw new InvalidOperationException("DatabaseService not initialized.");
            return await _db.GetTableMetadataAsync(tableName);
        }

        public bool DatabaseExists(string dbName)
        {
            if (!_isInit) throw new InvalidOperationException("DatabaseService not initialized.");
            return _db.DatabaseExists(dbName);
        }

        public bool CreateDatabase(string dbName)
        {
            if (!_isInit) throw new InvalidOperationException("DatabaseService not initialized.");

            try
            {
                _db.CreateDatabase(dbName);
                return true;
            }
            catch (Exception e)
            {
                return false;
            }
        }

        public async Task<bool> ConnectDatabase(DbInfo dbInfo)
        {
            if (!_isInit) throw new InvalidOperationException("DatabaseService not initialized.");
            try
            {
                return await _db.ConnectDatabase(dbInfo);
            }
            catch (Exception e)
            {
                // log
                QueryLogger.InitializeWithDbOption(_options);
                await QueryLogger.GenericLogAsync("", "query", null, "Failed", "Failed to connect database.");
                return false;
            }
        }

        public async Task<bool> DisconnectDatabase(DbInfo dbInfo)
        {
            if (!_isInit) throw new InvalidOperationException("DatabaseService not initialized.");
            try
            {
                return await _db.DisconnectDatabase(dbInfo);
            }
            catch (Exception e)
            {
                return false;
            }
        }

        public bool DropDatabase(string dbName)
        {
            if (!_isInit) throw new InvalidOperationException("DatabaseService not initialized.");
            try
            {
                return _db.DropDatabase(dbName);
            }
            catch (Exception e)
            {
                return false;
            }
        }

        public List<string?> ListDatabasesAsync()
        {
            if (!_isInit) throw new InvalidOperationException("DatabaseService not initialized.");
            return _db.ListDatabasesAsync();
        }
        
        public async Task<string?> GetCurrentDatabase()
        {
            if (!_isInit) throw new InvalidOperationException("DatabaseService not initialized.");
            try
            {
                return await _db.GetCurrentDatabase();
            }
            catch
            {
                // log
                QueryLogger.InitializeWithDbOption(_options);
                await QueryLogger.GenericLogAsync("", "query", null, "Failed", "Failed to get current database.");
            }
            return null;
        }
    }
}