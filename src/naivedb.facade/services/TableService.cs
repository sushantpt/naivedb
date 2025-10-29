using naivedb.core.configs;
using naivedb.core.storage;

namespace naivedb.facade.services
{
    public class TableService
    {
        private readonly string _dbPath;
        private readonly DbOptions _options;

        public TableService(string dbPath, DbOptions options)
        {
            _dbPath = dbPath;
            _options = options;
        }

        public bool TableExists(string tableName)
        {
            return Directory.Exists(Path.Combine(_dbPath, tableName));
        }

        public void CreateTable(string tableName)
        {
            var dbPath = Path.Combine(_dbPath);
            if (!Directory.Exists(dbPath))
                throw new DirectoryNotFoundException($"Database '{_dbPath}' not found.");
            
            if (TableExists(tableName))
                throw new InvalidOperationException($"Table '{tableName}' already exists.");

            _ = new PagedFileStorage(_dbPath, tableName, _options);
        }

        public bool DropTable(string tableName)
        {
            var tablePath = Path.Combine(_dbPath, tableName);
            if (Directory.Exists(tablePath))
            {
                Directory.Delete(tablePath, true);
                return true;
            }
            return false;
        }

        public IEnumerable<string> ListTables()
        {
            if (!Directory.Exists(_dbPath))
                return Enumerable.Empty<string>();

            var fullPaths = Directory.GetDirectories(_dbPath);
            var folders = fullPaths.Select(x => Path.GetFileName(x)).ToList();
            
            return folders;
        }
    }
}