using naivedb.core.configs;
using naivedb.core.engine;
using naivedb.core.storage;
using System.Text.Json;
using naivedb.core.serialization;

namespace naivedb.facade.services
{
    public class QueryService
    {
        private readonly DbOptions _options;
        private readonly string _root;
        private readonly IDataSerializer _serializer = new MessagePackDataSerializer();

        private QueryService(DbOptions options, string root)
        {
            _options = options;
            _root = root;
        }

        public static QueryService Init(DbOptions options, string root)
        {
            return new QueryService(options, root);
        }

        public List<string> ListTables(string dbName)
        {
            var dbPath = Path.Combine(_root, dbName);
            if (!Directory.Exists(dbPath))
                throw new DirectoryNotFoundException($"Database '{dbName}' not found.");

            var files = Directory.GetFiles(dbPath, "*.dbp");
            return files.Select(Path.GetFileNameWithoutExtension).ToList();
        }

        public async Task<Dictionary<string, object>> GetTableDataAsync(string dbName, string tableName)
        {
            var dbPath = Path.Combine(_root, dbName);
            var tablePath = Path.Combine(dbPath, $"{tableName}.dbp");
            
            if (!File.Exists(tablePath))
                throw new FileNotFoundException($"Table '{tableName}' not found.");

            try
            {
                var tableDataBytes = await File.ReadAllBytesAsync(tablePath);
                var tableData = _serializer.Deserialize<Dictionary<string, object>>(tableDataBytes);
                return tableData ?? new Dictionary<string, object>();
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to read table data for '{tableName}': {ex.Message}");
            }
        }

        public async Task ImportTableDataAsync(string dbName, string tableName, JsonElement tableData)
        {
            var dbPath = Path.Combine(_root, dbName);
            if (!Directory.Exists(dbPath))
                throw new DirectoryNotFoundException($"Database '{dbName}' not found.");

            var tablePath = Path.Combine(dbPath, $"{tableName}.dbp");
            var tableJson = JsonSerializer.Serialize(tableData, new JsonSerializerOptions 
            { 
                WriteIndented = true 
            });

            await File.WriteAllTextAsync(tablePath, tableJson);
        }
    }
}