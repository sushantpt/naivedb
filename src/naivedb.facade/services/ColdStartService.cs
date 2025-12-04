using naivedb.core.coldstart;
using naivedb.core.configs;
using naivedb.core.logger;
using naivedb.core.storage;

namespace naivedb.facade.services
{
    public class ColdStartService
    {
        private readonly DbOptions _options;
        
        public ColdStartService(DbOptions options)
        {
            _options = options;
        }
        
        public async void StartColdStartBackground()
        {
            string dataPath = _options.DataPath;
            var dbService = DatabaseService.Init(_options, dataPath);
            var currentDb = await dbService.GetCurrentDatabase();
            if (string.IsNullOrWhiteSpace(currentDb)) 
                return;
            
            var databasePath = dbService.GetDatabasePath(currentDb);
            
            _ = Task.Run(async () =>
            {
                try
                {
                    await LoadDatabaseIntoMemoryAsync(databasePath, currentDb);
                }
                catch (Exception ex)
                {
                    QueryLogger.InitializeWithDbOption(_options);
                    await QueryLogger.GenericLogAsync(
                        dbName: "coldstart",
                        operation: "LoadDatabaseIntoMemory",
                        status: "Failed",
                        extraInfo: "Background cold-start failed.",
                        ex: ex
                    );
                }
            });
        }
        
        private async Task LoadDatabaseIntoMemoryAsync(string databasePath, string currentDatabase)
        {
            var tables = await DiscoverTablesAsync(currentDatabase);
            ColdStartManager.Init(databasePath);
            await ColdStartManager.LoadIntoMemoryAsync(tables);
        }
        
        private async Task<Dictionary<string, PagedFileStorageUsingBPT>> DiscoverTablesAsync(string currentDatabase)
        {
            var tables = new Dictionary<string, PagedFileStorageUsingBPT>();
            if (!Directory.Exists(_options.DataPath))
                return tables;

            var databaseDir = Path.Combine(_options.DataPath, currentDatabase);
            var tableDirs = Directory.GetDirectories(databaseDir);
            if (tableDirs.Length == 0) 
                return tables;
            foreach (var table in tableDirs)
            {
                var tableName = Path.GetFileName(table);
                if (!string.IsNullOrWhiteSpace(tableName))
                {
                    tables[tableName] = new PagedFileStorageUsingBPT(databaseDir, tableName, _options);
                }
            }
            return tables;
        }
    }
}