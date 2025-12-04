using naivedb.core.configs;
using naivedb.core.indexing;
using naivedb.core.serialization;
using naivedb.core.storage;
using naivedb.core.storage.pages;

namespace naivedb.core.coldstart
{
    public static class ColdStartManager
    {
        private static string? _databasePath;
        private static readonly MessagePackDataSerializer Serializer = new();

        public static void Init(string dbPath)
        {
            _databasePath = dbPath;
        }
        
        public static async Task LoadIntoMemoryAsync(Dictionary<string, PagedFileStorageUsingBPT> tables = null!)
        {
            ArgumentNullException.ThrowIfNull(_databasePath, "Database path not initialized.");
            if (tables == null!) return;
            
            /*
             * 1. find/load all tables
             * 2. build in-mem indexes
             */
            var tableDirs = Directory.GetDirectories(_databasePath);
            foreach (var tableDir in tableDirs)
            {
                var tableName = Path.GetFileName(tableDir);
                if(string.IsNullOrEmpty(tableName)) continue;
                if (!tables.ContainsKey(tableName))
                {
                    tables[tableName] = new PagedFileStorageUsingBPT(_databasePath, tableName, new DbOptions());
                }
                
                var storage = tables[tableName];
                await storage.LoadBptAsync();
                await RebuildInMemoryIndexesAsync(storage);
            }
            
        }

        private static async Task RebuildInMemoryIndexesAsync(PagedFileStorageUsingBPT tableStorage)
        {
            var pageFiles = Directory.GetFiles(tableStorage.TableDirectory, "*.dbp").OrderBy(p => p);
            foreach (var pageFile in pageFiles)
            {
                var bytes = await File.ReadAllBytesAsync(pageFile);
                var page = Serializer.Deserialize<TablePage>(bytes);
                if (page == null) continue;
                for (int slotIndex = 0; slotIndex < page.Body.Count; slotIndex++)
                {
                    Row item = page.Body[slotIndex];
                    var ptr = new RowPointer(Path.GetFileNameWithoutExtension(pageFile), slotIndex);
                    tableStorage.AddToInMemoryIndex(item.Key, ptr);
                }
            }
        }
    }
}