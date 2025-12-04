using naivedb.core.configs;
using naivedb.core.engine.bpt;
using naivedb.core.indexing;
using naivedb.core.serialization;
using naivedb.core.storage.pages;
using naivedb.core.utils;

namespace naivedb.core.storage
{
    /// <summary>
    /// using bpt will internally save data as previously i.e. paged and in row structure, but will maintain a bpt for each db operations
    /// </summary>
    public class PagedFileStorageUsingBPT : IStorage
    {
        private readonly string _tableDirectory;
        private readonly DbOptions _options;
        private readonly MessagePackDataSerializer _serializer = new();
        private readonly BPlusTree<long, long> _bPlusTree; // key (id) -> page 
        private readonly InMemoryIndexManager _inMemIndexes = InMemoryIndexManager.Instance;
        private long _nextId;
        private readonly string _sequencePath;
        
        public PagedFileStorageUsingBPT(string basePath, string tableName, DbOptions options)
        {
            _options = options;
            _tableDirectory = Path.Combine(basePath, tableName);
            Directory.CreateDirectory(_tableDirectory);
            _sequencePath = Path.Combine(_tableDirectory, "_seq");
            if (File.Exists(_sequencePath))
            {
                var content = File.ReadAllText(_sequencePath);
                _nextId = long.TryParse(content, out var v) ? v : 0;
            }
            else
            {
                _nextId = 0;
            }
            
            string indexPath = Path.Combine(_tableDirectory, "index");
            _bPlusTree = new BPlusTree<long, long>(indexPath);
            _bPlusTree.Load();
        }
        
        
        public async Task AppendAsync(Row row)
        {
            ArgumentNullException.ThrowIfNull(row);
            var currentPage = GetCurrentPage(); // actual data to be saved in incremental row structure
            var id = await GetNextIdAsync();
            
            row["naivedb_sys_incremental_value"] = currentPage.Body.Count + 1; // per page incremental value
            row["naivedb_sys_timestamp_utc"] = DateTime.UtcNow.ToString("o");
            row["uid"] = Guid.NewGuid().ToString("N");
            row["_id_"] = id; // table global incremental id
            
            row.NormalizeToValidTypes();
            var rowBytes = _serializer.Serialize(row);
            var currentPagePath = Path.Combine(_tableDirectory, $"page_{currentPage.Header.PageNumber:D16}.dbp");
            var info = new FileInfo(currentPagePath);
            var currentSize = info.Exists ? info.Length : 0;

            if (currentSize + rowBytes.Length > _options.PageSizeBytes)
            {
                await SavePage(currentPage);
                currentPage = CreateNewPage();
            }

            currentPage.Body.Add(row);
            currentPage.Header.RecordCount++;
            currentPage.Header.LastUpdated = DateTime.UtcNow;
            
            currentPage.Footer.PageSizeBytes = currentSize + rowBytes.Length;
            currentPage.Footer.WrittenAt = DateTime.UtcNow;
            /*currentPage.Footer.Checksum = ChecksumUtils.ComputeChecksum(rowBytes);*/

            // update index i.e. key/id -> page
            await _bPlusTree.Add(id, currentPage.Header.PageNumber);

            await SavePage(currentPage); // save to actual physical layer
            await _bPlusTree.SaveAsync(); // save to build index (as node)

            // in-mem index update
            var ptr = new RowPointer($"page_{currentPage.Header.PageNumber:D16}", currentPage.Body.Count - 1);
            _inMemIndexes.AddOrUpdateIndex(id, ptr); // save to in-mem
            await File.WriteAllTextAsync(_sequencePath, _nextId.ToString()); // save next incremental id
        }
        
        public async Task<Row?> GetAsync(long key)
        {
            string pagePath;
            byte[] fileBytes;
            TablePage? page;

            // check in-mem index first
            if (_inMemIndexes.TryGetIndex(key, out var ptr) && ptr != null)
            {
                pagePath = Path.Combine(_tableDirectory, $"{ptr.NodeId}.dbp");
                if (!File.Exists(pagePath)) return null;

                fileBytes = await File.ReadAllBytesAsync(pagePath);
                page = _serializer.Deserialize<TablePage>(fileBytes);
                return page?.Body.ElementAtOrDefault(ptr.SlotIndex);
            }
            
            // fall back to bpt index
            var pageNumber = await _bPlusTree.GetAsync(key);
            if (pageNumber <= 0)
                return null;

            pagePath = Path.Combine(_tableDirectory, $"page_{pageNumber:D16}.dbp");
            if (!File.Exists(pagePath)) return null;

            fileBytes = await File.ReadAllBytesAsync(pagePath);
            page = _serializer.Deserialize<TablePage>(fileBytes);
            return page?.Body.FirstOrDefault(r => r.Key == key);
        }

        public async Task BulkAppendAsync(IEnumerable<Row> rows)
        {
            // todo -> batch/journaling/... be more efficient
            foreach (var row in rows)
                await AppendAsync(row);
        }

        public async IAsyncEnumerable<Row> ReadAllAsync()
        {
            var pageFiles = Directory.GetFiles(_tableDirectory, "*.dbp")
                .OrderBy(f => f);

            foreach (var pagePath in pageFiles)
            {
                var fileBytes = await File.ReadAllBytesAsync(pagePath);
                var page = _serializer.Deserialize<TablePage>(fileBytes);
                if (page == null) continue;

                foreach (var row in page.Body)
                    yield return row;
            }
        }

        public async Task SaveAllAsync(List<Row> records, string lastOperation = "update")
        {
            if (records == null || records.Count == 0) 
                return;

            // remove old pages
            foreach (var file in Directory.GetFiles(_tableDirectory, "*.dbp"))
                File.Delete(file);

            var page = CreateNewPage();
            long accumulatedSize = 0;

            foreach (var row in records)
            {
                row.NormalizeToValidTypes();
                var rowBytes = _serializer.Serialize(row);

                if (accumulatedSize + rowBytes.Length > _options.PageSizeBytes)
                {
                    await SavePage(page);
                    page = CreateNewPage();
                    accumulatedSize = 0;
                }

                page.Body.Add(row);
                page.Header.RecordCount++;
                accumulatedSize += rowBytes.Length;

                // update bpt index
                await _bPlusTree.Add(row.Key, page.Header.PageNumber);
            }

            if (page.Body.Count > 0)
                await SavePage(page);

            await _bPlusTree.SaveAsync();
        }

        public async Task<PageHeader> GetMetadataAsync()
        {
            var latestPage = Directory.GetFiles(_tableDirectory, "*.dbp")
                .OrderByDescending(f => f)
                .FirstOrDefault();

            if (latestPage == null)
                return new PageHeader { TableName = Path.GetFileName(_tableDirectory) };

            var fileBytes = await File.ReadAllBytesAsync(latestPage);
            var page = _serializer.Deserialize<TablePage>(fileBytes);
            return page?.Header ?? new PageHeader();
        }
        
        private TablePage GetCurrentPage()
        {
            var latest = Directory.GetFiles(_tableDirectory, "*.dbp")
                .OrderByDescending(f => f)
                .FirstOrDefault();
            if (latest == null) return CreateNewPage();

            var bytes = File.ReadAllBytes(latest);
            var page = _serializer.Deserialize<TablePage>(bytes);
            return page ?? CreateNewPage();
        }

        private TablePage CreateNewPage()
        {
            int nextPageNum = Directory.GetFiles(_tableDirectory, "*.dbp").Length + 1;
            return new TablePage
            {
                Header = new PageHeader
                {
                    PageNumber = nextPageNum,
                    TableName = Path.GetFileName(_tableDirectory),
                    CreatedAt = DateTime.UtcNow
                },
                Body = new List<Row>(),
                Footer = new PageFooter()
            };
        }

        private async Task SavePage(TablePage page)
        {
            var bytes = _serializer.Serialize(page);
            string path = Path.Combine(_tableDirectory, $"page_{page.Header.PageNumber:D16}.dbp");
            await File.WriteAllBytesAsync(path, bytes);
        }

        public async Task DeleteAsync(long key)
        {
            if (!_inMemIndexes.TryGetIndex(key, out RowPointer? ptr) || ptr == null)
            {
                var pageNumber = await _bPlusTree.GetAsync(key);
                if (pageNumber <= 0)
                    throw new Exception($"Key {key} not found in the index.");
                ptr = new RowPointer($"page_{pageNumber:D16}", -1); // row index unknown
            }

            var pagePath = Path.Combine(_tableDirectory, $"{ptr.NodeId}.dbp");
            if (!File.Exists(pagePath))
                throw new Exception($"Data page {ptr.NodeId} not found.");
            var fileBytes = await File.ReadAllBytesAsync(pagePath);
            var page = _serializer.Deserialize<TablePage>(fileBytes);
            if (page == null)
                throw new Exception($"Failed to deserialize data page {ptr.NodeId}.");

            var index = page.Body.FindIndex(r => r.Key == key);
            if (index == -1)
                throw new Exception($"Key {key} not found in data page {ptr.NodeId}.");
            page.Body.RemoveAt(index);
            page.Header.RecordCount = page.Body.Count;
            page.Header.LastUpdated = DateTime.UtcNow;
            page.Footer.WrittenAt = DateTime.UtcNow;

            var tmpPath = pagePath + ".tmp";
            await File.WriteAllBytesAsync(tmpPath, _serializer.Serialize(page));
            File.Move(tmpPath, pagePath, true);

            await _bPlusTree.DeleteAsync(key);
            await _bPlusTree.SaveAsync();
            _inMemIndexes.RemoveIndex(key);
        }
        
        public async IAsyncEnumerable<Row> FindRangeAsync(long startKey, long endKey)
        {
            await foreach(var pageNumber in _bPlusTree.TraverseRangeAsync(startKey, endKey))
            {
                var pagePath = Path.Combine(_tableDirectory, $"page_{pageNumber:D16}.dbp");
                if (!File.Exists(pagePath))
                    continue;

                var fileBytes = await File.ReadAllBytesAsync(pagePath);
                var page = _serializer.Deserialize<TablePage>(fileBytes);
                if (page == null)
                    continue;

                foreach (var row in page.Body)
                {
                    if (row.Key > startKey && row.Key < endKey)
                    {
                        yield return row;
                    }
                }
            }
        }
        
        public async Task LoadBptAsync() => await _bPlusTree.LoadAsync();
        public string TableDirectory => _tableDirectory;
        public void AddToInMemoryIndex(long key, RowPointer ptr)
        {
            _inMemIndexes.AddOrUpdateIndex(key, ptr);
        }
        
        private async Task<long> GetNextIdAsync()
        {
            var next = Interlocked.Increment(ref _nextId);
            await File.WriteAllTextAsync(_sequencePath, next.ToString());
            return next;
        }
    }
}