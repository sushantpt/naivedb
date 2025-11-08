using naivedb.core.configs;
using naivedb.core.engine.bpt;
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
        private readonly BPlusTree<string, long> _bPlusTree; // key -> page
        
        public PagedFileStorageUsingBPT(string basePath, string tableName, DbOptions options)
        {
            _options = options;
            _tableDirectory = Path.Combine(basePath, tableName);
            Directory.CreateDirectory(_tableDirectory);

            string indexPath = Path.Combine(_tableDirectory, "index");
            _bPlusTree = new BPlusTree<string, long>(indexPath);
        }
        
        
        public async Task AppendAsync(Row row)
        {
            ArgumentNullException.ThrowIfNull(row, nameof(row));
            var currentPage = GetCurrentPage(); // actual data to be saved in incremental row structure
            
            row["naivedb_sys_incremental_value"] = currentPage.Body.Count + 1;
            row["naivedb_sys_timestamp_utc"] = DateTime.UtcNow.ToString("o");
            row["uid"] = Guid.NewGuid().ToString("N");
            
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

            // update index i.e. key -> page
            string key = row.Key;
            await _bPlusTree.Add(key, currentPage.Header.PageNumber);

            await SavePage(currentPage);
            await _bPlusTree.SaveAsync();
        }
        
        public async Task<Row?> GetAsync(string key)
        {
            var pageNumber = await _bPlusTree.GetAsync(key);
            if (pageNumber == null || pageNumber == 0)
                return null;

            string pagePath = Path.Combine(_tableDirectory, $"page_{pageNumber:D16}.dbp");
            if (!File.Exists(pagePath)) return null;

            byte[] fileBytes = await File.ReadAllBytesAsync(pagePath);
            var page = _serializer.Deserialize<TablePage>(fileBytes);
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
    }
}