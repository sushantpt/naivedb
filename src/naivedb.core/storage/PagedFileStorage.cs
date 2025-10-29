using System.IO.Compression;
using naivedb.core.configs;
using naivedb.core.serialization;
using naivedb.core.storage.pages;
using naivedb.core.utils;

namespace naivedb.core.storage
{
    public class PagedFileStorage : IStorage
    {
        private readonly string _tableDirectory;
        private readonly DbOptions _options;
        private readonly MessagePackDataSerializer _serializer = new();

        public PagedFileStorage(string basePath, string tableName, DbOptions options)
        {
            _options = options;
            _tableDirectory = Path.Combine(basePath, tableName);
            Directory.CreateDirectory(_tableDirectory);
        }

        public async Task AppendAsync(Row row)
        {
            await WriteRowAsync(row, "insert");
        }

        public async Task BulkAppendAsync(IEnumerable<Row> rows)
        {
            foreach (var row in rows)
                await WriteRowAsync(row, "bulk-insert");
        }

        private async Task WriteRowAsync(Row row, string operation)
        {
            var currentPage = GetCurrentPage(); // latest page

            row["naivedb_sys_incremental_value"] = currentPage.Body.Count + 1;
            row["naivedb_sys_timestamp_utc"] = DateTime.UtcNow.ToString("o");

            row.NormalizeToValidTypes();
            var serializedMsgPackRow = _serializer.Serialize(row);

            var currentPagePath = Path.Combine(_tableDirectory, $"page_{currentPage.Header.PageNumber:D16}.dbp");
            var info = new FileInfo(currentPagePath);
            var currentSize = info.Exists ? info.Length : 0;
            
            if (currentSize + serializedMsgPackRow.Length > _options.PageSizeBytes)
            {
                await SavePage(currentPage);
                currentPage = CreateNewPage();
            }

            currentPage.Body.Add(row);
            currentPage.Header.RecordCount++;
            currentPage.Header.LastOperation = operation;
            currentPage.Header.LastUpdated = DateTime.UtcNow;
            
            currentPage.Footer.PageSizeBytes = currentSize + serializedMsgPackRow.Length;
            currentPage.Footer.WrittenAt = DateTime.UtcNow;

            await SavePage(currentPage);
        }

        public async IAsyncEnumerable<Row> ReadAllAsync()
        {
            foreach (var pagePath in Directory.GetFiles(_tableDirectory, "*.dbp").OrderBy(p => p))
            {
                byte[] fileBytes = await File.ReadAllBytesAsync(pagePath);

                if (_options.EnableCompression)
                {
                    using var input = new MemoryStream(fileBytes);
                    await using var brotli = new BrotliStream(input, CompressionMode.Decompress);
                    using var outStream = new MemoryStream();
                    await brotli.CopyToAsync(outStream);
                    fileBytes = outStream.ToArray();
                }
                var page = _serializer.Deserialize<TablePage>(fileBytes);
                if (page == null)
                    continue;
                var originalChecksum = page.Footer.Checksum;
                var checksumData = new { page.Header, page.Body };
                var dataBytes = _serializer.Serialize(checksumData);
                var computed = ChecksumUtils.ComputeCrc32C(dataBytes);

                if (computed != originalChecksum)
                    throw new InvalidDataException($"Checksum mismatch in {Path.GetFileName(pagePath)}");
                page.Footer.Checksum = originalChecksum;

                foreach (var row in page.Body)
                    yield return row;
            }
        }

        public async Task SaveAllAsync(List<Row> records, string lastOperation = "update")
        {
            var page = CreateNewPage();
            page.Body = records;
            page.Header.RecordCount = records.Count;
            page.Header.LastOperation = lastOperation;
            page.Footer.WrittenAt = DateTime.UtcNow;
            await SavePage(page);
        }

        private TablePage GetCurrentPage()
        {
            var latest = Directory.GetFiles(_tableDirectory, "*.dbp")
                .OrderByDescending(p => p)
                .FirstOrDefault();

            if (latest == null)
                return CreateNewPage();

            byte[] fileBytes = File.ReadAllBytes(latest);

            if (_options.EnableCompression)
            {
                using var input = new MemoryStream(fileBytes);
                using var brotli = new BrotliStream(input, CompressionMode.Decompress);
                using var outStream = new MemoryStream();
                brotli.CopyTo(outStream);
                fileBytes = outStream.ToArray();
            }

            var page = _serializer.Deserialize<TablePage>(fileBytes);
            if (page == null)
                return CreateNewPage();

            return page;
        }

        public async Task<PageHeader> GetMetadataAsync()
        {
            var latestPage = Directory.GetFiles(_tableDirectory, "*.dbp")
                .OrderByDescending(p => p)
                .FirstOrDefault();

            if (latestPage == null)
                return new PageHeader { TableName = Path.GetFileName(_tableDirectory) };

            byte[] fileBytes = await File.ReadAllBytesAsync(latestPage);

            if (_options.EnableCompression)
            {
                using var input = new MemoryStream(fileBytes);
                await using var brotli = new BrotliStream(input, CompressionMode.Decompress);
                using var outStream = new MemoryStream();
                await brotli.CopyToAsync(outStream);
                fileBytes = outStream.ToArray();
            }

            var page = _serializer.Deserialize<TablePage>(fileBytes);
            return page?.Header ?? new PageHeader();
        }


        private TablePage CreateNewPage()
        {
            int nextPageNum = Directory.GetFiles(_tableDirectory, "*.dbp").Length + 1;
            return new TablePage
            {
                Header = new PageHeader
                {
                    TableName = Path.GetFileName(_tableDirectory),
                    PageNumber = nextPageNum,
                    Serializer = _serializer.Format,
                    CreatedAt = DateTime.UtcNow
                },
                Body = new List<Row>(),
                Footer = new PageFooter()
            };
        }

        private async Task SavePage(TablePage page)
        {
            var checksumData = new { page.Header, page.Body };
            var dataBytes = _serializer.Serialize(checksumData);
            var checksum = ChecksumUtils.ComputeCrc32C(dataBytes);
    
            page.Footer.Checksum = checksum;
            page.Footer.PageSizeBytes = dataBytes.Length;
            page.Footer.WrittenAt = DateTime.UtcNow;

            var finalBytes = _serializer.Serialize(page);
    
            if (_options.EnableCompression)
            {
                using var ms = new MemoryStream();
                await using (var brotli = new BrotliStream(ms, CompressionLevel.Optimal, true))
                    brotli.Write(finalBytes, 0, finalBytes.Length);
                finalBytes = ms.ToArray();
            }

            var path = Path.Combine(_tableDirectory, $"page_{page.Header.PageNumber:D16}.dbp");
            await File.WriteAllBytesAsync(path, finalBytes);
        }
    }
}
