using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using naivedb.core.storage.pages;
using naivedb.core.utils;

namespace naivedb.core.storage
{
    [Obsolete("use paged file storage which uses messagepack and is paged", true)]
    /// <summary>
    /// Handles low-level storage operations for a table file.
    /// Stores data as a structured JSON page (header, body, footer).
    /// </summary>
    public class JsonFileStorage : IStorage
    {
        private readonly string _directory;
        private readonly string _tableName;
        private readonly string _tablePath;

        public JsonFileStorage(string directory, string tableName)
        {
            _directory = directory;
            _tableName = tableName;
            _tablePath = Path.Combine(_directory, $"{_tableName}.json");

            Directory.CreateDirectory(_directory);

            if (!File.Exists(_tablePath))
                InitializeEmptyPage();
        }

        public async Task AppendAsync(Row row)
        {
            var page = await LoadPage();
            
            row["naivedb_sys_incremental_value"] = page.Body.Count;
            row["naivedb_sys_timestamp_utc"] = DateTime.UtcNow.ToString("o");

            page.Body.Add(row);

            page.Header.OperationCount++;
            page.Header.RecordCount = page.Body.Count;
            page.Header.LastOperation = "insert";
            page.Header.LastUpdated = DateTime.UtcNow;

            await SavePage(page);
        }

        public async Task BulkAppendAsync(IEnumerable<Row> rows)
        {
            foreach(var row in rows)
                await AppendAsync(row);
        }

        public async IAsyncEnumerable<Row> ReadAllAsync()
        {
            var page = await LoadPage();
            foreach (var row in page.Body)
                yield return row;
        }

        public async Task SaveAllAsync(List<Row> records, string lastOperation = "update")
        {
            var page = await LoadPage();

            page.Body = records;
            page.Header.OperationCount++;
            page.Header.RecordCount = records.Count;
            page.Header.LastUpdated = DateTime.UtcNow;
            page.Header.LastOperation = lastOperation;

            await SavePage(page);
        }

        public async Task<PageHeader> GetMetadataAsync()
        {
            var page = await LoadPage();
            return page.Header;
        }

        private void InitializeEmptyPage()
        {
            var page = new TablePage
            {
                Header = new PageHeader
                {
                    TableName = _tableName,
                    CreatedAt = DateTime.UtcNow,
                    RecordCount = 0,
                    OperationCount = 1,
                    LastOperation = "create"
                },
                Body = new List<Row>(),
                Footer = new PageFooter
                {
                    WrittenAt = DateTime.UtcNow,
                    PageSizeBytes = 0,
                    Checksum = string.Empty
                }
            };

            var json = JsonSerializer.Serialize(page, JsonSerializerHelper.Options);
            File.WriteAllText(_tablePath, json);
        }

        private async Task<TablePage> LoadPage()
        {
            var json = await File.ReadAllTextAsync(_tablePath);
            if (string.IsNullOrWhiteSpace(json))
                return new TablePage { Header = new PageHeader { TableName = _tableName } };

            return JsonSerializer.Deserialize<TablePage>(json, JsonSerializerHelper.Options)
                   ?? new TablePage { Header = new PageHeader { TableName = _tableName } };
        }

        private async Task SavePage(TablePage page)
        {
            var bodyJson = JsonSerializer.Serialize(page.Body, JsonSerializerHelper.Options);
            page.Footer.Checksum = ComputeChecksum(bodyJson);
            page.Footer.PageSizeBytes = Encoding.UTF8.GetByteCount(bodyJson);
            page.Footer.WrittenAt = DateTime.UtcNow;

            var json = JsonSerializer.Serialize(page, JsonSerializerHelper.Options);
            await File.WriteAllTextAsync(_tablePath, json);
        }

        private static string ComputeChecksum(string data)
        {
            using var sha = SHA256.Create();
            var hash = sha.ComputeHash(Encoding.UTF8.GetBytes(data));
            return BitConverter.ToString(hash).Replace("-", "").Substring(0, 16);
        }
    }
}
