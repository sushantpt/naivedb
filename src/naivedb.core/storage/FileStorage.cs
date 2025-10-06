using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace naivedb.core.storage
{
    /// <summary>
    /// Handles low-level storage operations for a table file.
    /// Stores data as a structured JSON page (header, body, footer).
    /// </summary>
    public class FileStorage : IStorage
    {
        private readonly string _directory;
        private readonly string _tableName;
        private readonly string _tablePath;

        public FileStorage(string directory, string tableName)
        {
            _directory = directory;
            _tableName = tableName;
            _tablePath = Path.Combine(_directory, $"{_tableName}.json");

            if (!Directory.Exists(_directory))
                Directory.CreateDirectory(_directory);

            if (!File.Exists(_tablePath))
                InitializeEmptyPage();
        }

        public void Append(Record record)
        {
            var page = LoadPage();
            
            record["naivedb_sys_incremental_value"] = page.Body.Count;
            record["naivedb_sys_timestamp_utc"] = DateTime.UtcNow.ToString("o");

            page.Body.Add(record);

            page.Header.OperationCount++;
            page.Header.RecordCount = page.Body.Count;
            page.Header.LastOperation = "insert";
            page.Header.LastUpdated = DateTime.UtcNow;

            SavePage(page);
        }

        public IEnumerable<Record> ReadAll()
        {
            var page = LoadPage();
            return page.Body;
        }

        public void SaveAll(List<Record> records, string lastOperation = "update")
        {
            var page = LoadPage();

            page.Body = records;
            page.Header.OperationCount++;
            page.Header.RecordCount = records.Count;
            page.Header.LastUpdated = DateTime.UtcNow;
            page.Header.LastOperation = lastOperation;

            SavePage(page);
        }

        public PageMetadata GetMetadata()
        {
            var page = LoadPage();
            return page.Header;
        }

        private void InitializeEmptyPage()
        {
            var page = new TablePage
            {
                Header = new PageMetadata
                {
                    TableName = _tableName,
                    CreatedAt = DateTime.UtcNow,
                    RecordCount = 0,
                    OperationCount = 1,
                    LastOperation = "create"
                },
                Body = new List<Record>(),
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

        private TablePage LoadPage()
        {
            var json = File.ReadAllText(_tablePath);
            if (string.IsNullOrWhiteSpace(json))
                return new TablePage { Header = new PageMetadata { TableName = _tableName } };

            return JsonSerializer.Deserialize<TablePage>(json, JsonSerializerHelper.Options)
                ?? new TablePage { Header = new PageMetadata { TableName = _tableName } };
        }

        private void SavePage(TablePage page)
        {
            var bodyJson = JsonSerializer.Serialize(page.Body, JsonSerializerHelper.Options);
            page.Footer.Checksum = ComputeChecksum(bodyJson);
            page.Footer.PageSizeBytes = Encoding.UTF8.GetByteCount(bodyJson);
            page.Footer.WrittenAt = DateTime.UtcNow;

            var json = JsonSerializer.Serialize(page, JsonSerializerHelper.Options);
            File.WriteAllText(_tablePath, json);
        }

        private static string ComputeChecksum(string data)
        {
            using var sha = SHA256.Create();
            var hash = sha.ComputeHash(Encoding.UTF8.GetBytes(data));
            return BitConverter.ToString(hash).Replace("-", "").Substring(0, 16);
        }
    }
}
