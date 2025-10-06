using System.Text.Json;
using naivedb.cli.presentation.constants;
using naivedb.cli.presentation.renderers;
using naivedb.core.configs;
using naivedb.core.engine;
using naivedb.core.storage;
using Spectre.Console;

namespace naivedb.cli.presentation.commands
{
    public class QueryCommand : ICommand
    {
        private readonly string _root;
        private readonly string _dbName;
        private readonly Database _db;
        private readonly DbOptions _options;

        public QueryCommand(string root, string dbName, DbOptions options)
        {
            _root = root;
            _dbName = dbName;
            _options = options;
            _db = new Database(Path.Combine(root, dbName), _options);
        }

        public async Task ExecuteAsync(string[] args)
        {
            if (args.Length == 0)
            {
                QueryRenderer.RenderHelp();
                return;
            }

            var action = args[0].ToLowerInvariant();

            switch (action)
            {
                case "create":
                    await CreateTable(args);
                    break;

                case "get":
                    await GetTable(args);
                    break;
                
                case "tables":
                    await DatabaseInfo(args);
                    break;

                case "add":
                    await AddRecord(args);
                    break;

                case "update":
                    await UpdateRecord(args);
                    break;
                
                case "delete":
                    await DeleteRecord(args);
                    break;

                default:
                    QueryRenderer.RenderHelp();
                    break;
            }
        }

        private async Task DatabaseInfo(string[] args)
        {
            var dbPath = Path.Combine(_root, _dbName);

            if (!Directory.Exists(dbPath))
            {
                AnsiConsole.MarkupLine($"[red]Database '{_dbName}' not found.[/]");
                return;
            }

            var tableFiles = Directory.GetFiles(dbPath, "*.json", SearchOption.TopDirectoryOnly);
            if (tableFiles.Length == 0)
            {
                AnsiConsole.MarkupLine($"[yellow]No tables found in database '{_dbName}'.[/]");
                return;
            }

            var table = new Table()
                .Border(TableBorder.Rounded)
                .BorderColor(Color.Blue)
                .AddColumn("[bold]Table Name[/]")
                .AddColumn("[bold]Tuple count[/]")
                .AddColumn("[bold]Size (KB)[/]")
                .AddColumn("[bold]Last Modified (UTC)[/]");

            foreach (var file in tableFiles)
            {
                var tableName = Path.GetFileNameWithoutExtension(file);
                var fileInfo = new FileInfo(file);
                int recordCount = 0;

                try
                {
                    var json = await File.ReadAllTextAsync(file);
                    if (!string.IsNullOrWhiteSpace(json))
                    {
                        var recordsCount = JsonSerializer.Deserialize<TablePage>(json, JsonSerializerHelper.Options)?.Header?.RecordCount;
                        
                        recordCount = recordsCount ?? 0;
                    }
                }
                catch
                {
                    recordCount = -1;
                }

                table.AddRow(
                    $"[green]{tableName}[/]",
                    recordCount == -1 ? "[red]Error[/]" : recordCount.ToString(),
                    (fileInfo.Length / 1024.0).ToString("F2"),
                    fileInfo.LastWriteTimeUtc.ToString("yyyy-MM-dd HH:mm:ss")
                );
            }

            AnsiConsole.MarkupLine($"[bold blue]Database:[/] {_dbName}");
            AnsiConsole.Write(table);
        }

        private Task DeleteRecord(string[] args)
        {
            var nameIndex = Array.IndexOf(args, "-n");
            var whereIndex = Array.IndexOf(args, "where");

            if (nameIndex == -1 || whereIndex == -1 ||
                nameIndex + 1 >= args.Length || whereIndex + 1 >= args.Length)
            {
                AnsiConsole.MarkupLine("[red]Usage:[/] query delete -n <table> where key==value");
                return Task.CompletedTask;
            }

            var tableName = args[nameIndex + 1];
            var condition = args[whereIndex + 1];

            var parts = condition.Split("==", StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length != 2)
            {
                AnsiConsole.MarkupLine("[red]Invalid where clause. Use: where key==value[/]");
                return Task.CompletedTask;
            }

            var key = parts[0].Trim();
            var value = parts[1].Trim();

            try
            {
                var resultSet = _db.ReadAll(tableName);
                var match = resultSet.Records.FirstOrDefault(r => r.ContainsKey(key) && r[key]?.ToString() == value);

                if (match == null)
                {
                    AnsiConsole.MarkupLine($"[yellow]No record found where {key} == {value}[/]");
                    return Task.CompletedTask;
                }

                _db.Delete(tableName, match.Key ?? string.Empty);
                AnsiConsole.MarkupLine($"[green]Record deleted from '{tableName}'.[/]");
            }
            catch (Exception ex)
            {
                AnsiConsole.MarkupLine($"[red]Failed to delete record: {ex.Message}[/]");
            }

            return Task.CompletedTask;
        }

        private Task CreateTable(string[] args)
        {
            var nameIndex = Array.IndexOf(args, "-n");
            if (nameIndex == -1 || nameIndex + 1 >= args.Length)
            {
                AnsiConsole.MarkupLine("[red]Usage:[/] query create -n <table>");
                return Task.CompletedTask;
            }

            var tableName = args[nameIndex + 1];
            var path = Path.Combine(_root, _dbName, $"{tableName}.json");

            if (File.Exists(path))
            {
                AnsiConsole.MarkupLine($"[yellow]Table '{tableName}' already exists.[/]");
                return Task.CompletedTask;
            }

            // use FileStorage to initialize
            var storage = new FileStorage(Path.Combine(_root, _dbName), tableName);
    
            AnsiConsole.MarkupLine($"[green]Table '{tableName}' created successfully.[/]");
            return Task.CompletedTask;
        }

        private Task GetTable(string[] args)
        {
            var nameIndex = Array.IndexOf(args, "-n");
            if (nameIndex == -1 || nameIndex + 1 >= args.Length)
            {
                AnsiConsole.MarkupLine("[red]Usage:[/] query get -n <table>");
                return Task.CompletedTask;
            }

            var tableName = args[nameIndex + 1];
            var showAll = args.Contains("-all", StringComparer.OrdinalIgnoreCase);
            
            var records = _db.ReadAll(tableName).Records
                .Select(r => FilterSystemFields(r, showAll))
                .ToList();

            QueryRenderer.RenderRecords(tableName, records);
            return Task.CompletedTask;
        }
        
        private Record FilterSystemFields(Record record, bool showAll)
        {
            if (showAll)
                return record;

            var filtered = new Record();
            foreach (var kv in record)
            {
                if (!kv.Key.StartsWith("naivedb_sys_", StringComparison.OrdinalIgnoreCase))
                    filtered[kv.Key] = kv.Value;
            }
            return filtered;
        }

        private Task AddRecord(string[] args)
        {
            var nameIndex = Array.IndexOf(args, "-n");
            var dataIndex = Array.IndexOf(args, "-data");

            if (nameIndex == -1 || dataIndex == -1 ||
                nameIndex + 1 >= args.Length || dataIndex + 1 >= args.Length)
            {
                AnsiConsole.MarkupLine("[red]Usage:[/] query add -n <table> -data '{json}'");
                return Task.CompletedTask;
            }

            var tableName = args[nameIndex + 1];
            var dataJson = args[dataIndex + 1];

            try
            {
                var tableMetadata = _db.GetTableMetadata(tableName);
                var lastId = tableMetadata.RecordCount;
                var record = new Record();
                var sysMeta = new Dictionary<string, object?>
                {
                    ["naivedb_sys_incremental_value"] = lastId + 1,
                    ["naivedb_sys_unique_value"] = Guid.CreateVersion7(),
                    ["naivedb_sys_device_info"] = Environment.MachineName,
                    ["naivedb_sys_timestamp_utc"] = DateTime.UtcNow.ToString("o"),
                    ["naivedb_sys_version"] = AppConstants.Version,
                };
                foreach (var kv in sysMeta) 
                    record[kv.Key] = kv.Value;
                
                var recordToAdd = JsonSerializer.Deserialize<Record>(dataJson);
                if (recordToAdd == null)
                {
                    AnsiConsole.MarkupLine("[red]Invalid JSON.[/]");
                    return Task.CompletedTask;
                }
                
                foreach (var kv in recordToAdd)
                    record[kv.Key] = kv.Value;

                _db.Create(tableName, record);
                AnsiConsole.MarkupLine($"[green]Record added to '{tableName}'.[/]");
            }
            catch (Exception ex)
            {
                AnsiConsole.MarkupLine($"[red]Failed to parse data: {ex.Message}[/]");
            }

            return Task.CompletedTask;
        }

        private Task UpdateRecord(string[] args)
        {
            var nameIndex = Array.IndexOf(args, "-n");
            var dataIndex = Array.IndexOf(args, "-data");
            var whereIndex = Array.IndexOf(args, "where");

            if (nameIndex == -1 || dataIndex == -1 || whereIndex == -1 ||
                nameIndex + 1 >= args.Length || dataIndex + 1 >= args.Length || whereIndex + 1 >= args.Length)
            {
                AnsiConsole.MarkupLine("[red]Usage:[/] query update -n <table> -data '{json}' where key==value");
                return Task.CompletedTask;
            }

            var tableName = args[nameIndex + 1];
            var dataJson = args[dataIndex + 1];
            var condition = args[whereIndex + 1];

            // parse predicate/where clause
            var parts = condition.Split("==", StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length != 2)
            {
                AnsiConsole.MarkupLine("[red]Invalid where clause. Use: where key==value[/]");
                return Task.CompletedTask;
            }

            var key = parts[0].Trim();
            var value = parts[1].Trim();

            try
            {
                var recordUpdate = JsonSerializer.Deserialize<Record>(dataJson);
                if (recordUpdate == null)
                {
                    AnsiConsole.MarkupLine("[red]Invalid JSON data.[/]");
                    return Task.CompletedTask;
                }

                var records = _db.ReadAll(tableName).Records;
                var match = records.FirstOrDefault(r => r.ContainsKey(key) && r[key]?.ToString() == value);

                if (match == null)
                {
                    AnsiConsole.MarkupLine($"[yellow]No record found where {key} == {value}[/]");
                    return Task.CompletedTask;
                }

                foreach (var kvp in recordUpdate)
                    match[kvp.Key] = kvp.Value;

                _db.Update(tableName, match);
                AnsiConsole.MarkupLine($"[green]Record updated in '{tableName}'.[/]");
            }
            catch (Exception ex)
            {
                AnsiConsole.MarkupLine($"[red]Failed to update: {ex.Message}[/]");
            }

            return Task.CompletedTask;
        }
    }
}