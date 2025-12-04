using System.Text.Json;
using naivedb.cli.presentation.renderers;
using naivedb.core.configs;
using naivedb.core.constants;
using naivedb.core.engine;
using naivedb.core.storage;
using naivedb.core.storage.pages;
using naivedb.core.utils;
using naivedb.facade.services;
using Spectre.Console;

namespace naivedb.cli.query.commands
{
    public class QueryCommand : ICommand
    {
        private readonly string _root;
        private readonly string _dbName;
        private readonly Database _db;
        private readonly DbOptions _options;
        private readonly TableService _tableService;

        public QueryCommand(string root, string dbName, DbOptions options)
        {
            _root = root;
            _dbName = dbName;
            _options = options;
            _db = new Database(root, options);
            _tableService = new TableService(root, options);
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
                
                case "info":
                    await TableInfo(args);
                    break;

                case "add":
                    await AddRecord(args);
                    break;

                case "update":
                    await UpdateRecord(args);
                    break;
                
                case "delete":
                    if (args.Contains("by", StringComparer.OrdinalIgnoreCase) && args.Contains("key", StringComparer.OrdinalIgnoreCase))
                        await DeleteRecordByKey(args);
                    else if (args.Contains("range", StringComparer.OrdinalIgnoreCase))
                        await DeleteRecordRange(args);
                    else
                        await DeleteRecord(args);
                    break;
                
                case "drop":
                    await DropTable(args);
                    break;

                case "range":
                    await RangeQuery(args);
                    break;

                default:
                    QueryRenderer.RenderHelp();
                    break;
            }
        }

        private async Task RangeQuery(string[] args)
        {
            var nameIndex = Array.IndexOf(args, "-n");
            var fromIndex = Array.IndexOf(args, "-from");
            var toIndex = Array.IndexOf(args, "-to");
            if (nameIndex == -1 || fromIndex == -1 || toIndex == -1 ||
                nameIndex + 1 >= args.Length || fromIndex + 1 >= args.Length || toIndex + 1 >= args.Length)
            {
                AnsiConsole.MarkupLine("[red]Usage:[/] query range -n <table> -from <start> -to <end>[/]");
                return;
            }
            var tableName = args[nameIndex + 1];
            var startKey = args[fromIndex + 1];
            var endKey = args[toIndex + 1];
            
            if(!long.TryParse(startKey, out var startKeyLong) || !long.TryParse(endKey, out var endKeyLong))
            {
                AnsiConsole.MarkupLine("[red]Invalid key. Use: long or int[/]");
                return;
            }

            var storage = new PagedFileStorageUsingBPT(_root, tableName, _options);
            var records = new List<Row>();
            await foreach (var record in storage.FindRangeAsync(startKeyLong, endKeyLong))
                records.Add(record);

            if (records.Count == 0)
            {
                AnsiConsole.MarkupLine($"[yellow]No records found in range {startKey} to {endKey}.[/]");
                return;
            }
            QueryRenderer.RenderRecords(tableName, records);
        }

        private async Task DatabaseInfo(string[] args)
        {
            var dbPath = _root;

            if (!Directory.Exists(dbPath))
            {
                AnsiConsole.MarkupLine($"[red]Database '{_dbName}' not found.[/]");
                return;
            }

            var tableFiles = _tableService.ListTables().ToList();
            if (tableFiles.Count == 0)
            {
                AnsiConsole.MarkupLine($"[yellow]No tables found in database '{_dbName}'.[/]");
                return;
            }

            var table = new Table()
                .Border(TableBorder.Rounded)
                .BorderColor(Color.Blue)
                .AddColumn("[bold]Table Name[/]")
                ;

            foreach (var file in tableFiles)
            {
                var tableName = Path.GetFileNameWithoutExtension(file);
                table.AddRow($"[green]{tableName}[/]");
            }

            AnsiConsole.MarkupLine($"[bold blue]Database:[/] {_dbName}");
            AnsiConsole.Write(table);
        }
        
        private async Task TableInfo(string[] args)
        {
            var dbPath = _root;

            if (!Directory.Exists(dbPath))
            {
                AnsiConsole.MarkupLine($"[red]Database '{_dbName}' not found.[/]");
                return;
            }

            var tableFiles = _tableService.ListTables().ToList();
            if (tableFiles.Count == 0)
            {
                AnsiConsole.MarkupLine($"[yellow]No tables found in database '{_dbName}'.[/]");
                return;
            }
            var nameIndex = Array.IndexOf(args, "-n");
            if (nameIndex == -1 || nameIndex + 1 >= args.Length)
            {
                AnsiConsole.MarkupLine("[red]Usage:[/] query info -n <table>");
                return;
            }
            var tableName = args[nameIndex + 1];
            if (!tableFiles.Contains(tableName))
            {
                AnsiConsole.MarkupLine($"[red]Table '{tableName}' not found.[/]");
                return;
            }

            var tableInfo = await _db.GetTableMetadataAsync(tableName);

            var table = new Table()
                .Border(TableBorder.Rounded)
                .BorderColor(Color.Blue)
                .AddColumn("[bold]Property[/]")
                .AddColumn("[bold]Value[/]");

            table.AddRow("[cyan]Table Name[/]", $"[green]{tableInfo.TableName}[/]");
            table.AddRow("[cyan]Created At[/]", $"[green]{tableInfo.CreatedAt:yyyy-MM-dd HH:mm:ss}[/]");
            table.AddRow("[cyan]Last Updated[/]", $"[green]{tableInfo.LastUpdated:yyyy-MM-dd HH:mm:ss}[/]");
            table.AddRow("[cyan]Page Number[/]", $"[green]{tableInfo.PageNumber}[/]");
            table.AddRow("[cyan]Record Count[/]", $"[green]{tableInfo.RecordCount}[/]");
            table.AddRow("[cyan]Operation Count[/]", $"[green]{tableInfo.OperationCount}[/]");
            table.AddRow("[cyan]Last Operation[/]", $"[green]{tableInfo.LastOperation}[/]");
            table.AddRow("[cyan]Version[/]", $"[green]{tableInfo.Version}[/]");
            table.AddRow("[cyan]Serializer[/]", $"[green]{tableInfo.Serializer}[/]");

            AnsiConsole.Write(table);
            AnsiConsole.Write(new Rule().RuleStyle("grey"));
        }
        
        private async Task DropTable(string[] args)
        {
            var nameIndex = Array.IndexOf(args, "-n");

            if (nameIndex == -1  || nameIndex + 1 >= args.Length)
            {
                AnsiConsole.MarkupLine("[red]Usage:[/] query drop -n <table>");
                return;
            }

            var tableName = args[nameIndex + 1];
            var dropTable = _tableService.DropTable(tableName);
            if (!dropTable)
            {
                AnsiConsole.MarkupLine($"[red]Could not find table '{tableName}'. Use 'query tables' to list tables. [/]");
                return;
            }
            AnsiConsole.MarkupLine($"[green]Table '{tableName}' dropped.[/]");
        }

        private async Task DeleteRecord(string[] args)
        {
            var nameIndex = Array.IndexOf(args, "-n");
            var whereIndex = Array.IndexOf(args, "where");

            if (nameIndex == -1 || whereIndex == -1 ||
                nameIndex + 1 >= args.Length || whereIndex + 1 >= args.Length)
            {
                AnsiConsole.MarkupLine("[red]Usage:[/] query delete -n <table> where key==value");
                return;
            }

            var tableName = args[nameIndex + 1];
            var condition = args[whereIndex + 1];

            var parts = condition.Split("==", StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length != 2)
            {
                AnsiConsole.MarkupLine("[red]Invalid where clause. Use: where key==value[/]");
                return;
            }

            var key = parts[0].Trim();
            var value = parts[1].Trim();

            try
            {
                var resultSet = await _db.ReadAllRecordAsync(tableName);
                var match = resultSet.Records.FirstOrDefault(r => 
                    r.ContainsKey(key) && r[key]?.ToString() == value);

                if (match == null)
                {
                    AnsiConsole.MarkupLine($"[yellow]No record found where {key} == {value}[/]");
                    return;
                }

                await _db.DeleteRecordAsync(tableName, match.Key);
                AnsiConsole.MarkupLine($"[green]Record deleted from '{tableName}'.[/]");
            }
            catch (Exception ex)
            {
                AnsiConsole.MarkupLine($"[red]Failed to delete record: {ex.Message}[/]");
            }
        }
        
        private async Task DeleteRecordByKey(string[] args)
        {
            var nameIndex = Array.IndexOf(args, "-n");
            var keyIndex = Array.IndexOf(args, "-key");

            if (nameIndex == -1 || keyIndex == -1 ||
                nameIndex + 1 >= args.Length || keyIndex + 1 >= args.Length)
            {
                AnsiConsole.MarkupLine("[red]Usage:[/] query delete by key -n <table> -key <key_value>");
                return;
            }

            var tableName = args[nameIndex + 1];
            var keyValue = args[keyIndex + 1];
            if (!long.TryParse(keyValue, out var keyLong))
            {
                AnsiConsole.MarkupLine("[red]Invalid key. Use: long or int[/]");
                return;
            }

            try
            {
                await _db.DeleteRecordAsync(tableName, keyLong);
                AnsiConsole.MarkupLine($"[green]Record with key '{keyValue}' deleted from '{tableName}'.[/]");
            }
            catch (Exception ex)
            {
                AnsiConsole.MarkupLine($"[red]Failed to delete record: {ex.Message}[/]");
            }
        }

        private async Task DeleteRecordRange(string[] args)
        {
            var nameIndex = Array.IndexOf(args, "-n");
            var startIndex = Array.IndexOf(args, "-start");
            var endIndex = Array.IndexOf(args, "-end");

            if (nameIndex == -1 || startIndex == -1 || endIndex == -1 ||
                nameIndex + 1 >= args.Length || startIndex + 1 >= args.Length || endIndex + 1 >= args.Length)
            {
                AnsiConsole.MarkupLine("[red]Usage:[/] query delete range -n <table> -start <start_key> -end <end_key>");
                return;
            }

            var tableName = args[nameIndex + 1];
            var startKey = args[startIndex + 1];
            var endKey = args[endIndex + 1];

            if (!long.TryParse(startKey, out var startKeyLong) || !long.TryParse(endKey, out var endKeyLong))
            {
                AnsiConsole.MarkupLine("[red]Invalid key. Use: long or int[/]");
                return;
            }

            try
            {
                var storage = new PagedFileStorageUsingBPT(_root, tableName, _options);
                var keysToDelete = new List<long>();

                await foreach (var record in storage.FindRangeAsync(startKeyLong, endKeyLong))
                {
                    keysToDelete.Add(record.Key);
                }

                if (keysToDelete.Count == 0)
                {
                    AnsiConsole.MarkupLine($"[yellow]No records found between {startKey} and {endKey} in '{tableName}'.[/]");
                    return;
                }

                foreach (var key in keysToDelete)
                    await _db.DeleteRecordAsync(tableName, key);

                AnsiConsole.MarkupLine($"[green]{keysToDelete.Count} records deleted from '{tableName}' in range {startKey}–{endKey}.[/]");
            }
            catch (Exception ex)
            {
                AnsiConsole.MarkupLine($"[red]Failed to delete range: {ex.Message}[/]");
            }
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

            try
            {
                if (_tableService.TableExists(tableName))
                {
                    AnsiConsole.MarkupLine($"[yellow]Table '{tableName}' already exists.[/]");
                    return Task.CompletedTask;
                }

                _tableService.CreateTable(tableName);
                AnsiConsole.MarkupLine($"[green]Table '{tableName}' created successfully.[/]");
            }
            catch (Exception ex)
            {
                AnsiConsole.MarkupLine($"[red]Failed to create table: {ex.Message}[/]");
            }

            return Task.CompletedTask;
        }

        private async Task GetTable(string[] args)
        {
            var nameIndex = Array.IndexOf(args, "-n");
            if (nameIndex == -1 || nameIndex + 1 >= args.Length)
            {
                AnsiConsole.MarkupLine("[red]Usage:[/] query get -n <table>");
                return;
            }
            
            var tableName = args[nameIndex + 1];
            
            if (!_tableService.TableExists(tableName))
            {
                AnsiConsole.MarkupLine($"[red]Table '{tableName}' not found.[/]");
                return;
            }

            var showAll = args.Contains("-all", StringComparer.OrdinalIgnoreCase);
            
            try
            {
                /*
                 * handle by key: commandsTable.AddRow("query get by key", "Retrieve record by key", "query get by key -n <table_name> -key <key>", "query get by key -n users -key eb2232436666482cbdcb1b1fb5382217");
                 */
                if (args.Contains("-key", StringComparer.OrdinalIgnoreCase))
                {
                    var keyIndex = Array.IndexOf(args, "-key");
                    if (keyIndex + 1 >= args.Length)
                    {
                        AnsiConsole.MarkupLine("[red]Invalid key. Use: -key <key_value>[/]");
                        return;
                    }
                    var keyValue = args[keyIndex + 1];
                    if (!long.TryParse(keyValue, out var keyLong))
                    {
                        AnsiConsole.MarkupLine("[red]Invalid key. Use: long or int[/]");
                        return;
                    }
                    var record = await _db.GetRecordByKeyAsync(tableName, keyLong);
                    if (record == null)
                    {
                        AnsiConsole.MarkupLine($"[yellow]No record found with key '{keyValue}'[/]");
                    }
                    else
                    {
                        QueryRenderer.RenderRecords(tableName, [record]);
                    }
                    return;
                }
                
                var resultSet = await _db.ReadAllRecordAsync(tableName);
                var records = resultSet.Records
                    .Select(r => FilterSystemFields(r, showAll))
                    .ToList();

                if (args.Contains("where", StringComparer.OrdinalIgnoreCase))
                {
                    var whereIndex = Array.IndexOf(args, "where");
                    if (whereIndex + 1 >= args.Length)
                    {
                        AnsiConsole.MarkupLine("[red]Invalid where clause. Use: where key==value[/]");
                        return;
                    }

                    var condition = args[whereIndex + 1];
                    var parts = condition.Split("==", StringSplitOptions.TrimEntries);
                    if (parts.Length != 2)
                    {
                        AnsiConsole.MarkupLine("[red]Invalid where clause. Use: where key==value[/]");
                        return;
                    }

                    var key = parts[0].Trim();
                    var value = parts[1].Trim();
                    var matches = records.Where(r => 
                        r.ContainsKey(key) && r[key]?.ToString() == value).ToList();

                    if (matches.Count == 0)
                    {
                        AnsiConsole.MarkupLine($"[yellow]No records found where {key} == {value}[/]");
                        return;
                    }

                    QueryRenderer.RenderRecords(tableName, matches);
                    return;
                }
                
                QueryRenderer.RenderRecords(tableName, records);
            }
            catch (Exception ex)
            {
                AnsiConsole.MarkupLine($"[red]Failed to read table: {ex.Message}[/]");
            }
        }
        
        private Row FilterSystemFields(Row row, bool showAll)
        {
            if (showAll || row == null)
                return row;

            var filtered = new Row();
            foreach (var kv in row)
            {
                if (!kv.Key.StartsWith("naivedb_sys_", StringComparison.OrdinalIgnoreCase))
                    filtered[kv.Key] = kv.Value;
            }
            return filtered;
        }

        private async Task AddRecord(string[] args)
        {
            var nameIndex = Array.IndexOf(args, "-n");
            var dataIndex = Array.IndexOf(args, "-data");

            if (nameIndex == -1 || dataIndex == -1 ||
                nameIndex + 1 >= args.Length || dataIndex + 1 >= args.Length)
            {
                AnsiConsole.MarkupLine("[red]Usage:[/] query add -n <table> -data '{json}'");
                return;
            }

            var tableName = args[nameIndex + 1];
            var dataJson = string.Join(" ", args.Skip(dataIndex + 1));

            try
            {
                if (dataJson.StartsWith('"') && dataJson.EndsWith('"'))
                    dataJson = dataJson.Substring(1, dataJson.Length - 2);

                var recordToAdd = JsonSerializer.Deserialize<Row>(dataJson, JsonSerializerHelper.Options);
                if (recordToAdd == null)
                {
                    AnsiConsole.MarkupLine("[red]Invalid JSON data.[/]");
                    return;
                }

                var record = new Row();
                
                record["naivedb_sys_incremental_value"] = await GetNextIncrementalValue(tableName);
                record["naivedb_sys_unique_value"] = Guid.NewGuid().ToString();
                record["naivedb_sys_device_info"] = Environment.MachineName;
                record["naivedb_sys_timestamp_utc"] = DateTime.UtcNow.ToString("o");
                record["naivedb_sys_version"] = AppConstants.Version;
                
                foreach (var kv in recordToAdd)
                    record[kv.Key] = kv.Value;

                await _db.CreateRecordAsync(tableName, record);
                AnsiConsole.MarkupLine($"[green]Record added to '{tableName}'.[/]");
            }
            catch (JsonException ex)
            {
                AnsiConsole.MarkupLine($"[red]Invalid JSON format: {ex.Message}[/]");
            }
            catch (Exception ex)
            {
                AnsiConsole.MarkupLine($"[red]Failed to add record: {ex.Message}[/]");
            }
        }

        private async Task<int> GetNextIncrementalValue(string tableName)
        {
            try
            {
                var resultSet = await _db.ReadAllRecordAsync(tableName);
                var maxId = resultSet.Records
                    .Where(r => r.ContainsKey("naivedb_sys_incremental_value"))
                    .Select(r => r["naivedb_sys_incremental_value"])
                    .Where(v => v is int)
                    .Cast<int>()
                    .DefaultIfEmpty(0)
                    .Max();
                return maxId + 1;
            }
            catch
            {
                return 1;
            }
        }

        private async Task UpdateRecord(string[] args)
        {
            var nameIndex = Array.IndexOf(args, "-n");
            var dataIndex = Array.IndexOf(args, "-data");
            var whereIndex = Array.IndexOf(args, "where");

            if (nameIndex == -1 || dataIndex == -1 || whereIndex == -1 ||
                nameIndex + 1 >= args.Length || dataIndex + 1 >= args.Length || whereIndex + 1 >= args.Length)
            {
                AnsiConsole.MarkupLine("[red]Usage:[/] query update -n <table> -data '{json}' where key==value");
                return;
            }

            var tableName = args[nameIndex + 1];
            var dataJson = string.Join(" ", args.Skip(dataIndex + 1)); // Handle JSON with spaces
            var condition = args[whereIndex + 1];

            var parts = condition.Split("==", StringSplitOptions.TrimEntries);
            if (parts.Length != 2)
            {
                AnsiConsole.MarkupLine("[red]Invalid where clause. Use: where key==value[/]");
                return;
            }

            var key = parts[0].Trim();
            var value = parts[1].Trim();

            try
            {
                if (dataJson.StartsWith('"') && dataJson.EndsWith('"'))
                    dataJson = dataJson.Substring(1, dataJson.Length - 2);

                var recordUpdate = JsonSerializer.Deserialize<Row>(dataJson, JsonSerializerHelper.Options);
                if (recordUpdate == null)
                {
                    AnsiConsole.MarkupLine("[red]Invalid JSON data.[/]");
                    return;
                }

                var resultSet = await _db.ReadAllRecordAsync(tableName);
                var match = resultSet.Records.FirstOrDefault(r => 
                    r.ContainsKey(key) && r[key]?.ToString() == value);

                if (match == null)
                {
                    AnsiConsole.MarkupLine($"[yellow]No record found where {key} == {value}[/]");
                    return;
                }

                foreach (var kvp in recordUpdate)
                    match[kvp.Key] = kvp.Value;

                await _db.UpdateRecordAsync(tableName, match);
                AnsiConsole.MarkupLine($"[green]Record updated in '{tableName}'.[/]");
            }
            catch (JsonException ex)
            {
                AnsiConsole.MarkupLine($"[red]Invalid JSON format: {ex.Message}[/]");
            }
            catch (Exception ex)
            {
                AnsiConsole.MarkupLine($"[red]Failed to update record: {ex.Message}[/]");
            }
        }
    }
}