using System.Text.Json;
using naivedb.cli.presentation.renderers;
using naivedb.core.configs;
using naivedb.core.storage;
using Spectre.Console;

namespace naivedb.cli.presentation.commands
{
    public class DatabaseCommand : ICommand
    {
        private readonly DbOptions _options;
        public DatabaseCommand(DbOptions options)
        {
            _options = options;
        }
        public async Task ExecuteAsync(string[] args)
        {
            var action = args[0].ToLowerInvariant();
            var dbRoot = Path.Combine(Directory.GetCurrentDirectory(), _options.DataPath);

            Directory.CreateDirectory(dbRoot);

            switch (action)
            {
                case "create":
                    await CreateDatabase(dbRoot, args);
                    break;
                case "connect":
                    await ConnectDatabase(dbRoot, args);
                    break;
                case "drop":
                    await DropDatabase(dbRoot, args);
                    break;
                case "list":
                    await ListDatabases(dbRoot);
                    break;
                case "query":
                    await RunQuery(dbRoot, args);
                    break;
                case "import":
                    await ImportData(dbRoot, args);
                    break;
                case "export":
                    await ExportData(dbRoot, args);
                    break;
                default:
                    DatabaseRenderer.RenderHelp();
                    break;
            }
        }
        
        private Task CreateDatabase(string root, string[] args)
        {
            if (args.Length < 2)
            {
                AnsiConsole.MarkupLine("[red]Usage:[/] create <dbname>");
                return Task.CompletedTask;
            }

            var dbName = args[1];
            var path = Path.Combine(root, dbName);

            if (Directory.Exists(path))
            {
                AnsiConsole.MarkupLine($"[yellow]Database '{dbName}' already exists.[/]");
                return Task.CompletedTask;
            }

            Directory.CreateDirectory(path);
            AnsiConsole.MarkupLine($"[green]Database '{dbName}' created successfully.[/]");
            return Task.CompletedTask;
        }

        private Task ConnectDatabase(string root, string[] args)
        {
            if (args.Length < 2)
            {
                AnsiConsole.MarkupLine("[red]Usage:[/] connect <dbname>");
                return Task.CompletedTask;
            }

            var dbName = args[1];
            var path = Path.Combine(root, dbName);

            if (!Directory.Exists(path))
            {
                AnsiConsole.MarkupLine($"[red]Database '{dbName}' not found.[/]");
                return Task.CompletedTask;
            }

            File.WriteAllText(Path.Combine(root, "current_db.txt"), dbName);
            AnsiConsole.MarkupLine($"[green]Connected to database:[/] {dbName}");
            return Task.CompletedTask;
        }

        private Task DropDatabase(string root, string[] args)
        {
            var dbName = args[1];
            var path = Path.Combine(root, dbName);

            if (!Directory.Exists(path))
            {
                AnsiConsole.MarkupLine($"[red]Database '{dbName}' not found.[/]");
                return Task.CompletedTask;
            }
            
            File.Delete(Path.Combine(root, "current_db.txt"));

            Directory.Delete(path, true);
            AnsiConsole.MarkupLine($"[red]Database '{dbName}' dropped.[/]");
            return Task.CompletedTask;
        }

        private Task ListDatabases(string root)
        {
            var dbs = Directory.GetDirectories(root)
                .Select(Path.GetFileName)
                .ToList();

            DatabaseRenderer.RenderList(dbs);
            return Task.CompletedTask;
        }

        private async Task RunQuery(string root, string[] args)
        {
            var dbName = GetCurrentDb(root);
            if (dbName == null)
            {
                AnsiConsole.MarkupLine("[red]No database connected. Use 'connect <dbname>'.[/]");
                return;
            }

            var queryArgs = args.Skip(1).ToArray();
            var queryCommand = new QueryCommand(root, dbName, _options);
            await queryCommand.ExecuteAsync(queryArgs);
        }

        private async Task ImportData(string root, string[] args)
        {
            if (args.Length < 2)
            {
                AnsiConsole.MarkupLine("[red]Usage:[/] import <export_file.json> [-n <new_db_name>]");
                return;
            }

            var exportFile = args[1];
            if (!File.Exists(exportFile))
            {
                AnsiConsole.MarkupLine($"[red]Export file '{exportFile}' not found.[/]");
                return;
            }
            
            string? dbName = null;
            var nameIndex = Array.IndexOf(args, "-n");
            if (nameIndex != -1 && nameIndex + 1 < args.Length)
                dbName = args[nameIndex + 1];
            else
                dbName = Path.GetFileNameWithoutExtension(exportFile).Replace("_export", "");

            var dbPath = Path.Combine(root, dbName);
            Directory.CreateDirectory(dbPath);

            try
            {
                var json = await File.ReadAllTextAsync(exportFile);
                var tables = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(json);

                if (tables == null || tables.Count == 0)
                {
                    AnsiConsole.MarkupLine($"[red]No valid table data found in '{exportFile}'.[/]");
                    return;
                }

                var options = new JsonSerializerOptions { WriteIndented = true };

                foreach (var kvp in tables)
                {
                    var tableName = kvp.Key;
                    var tableJson = JsonSerializer.Serialize(kvp.Value, options);

                    var tablePath = Path.Combine(dbPath, $"{tableName}.json");
                    await File.WriteAllTextAsync(tablePath, tableJson);

                    AnsiConsole.MarkupLine($"[green]Imported table:[/] {tableName}");
                }

                var absPath = Path.GetFullPath(dbPath);
                AnsiConsole.MarkupLine($"\n[bold green]Database imported successfully to:[/] [yellow]{absPath}[/]");
            }
            catch (Exception ex)
            {
                AnsiConsole.MarkupLine($"[red]Import failed:[/] {ex.Message}");
            }
        }

        private Task ExportData(string root, string[] args)
        {
            if (args.Length < 2)
            {
                AnsiConsole.MarkupLine("[red]Usage:[/] export <dbname>");
                return Task.CompletedTask;
            }

            var dbName = args[1];
            var path = Path.Combine(root, dbName);
            if (!Directory.Exists(path))
            {
                AnsiConsole.MarkupLine($"[red]Database '{dbName}' not found.[/]");
                return Task.CompletedTask;
            }

            var exportFileName = $"{dbName}_export.json";
            var exportFilePath = Path.GetFullPath(exportFileName);
            
            var exportData = new Dictionary<string, object>();

            foreach (var file in Directory.GetFiles(path, "*.json"))
            {
                var tableName = Path.GetFileNameWithoutExtension(file);
                try
                {
                    var json = File.ReadAllText(file);
                    var tableData = JsonSerializer.Deserialize<JsonElement>(json);
                    exportData[tableName] = tableData;
                }
                catch
                {
                    exportData[tableName] = $"Error reading {file}";
                }
            }
            var finalJson = JsonSerializer.Serialize(exportData, JsonSerializerHelper.Options);
            File.WriteAllText(exportFilePath, finalJson);

            AnsiConsole.MarkupLine($"[green]Exported database '{dbName}' to:[/] [yellow]{exportFilePath}[/]");
            return Task.CompletedTask;
        }

        private string? GetCurrentDb(string root)
        {
            var file = Path.Combine(root, "current_db.txt");
            return File.Exists(file) ? File.ReadAllText(file) : null;
        }
    }
}