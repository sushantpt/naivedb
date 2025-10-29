using System.Diagnostics;
using System.Text.Json;
using naivedb.cli.presentation.renderers;
using naivedb.core.configs;
using naivedb.core.logger;
using naivedb.core.metadata;
using naivedb.facade.services;
using Spectre.Console;

namespace naivedb.cli.query.commands
{
    public class DatabaseCommand : ICommand
    {
        private readonly DbOptions _options;
        private readonly DatabaseService _dbService;
        private readonly QueryService _queryService;
        
        public DatabaseCommand(DbOptions options)
        {
            _options = options;
            var dbRoot = Path.Combine(Directory.GetCurrentDirectory(), _options.DataPath);
            Directory.CreateDirectory(dbRoot);
            _dbService = DatabaseService.Init(_options, dbRoot);
            _queryService = QueryService.Init(_options, dbRoot);
        }

        public async Task ExecuteAsync(string[] args)
        {
            var action = args[0].ToLowerInvariant();

            switch (action)
            {
                case "create":
                    await CreateDatabaseAsync(args);
                    break;
                case "connect":
                    await ConnectDatabaseAsync(args);
                    break;
                case "disconnect":
                    await DisconnectDatabaseAsync(args);
                    break;
                case "drop":
                    await DropDatabase(args);
                    break;
                case "list":
                    await ListDatabases();
                    break;
                case "query":
                    await RunQuery(args);
                    break;
                case "import":
                    await ImportDataAsync(args);
                    break;
                case "export":
                    await ExportDataAsync(args);
                    break;
                default:
                    DatabaseRenderer.RenderHelp();
                    break;
            }
        }

        private async Task DisconnectDatabaseAsync(string[] args)
        {
            if (args.Length < 2)
            {
                AnsiConsole.MarkupLine("[red]Usage:[/] disconnect <dbname>");
                return;
            }
            var dbName = args[1];
            var currentDbName = await _dbService.GetCurrentDatabase();
            if (string.IsNullOrWhiteSpace(currentDbName) || currentDbName != dbName)
            {
                AnsiConsole.MarkupLine($"[red]Database '{dbName}' not connected. Use 'connect <dbname>' to connect to a database.[/]");
                await Task.CompletedTask;
                return;
            }
            var dbInfo = new DbInfo
            {
                CurrentDatabase = dbName,
                LastConnectedUtc = DateTime.UtcNow
            };
            var disconnect = await _dbService.DisconnectDatabase(dbInfo);
            AnsiConsole.MarkupLine(disconnect
                ? $"[red]Disconnected from database:[/] {dbName}"
                : $"[red]Failed to disconnect from database '{dbName}'.[/]");
            await Task.CompletedTask;
        }

        private async Task CreateDatabaseAsync(string[] args)
        {
            if (args.Length < 2)
            {
                AnsiConsole.MarkupLine("[red]Usage:[/] create <dbname>");
                return;
            }

            var dbName = args[1];
            
            if (_dbService.DatabaseExists(dbName))
            {
                AnsiConsole.MarkupLine($"[yellow]Database '{dbName}' already exists.[/]");
                return;
            }

            if (_dbService.CreateDatabase(dbName))
            {
                AnsiConsole.MarkupLine($"[green]Database '{dbName}' created successfully.[/]");
            }
            else
            {
                AnsiConsole.MarkupLine($"[red]Failed to create database '{dbName}'.[/]");
            }
            
            await Task.CompletedTask;
        }

        private async Task ConnectDatabaseAsync(string[] args)
        {
            try
            {
                if (args.Length < 2)
                {
                    AnsiConsole.MarkupLine("[red]Usage:[/] connect <dbname>");
                    return;
                }

                var dbName = args[1];

                if (!_dbService.DatabaseExists(dbName))
                {
                    AnsiConsole.MarkupLine($"[red]Database '{dbName}' not found.[/]");
                    return;
                }

                var dbInfo = new DbInfo
                {
                    CurrentDatabase = dbName,
                    LastConnectedUtc = DateTime.UtcNow
                };

                if (await _dbService.ConnectDatabase(dbInfo))
                {
                    AnsiConsole.MarkupLine($"[green]Connected to database:[/] {dbName}");
                }
                else
                {
                    AnsiConsole.MarkupLine($"[red]Failed to connect to database '{dbName}'.[/]");
                }
            }
            catch (Exception e)
            {
                AnsiConsole.MarkupLine($"[red]Failed to connect to '{args[1]}'. {e.Message}[/]");
            }
        }

        private async Task DropDatabase(string[] args)
        {
            if (args.Length < 2)
            {
                AnsiConsole.MarkupLine("[red]Usage:[/] drop <dbname>");
                return;
            }

            var dbName = args[1];

            if (!_dbService.DatabaseExists(dbName))
            {
                AnsiConsole.MarkupLine($"[red]Database '{dbName}' not found.[/]");
                return;
            }

            if (_dbService.DropDatabase(dbName))
            {
                AnsiConsole.MarkupLine($"[red]Database '{dbName}' dropped.[/]");
            }
            else
            {
                AnsiConsole.MarkupLine($"[red]Failed to drop database '{dbName}'.[/]");
            }

            await Task.CompletedTask;
        }

        private async Task ListDatabases()
        {
            var dbs = _dbService.ListDatabasesAsync();
            DatabaseRenderer.RenderList(dbs);
            await Task.CompletedTask;
        }

        private async Task RunQuery(string[] args)
        {
            var sw = Stopwatch.StartNew();
            var query = string.Join(' ', args.Skip(1));
            var context = new QueryContext(query);
            
            var dbName = await _dbService.GetCurrentDatabase();
            if (string.IsNullOrEmpty(dbName))
            {
                AnsiConsole.MarkupLine("[red]No database connected. Use 'connect <dbname>'.[/]");
                sw.Stop();
                await QueryLogger.LogAsync(context, dbName, "query", sw, "Failed", "No database connected. Use 'connect <dbname>'.");
                return;
            }

            var queryArgs = args.Skip(1).ToArray();
            var queryCommand = new QueryCommand(_dbService.GetDatabasePath(dbName), dbName, _options);
            await queryCommand.ExecuteAsync(queryArgs);
            sw.Stop();
            await QueryLogger.LogAsync(context, dbName, "query", sw, "Success");
        }

        private async Task ImportDataAsync(string[] args)
        {
            var sw = Stopwatch.StartNew();
            var query = string.Join(' ', args.Skip(1));
            var context = new QueryContext(query);
            
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
            
            string dbName;
            var nameIndex = Array.IndexOf(args, "-n");
            if (nameIndex != -1 && nameIndex + 1 < args.Length)
            {
                dbName = args[nameIndex + 1];
            }
            else
            {
                dbName = Path.GetFileNameWithoutExtension(exportFile)
                    .Replace("_export", "")
                    .Replace("export_", "");
                
                if (string.IsNullOrEmpty(dbName))
                {
                    dbName = $"imported_db_{DateTime.Now:yyyyMMdd_HHmmss}";
                }
            }

            if (_dbService.DatabaseExists(dbName))
            {
                AnsiConsole.MarkupLine($"[yellow]Database '{dbName}' already exists. Use -n to specify a different name.[/]");
                return;
            }

            try
            {
                if (!_dbService.CreateDatabase(dbName))
                {
                    AnsiConsole.MarkupLine($"[red]Failed to create database '{dbName}'.[/]");
                    return;
                }

                var json = await File.ReadAllTextAsync(exportFile);
                var exportData = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(json);

                if (exportData == null || exportData.Count == 0)
                {
                    AnsiConsole.MarkupLine($"[red]No valid table data found in '{exportFile}'.[/]");
                    _dbService.DropDatabase(dbName);
                    return;
                }

                var importedCount = 0;
                foreach (var (tableName, tableData) in exportData)
                {
                    try
                    {
                        await _queryService.ImportTableDataAsync(dbName, tableName, tableData);
                        importedCount++;
                        AnsiConsole.MarkupLine($"[green]Imported table:[/] {tableName}");
                    }
                    catch (Exception ex)
                    {
                        AnsiConsole.MarkupLine($"[yellow]Failed to import table '{tableName}': {ex.Message}[/]");
                    }
                }

                var dbInfo = new DbInfo
                {
                    CurrentDatabase = dbName,
                    LastConnectedUtc = DateTime.UtcNow
                };
                await _dbService.ConnectDatabase(dbInfo);

                sw.Stop();
                var absPath = Path.GetFullPath(_dbService.GetDatabasePath(dbName));
                
                AnsiConsole.MarkupLine($"\n[bold green]Import completed successfully![/]");
                AnsiConsole.MarkupLine($"[green]Database:[/] {dbName}");
                AnsiConsole.MarkupLine($"[green]Location:[/] {absPath}");
                AnsiConsole.MarkupLine($"[green]Tables imported:[/] {importedCount}/{exportData.Count}");

                await QueryLogger.LogAsync(context, dbName, "import", sw, "Success");
            }
            catch (Exception ex)
            {
                string detailedExceptionMessage = ex.ToString();
                AnsiConsole.MarkupLine($"[red]Import failed:[/] {ex.Message}");
                await QueryLogger.LogAsync(context, dbName ?? "unknown", "import", sw, "Failed", detailedExceptionMessage);
            }
        }

        private async Task ExportDataAsync(string[] args)
        {
            var sw = Stopwatch.StartNew();
            var query = string.Join(' ', args.Skip(1));
            var context = new QueryContext(query);
            
            if (args.Length < 2)
            {
                AnsiConsole.MarkupLine("[red]Usage:[/] export <dbname>");
                return;
            }

            var dbName = args[1];
            
            if (!_dbService.DatabaseExists(dbName))
            {
                AnsiConsole.MarkupLine($"[red]Database '{dbName}' not found.[/]");
                return;
            }

            try
            {
                var exportFileName = $"{dbName}_export_{DateTime.Now:yyyyMMdd_HHmmss}.json";
                var exportFilePath = Path.GetFullPath(exportFileName);
                
                var exportData = new Dictionary<string, object>();
                var tableNames = _queryService.ListTables(dbName);
                var exportedCount = 0;

                foreach (var tableName in tableNames)
                {
                    try
                    {
                        var tableData = await _queryService.GetTableDataAsync(dbName, tableName);
                        exportData[tableName] = tableData;
                        exportedCount++;
                    }
                    catch (Exception ex)
                    {
                        AnsiConsole.MarkupLine($"[yellow]Failed to read table '{tableName}': {ex.Message}[/]");
                        exportData[tableName] = $"Error reading table: {ex.Message}";
                    }
                }

                if (exportedCount == 0)
                {
                    AnsiConsole.MarkupLine($"[yellow]No tables found to export from database '{dbName}'.[/]");
                    return;
                }

                var finalJson = JsonSerializer.Serialize(exportData, new JsonSerializerOptions 
                { 
                    WriteIndented = true,
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                });
                
                await File.WriteAllTextAsync(exportFilePath, finalJson);
                sw.Stop();

                AnsiConsole.MarkupLine($"[green]Exported database '{dbName}' to:[/] [yellow]{exportFilePath}[/]");
                AnsiConsole.MarkupLine($"[green]Tables exported:[/] {exportedCount}");
                AnsiConsole.MarkupLine($"[green]File size:[/] {new FileInfo(exportFilePath).Length / 1024} KB");

                await QueryLogger.LogAsync(context, dbName, "export", sw, "Success");
            }
            catch (Exception ex)
            {
                string detailedExceptionMessage = ex.ToString();
                AnsiConsole.MarkupLine($"[red]Export failed:[/] {ex.Message}");
                await QueryLogger.LogAsync(context, dbName, "export", sw, "Failed", detailedExceptionMessage);
            }
        }
    }
}