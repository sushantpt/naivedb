using naivedb.core.constants;
using Spectre.Console;

namespace naivedb.cli.presentation.renderers
{
    public class HelpRenderer : IOutputRenderer
    {
        public Task RenderAsync()
        {
            AnsiConsole.Write(new FigletText($"{AppConstants.AppName}")
                .LeftJustified()
                .Color(Color.Green));
            
            var optionsTable = new Table()
                .Border(TableBorder.Rounded)
                .Title("Global Options")
                .AddColumn("Option")
                .AddColumn("Description")
                .AddColumn("Example");

            optionsTable.AddRow("--help, -h", "Show help information", "--help");
            optionsTable.AddRow("--info", "Show system information", "--info");
            optionsTable.AddRow("--version, -v", "Show version", "--version");

            AnsiConsole.Write(optionsTable);
            AnsiConsole.WriteLine();
            
            var commandsTable = new Table()
                .Border(TableBorder.Rounded)
                .Title("Commands")
                .AddColumn("Command initials")
                .AddColumn("Description")
                .AddColumn("Usage")
                .AddColumn("Example");
            
            commandsTable.AddRow("create", "Create a new database", "create <database_name>", "create mydatabase");
            commandsTable.AddRow("list", "List all databases", "list", "list");
            commandsTable.AddRow("connect database", "Connect to a database", "connect <database_name>", "connect mydatabase");
            commandsTable.AddRow("disconnect database", "Disconnect to a database", "disconnect <database_name>", "disconnect mydatabase");
            commandsTable.AddRow("export", "Export database to JSON", "export <database_name>", "export mydatabase");
            commandsTable.AddRow("import", "Import database from JSON", "import <path_to_json>", "import ./backup.json");
            commandsTable.AddRow("drop", "Delete database", "drop <database_name>", "drop mydatabase");
            commandsTable.AddEmptyRow();
            
            commandsTable.AddRow("query create", "Create a new table", "query create table -n <table_name>", "query create table -n users");
            commandsTable.AddRow("query info", "View table metadata", "query info -n <table_name>", "query info -n users");
            commandsTable.AddRow("query add", "Insert new record", "query add -n <table_name> -data '{json}'", "query add -n users -data '{\"id\": 1, \"name\": \"John\"}'");
            commandsTable.AddRow("query get", "Retrieve records", "query get -n <table_name>", "query get -n users");
            commandsTable.AddRow("query get by key", "Retrieve record by key", "query get by key -n <table_name> -key <key>", "query get by key -n users -key 123");
            commandsTable.AddRow("query get range", "Retrieve records in a key range", "query get range -n <table_name> -start <start_key> -end <end_key>", "query get range -n users -start 1 -end 10");
            commandsTable.AddRow("query delete any predicate", "Delete record(s)", "query delete -n <table_name> where key==value", "query delete -n users where name==john");
            commandsTable.AddRow("query delete by key", "Delete record by key", "query delete by key -n <table_name> -key <key_value>", "query delete by key -n users -key 123 ");
            commandsTable.AddRow("query delete range", "Delete records in a key range", "query delete range -n <table_name> -start <start_key> -end <end_key>", "query delete range -n users -start 1 -end 10");

            commandsTable.AddRow("query update", "Update record(s) with predicate", "query update -n <table_name> -data '{json}' where key==value", "query update -n users -data '{\"name\": \"John\"}' where id==1");
            commandsTable.AddRow("query tables", "Get list of tables", "query tables", "query tables");
            commandsTable.AddRow("query drop", "Drop table", "query drop -n <table_name>", "query drop -n users");

            AnsiConsole.Write(commandsTable);
            AnsiConsole.WriteLine();
            return Task.CompletedTask;
        }
    }
}