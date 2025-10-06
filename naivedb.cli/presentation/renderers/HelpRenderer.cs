using naivedb.cli.presentation.constants;
using Spectre.Console;

namespace naivedb.cli.presentation.renderers
{
    public class HelpRenderer : IOutputRenderer
    {
        public void Render()
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
            optionsTable.AddRow("--verbose", "Enable verbose output", "--verbose");

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
            commandsTable.AddRow("query create", "Create a new table", "query create table -n <table_name>", "query create table -n users");
            commandsTable.AddRow("query get", "Retrieve records", "query get -n <table_name>", "query get -n users");
            commandsTable.AddRow("query add", "Insert new record", "query add -n <table_name> -data '{json}'", "query add -n users -data '{\"id\": 1, \"name\": \"John\"}'");
            commandsTable.AddRow("query delete", "Delete record(s)", "query delete -n <table_name> where key==value", "query delete -n users where name==john");
            commandsTable.AddRow("export", "Export database to JSON", "export <database_name>", "export mydatabase");
            commandsTable.AddRow("import", "Import database from JSON", "import <path_to_json>", "import ./backup.json");
            commandsTable.AddRow("drop", "Delete database", "drop <database_name>", "drop mydatabase");
            commandsTable.AddRow("list", "List all databases", "list", "list");

            AnsiConsole.Write(commandsTable);
            AnsiConsole.WriteLine();
        }
    }
}