using Spectre.Console;

namespace naivedb.cli.presentation.renderers
{
    public class DatabaseRenderer
    {
        public static void RenderList(List<string> dbs)
        {
            if (dbs.Count == 0)
            {
                AnsiConsole.MarkupLine("[yellow]No databases found.[/]");
                return;
            }

            var table = new Table().Border(TableBorder.Rounded)
                .AddColumn("[bold cyan]Database Name[/]")
                .AddColumn("[bold green]Created at[/]");

            foreach (var db in dbs)
            {
                table.AddRow(db, Directory.GetCreationTime(db).ToString("g"));
            }

            AnsiConsole.Write(table);
        }

        public static void RenderHelp()
        {
            var table = new Table().Border(TableBorder.Rounded)
                .AddColumn("[bold]Command[/]")
                .AddColumn("[bold]Description[/]")
                .AddColumn("[bold]Usage[/]");

            table.AddRow("create", "Create a new database", "create <dbname>");
            table.AddRow("connect", "Connect to a database", "connect <dbname>");
            table.AddRow("drop", "Drop a database", "drop <dbname>");
            table.AddRow("list", "List all databases", "list");
            table.AddRow("query", "Execute a query", "query \"SELECT * FROM table\"");
            table.AddRow("import", "Import data from file", "import <file>");
            table.AddRow("export", "Export data to file", "export <dbname>");

            AnsiConsole.Write(table);
        }
    }
}