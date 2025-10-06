using naivedb.core.storage;
using Spectre.Console;

namespace naivedb.cli.presentation.renderers
{
    public static class QueryRenderer
    {
        public static void RenderHelp()
        {
            var table = new Table().Border(TableBorder.Rounded)
                .AddColumn("[bold]Command[/]")
                .AddColumn("[bold]Description[/]")
                .AddColumn("[bold]Usage[/]");

            table.AddRow("create", "Create a table", "query create -n <table>");
            table.AddRow("get", "List all rows from table", "query get -n <table>");
            table.AddRow("add", "Add record to table", "query add -n <table> -data '{json}'");
            table.AddRow("update", "Update record", "query update -n <table> -data '{json}' where key==value");

            AnsiConsole.Write(table);
        }

        public static void RenderRecords(string tableName, IEnumerable<Record> records)
        {
            var recList = records.ToList();
            if (recList.Count == 0)
            {
                AnsiConsole.MarkupLine($"[yellow]No records found in table '{tableName}'.[/]");
                return;
            }

            var first = recList.First();
            var table = new Table().Border(TableBorder.Rounded);
            foreach (var key in first.Keys)
                table.AddColumn($"[bold cyan]{key}[/]");

            foreach (var r in recList)
                table.AddRow(r.Values.Select(v => v?.ToString() ?? "").ToArray());

            AnsiConsole.Write(table);
        }
    }
}