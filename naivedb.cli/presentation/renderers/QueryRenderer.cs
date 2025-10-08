using naivedb.core.storage.pages;
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

        public static void RenderRecords(string tableName, IEnumerable<Row> records)
        {
            var recList = records.Where(x => x != null).ToList();
            if (recList.Count == 0 || recList.All(r => r.Values.Count == 0))
            {
                AnsiConsole.MarkupLine($"[yellow]No records found in table '{tableName}'.[/]");
                return;
            }
            
            var allKeys = recList
                .SelectMany(r => r.Keys)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();
            var table = new Table().Border(TableBorder.Rounded);
            foreach (var key in allKeys)
                table.AddColumn($"[bold cyan]{key}[/]");
            foreach (var r in recList)
            {
                var rowValues = allKeys.Select(k =>
                    r.TryGetValue(k, out var val) ? val?.ToString() ?? "" : ""
                ).ToArray();

                table.AddRow(rowValues);
            }

            AnsiConsole.Write(table);
        }
    }
}