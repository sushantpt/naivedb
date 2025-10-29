using Spectre.Console;

namespace naivedb.cli.presentation.renderers
{
    public class TableRenderer
    {
        public static async Task RenderTableListAsync(List<(string TableName, int RecordCount, long SizeKB, DateTime LastModified)> tables)
        {
            if (tables.Count == 0)
            {
                AnsiConsole.MarkupLine("[yellow]No tables found.[/]");
                return;
            }

            var table = new Table().Border(TableBorder.Rounded)
                .AddColumn("[bold cyan]Table Name[/]")
                .AddColumn("[bold green]Records[/]")
                .AddColumn("[bold blue]Size (KB)[/]")
                .AddColumn("[bold magenta]Last Modified[/]");

            foreach (var (tableName, recordCount, sizeKb, lastModified) in tables)
            {
                table.AddRow(
                    tableName,
                    recordCount.ToString(),
                    sizeKb.ToString(),
                    lastModified.ToString("g")
                );
            }

            AnsiConsole.Write(table);
        }
    }
}