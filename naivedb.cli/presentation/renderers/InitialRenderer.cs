using naivedb.cli.presentation.constants;
using naivedb.core.configs;
using Spectre.Console;

namespace naivedb.cli.presentation.renderers
{
    public class InitialRenderer : IOutputRenderer
    {
        private readonly DbOptions _options;
        public InitialRenderer(DbOptions options)
        {
            _options = options;
        }
        public void Render()
        {
            AnsiConsole.Write(new FigletText(AppConstants.AppName)
                .LeftJustified()
                .Color(Color.Green));
            
            var dbPath = Path.Combine(Directory.GetCurrentDirectory(), _options.DataPath);
            var currentDbFile = Path.Combine(dbPath, "current_db.txt");
            if (!Directory.Exists(dbPath))
                Directory.CreateDirectory(dbPath);
            
            string? currentlyConnected = File.Exists(currentDbFile) ? File.ReadAllText(currentDbFile).Trim() : null;
            currentlyConnected = string.IsNullOrWhiteSpace(currentlyConnected) ? null : currentlyConnected; // check whitespace
            
            var usageTable = new Table()
                .Border(TableBorder.Rounded)
                .BorderColor(Color.Yellow)
                .AddColumn(new TableColumn("[bold]Command[/]").Centered())
                .AddColumn(new TableColumn("[bold]Description[/]").Centered())
                .AddRow("[blue]--help[/]", "Show detailed help")
                .AddRow("[blue]--info[/]", "Show system information")
                .AddRow("[blue]--version[/]", "Show version information");
            
            var contentGrid = new Grid()
                .AddColumn()
                .AddRow($"[bold green]{AppConstants.Description}[/]")
                .AddRow("------------------")
                .AddRow($"[yellow]Version:[/] {AppConstants.Version}")
                .AddRow($"[yellow]Author:[/] {AppConstants.Author}")
                .AddRow("------------------")
                .AddRow(usageTable)
                .AddRow(currentlyConnected is not null ? $"[green]Currently connected to:[/] {currentlyConnected}" : $"[red]Not connected to any database.[/]");
            
            var mainPanel = new Panel(contentGrid)
            {
                Header = new PanelHeader("Welcome to naiveDB!", Justify.Center),
                Border = BoxBorder.Rounded,
                BorderStyle = new Style(Color.Blue),
                Padding = new Padding(3, 1, 3, 1)
            };

            AnsiConsole.Write(mainPanel);
            AnsiConsole.WriteLine();
        }
    }
}