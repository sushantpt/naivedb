using naivedb.core.configs;
using naivedb.core.constants;
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
        public async Task RenderAsync()
        {
            AnsiConsole.Write(new FigletText(AppConstants.AppName)
                .LeftJustified()
                .Color(Color.Green));
            
            var dbPath = Path.Combine(Directory.GetCurrentDirectory(), _options.DataPath);
            var currentDbFile = Path.Combine(dbPath, _options.DbInfoFile);
            Directory.CreateDirectory(dbPath);

            var dbInfo = new DbInfo();
            string? currentlyConnected = string.Empty;

            if (File.Exists(currentDbFile))
            {
                dbInfo = await dbInfo.LoadAsync(currentDbFile);
                currentlyConnected = dbInfo?.CurrentDatabase;
            }
            
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
                .AddRow("------------------")
                .AddRow(usageTable)
                .AddRow(string.IsNullOrWhiteSpace(currentlyConnected) || string.IsNullOrEmpty(currentlyConnected)
                    ? $"[red]Not connected to any database.[/]" 
                    : $"[green]Currently connected to:[/] {currentlyConnected}");
            
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