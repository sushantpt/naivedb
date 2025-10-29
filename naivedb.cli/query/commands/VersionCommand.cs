using naivedb.core.constants;
using Spectre.Console;

namespace naivedb.cli.query.commands
{
    public class VersionCommand : ICommand
    {
        public Task ExecuteAsync(string[] args)
        {
            AnsiConsole.MarkupLine($"[bold blue]{AppConstants.AppName}[/] version [green]{AppConstants.Version}[/]");
            return Task.CompletedTask;
        }
    }
}