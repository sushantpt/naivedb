using naivedb.cli.presentation.constants;
using Spectre.Console;

namespace naivedb.cli.presentation.commands
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