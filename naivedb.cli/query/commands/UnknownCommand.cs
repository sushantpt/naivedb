using Spectre.Console;

namespace naivedb.cli.query.commands
{
    public class UnknownCommand(string[] args) : ICommand
    {
        public Task ExecuteAsync(string[] args1)
        {
            AnsiConsole.MarkupLine($"[red]Error:[/] Unknown command '[yellow]{string.Join(" ", args)}[/]'");
            AnsiConsole.MarkupLine("Type '[blue]help[/]' to see available commands.");
            return Task.CompletedTask;
        }
    }
}