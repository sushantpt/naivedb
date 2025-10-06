using System.Text;
using naivedb.cli.presentation.commands;
using naivedb.core.configs;
using Spectre.Console;

namespace naivedb.cli
{
    abstract class Program
    {
        private static bool _isRunning = true;
        private static DbOptions _dbOption = new()
        {
            DataPath = "data"
        };
        static async Task Main(string[] args)
        {
            try
            {
                if (args.Length > 0)
                {
                    await ProcessCommand(args, _dbOption);
                    return;
                }
                await RunRepl();
            }
            catch (Exception ex)
            {
                AnsiConsole.WriteException(ex, ExceptionFormats.ShortenEverything);
            }
        }
        
        static async Task RunRepl()
        {
            await new RootCommand(_dbOption).ExecuteAsync([]);
            
            AnsiConsole.MarkupLine("[grey]Type 'exit', or 'quit' to leave. Type 'help' for available commands.[/]");
            AnsiConsole.WriteLine();

            while (_isRunning)
            {
                var input = AnsiConsole.Ask<string>("[blue]naivedb>[/] ").Trim();

                if (string.IsNullOrWhiteSpace(input))
                    continue;
                
                await ProcessReplInput(input);
            }
            AnsiConsole.Clear();
            AnsiConsole.MarkupLine("[red]Quitting...[/]");
            AnsiConsole.Cursor.Hide();
            await Task.Delay(2000);
            AnsiConsole.Cursor.Show();
        }

        static async Task ProcessReplInput(string input)
        {
            var args = ParseInput(input);

            if (args.Length == 0)
                return;

            var command = args[0].ToLower();
            
            switch (command)
            {
                case "exit":
                case "quit":
                    _isRunning = false;
                    return;
                case "clear" or "cls": 
                    AnsiConsole.Clear();
                    return;
                case "help" or "h" or "?":
                    await new HelpCommand().ExecuteAsync([]);
                    return;
            }
            
            await ProcessCommand(args, _dbOption);
        }

        static async Task ProcessCommand(string[] args, DbOptions dbOption)
        {
            ICommand command = args switch
            {
                [] => new RootCommand(_dbOption),
                ["--help"] or ["-h"] or ["help"] => new HelpCommand(),
                ["--info"] or ["info"] => new InfoCommand(),
                ["--version"] or ["-v"] or ["version"] => new VersionCommand(),
                ["create", ..] or ["connect", ..] or ["drop", ..] or ["list", ..] or ["query", ..] or ["import", ..] or ["export", ..]
                    => new DatabaseCommand(dbOption),
                _ => new UnknownCommand(args)
            };
            await command.ExecuteAsync(args);
        }

        static string[] ParseInput(string input)
        {
            var args = new List<string>();
            var currentArg = new StringBuilder();
            var inQuotes = false;
            var quoteChar = '\0';

            foreach (var c in input)
            {
                if (c == '"' || c == '\'')
                {
                    if (!inQuotes)
                    {
                        inQuotes = true;
                        quoteChar = c;
                    }
                    else if (c == quoteChar)
                    {
                        inQuotes = false;
                        if (currentArg.Length > 0)
                        {
                            args.Add(currentArg.ToString());
                            currentArg.Clear();
                        }
                    }
                    else
                    {
                        currentArg.Append(c);
                    }
                }
                else if (char.IsWhiteSpace(c) && !inQuotes)
                {
                    if (currentArg.Length > 0)
                    {
                        args.Add(currentArg.ToString());
                        currentArg.Clear();
                    }
                }
                else
                {
                    currentArg.Append(c);
                }
            }

            if (currentArg.Length > 0)
            {
                args.Add(currentArg.ToString());
            }

            return args.ToArray();
        }
    }
}