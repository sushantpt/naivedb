using System.Diagnostics;
using System.Text;
using naivedb.cli.query.commands;
using naivedb.core.configs;
using naivedb.core.logger;
using naivedb.facade.services;
using Spectre.Console;

namespace naivedb.cli
{
    abstract class Program
    {
        private static bool _isRunning = true;
        private static readonly DbOptions DbOption = new();
        
        static async Task Main(string[] args)
        {
            try
            {
                /*
                 * 0. cold start
                 * 1. initialize with options
                 * 2. process command and show responses
                 * 3. run repl
                 */
                var coldStart = new ColdStartService(DbOption);
                coldStart.StartColdStartBackground();
                
                if (args.Length > 0)
                {
                    QueryLogger.InitializeWithDbOption(DbOption);
                    await ProcessCommand(args, DbOption);
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
            await new RootCommand(DbOption).ExecuteAsync([]);
            
            AnsiConsole.MarkupLine("[grey]Type 'exit', or 'quit' to leave. Type 'help' for available commands.[/]");
            AnsiConsole.WriteLine();
            
            ReadLine.HistoryEnabled = true;
            ReadLine.AutoCompletionHandler = new AutoCompletionHelper();

            while (_isRunning)
            {
                var input = ReadLine.Read("naivedb: ")?.Trim();

                if (string.IsNullOrWhiteSpace(input))
                    continue;
                
                await ProcessReplInput(input);
            }
            AnsiConsole.Clear();
            AnsiConsole.MarkupLine("[red]Quitting...[/]");
            await Task.Delay(1000);
            AnsiConsole.MarkupLine("[red]bye! [/]");
            AnsiConsole.Cursor.Hide();
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
            
            await ProcessCommand(args, DbOption);
        }

        static async Task ProcessCommand(string[] args, DbOptions dbOption)
        {
            var sw = Stopwatch.StartNew();
            var commandProcessor = new CliCommandProcessor(dbOption);
            await commandProcessor.ProcessCommandAsync(args);
            sw.Stop();
            double ns = sw.ElapsedTicks * (1_000_000_000.0 / Stopwatch.Frequency);
            double ms = sw.Elapsed.TotalMilliseconds;
            double seconds = ms / 1000;
            AnsiConsole.MarkupLine($"[grey]Query completed in {ms:F3} ms ({ns:F0} ns, {seconds:F6} s)[/]");
        }

        /// <summary>
        /// parse args with quotes and spaces
        /// </summary>
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