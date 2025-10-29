using naivedb.cli.query.commands;
using naivedb.core.configs;

namespace naivedb.cli
{
    public class CliCommandProcessor
    {
        private readonly DbOptions _dbOption;
        private static readonly HashSet<string> RootCommands = ["--root", "--home"];
        private static readonly HashSet<string> HelpCommands = ["--help", "-h", "help"];
        private static readonly HashSet<string> InfoCommands = ["--info", "info"];
        private static readonly HashSet<string> VersionCommands = ["--version", "-v", "version"];
        private static readonly HashSet<string> DatabaseCommands = ["create", "connect", "drop", "list", "query", "import", "disconnect"];
        
        public CliCommandProcessor(DbOptions dbOption)
        {
            _dbOption = dbOption;
        }
        
        /// <summary>
        /// process and do commands and show them (result) accordingly in the console.
        /// </summary>
        /// <param name="args"></param>

        public async Task ProcessCommandAsync(string[] args)
        {
            /*
             * match command for 1 or more commands and execute accordingly
             */
            ICommand command = args switch
            {
                [] => new RootCommand(_dbOption),
                [var cmd, ..] when RootCommands.Contains(cmd) => new RootCommand(_dbOption),
                [var cmd, ..] when HelpCommands.Contains(cmd) => new HelpCommand(),
                [var cmd, ..] when InfoCommands.Contains(cmd) => new InfoCommand(),
                [var cmd, ..] when VersionCommands.Contains(cmd) => new VersionCommand(),
                [var cmd, ..] when DatabaseCommands.Contains(cmd) => new DatabaseCommand(_dbOption),
                _ => new UnknownCommand(args)
            };
            await command.ExecuteAsync(args);
        }
    }
}