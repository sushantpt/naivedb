using naivedb.cli.presentation.renderers;
using naivedb.core.configs;

namespace naivedb.cli.presentation.commands
{
    public class RootCommand : ICommand
    {
        private readonly DbOptions _options;
        public RootCommand(DbOptions options)
        {
            _options = options;
        }
        public Task ExecuteAsync(string[] args)
        {
            var renderer = new InitialRenderer(_options);
            renderer.Render();
            return Task.CompletedTask;
        }
    }
}