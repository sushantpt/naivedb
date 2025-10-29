using naivedb.cli.presentation.renderers;
using naivedb.core.configs;

namespace naivedb.cli.query.commands
{
    public class RootCommand : ICommand
    {
        private readonly DbOptions _options;
        public RootCommand(DbOptions options)
        {
            _options = options;
        }
        public async Task ExecuteAsync(string[] args)
        {
            var renderer = new InitialRenderer(_options);
            await renderer.RenderAsync();
            await Task.CompletedTask;
        }
    }
}