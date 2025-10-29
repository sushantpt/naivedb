using naivedb.cli.presentation.renderers;

namespace naivedb.cli.query.commands
{
    public class HelpCommand : ICommand
    {
        public Task ExecuteAsync(string[] args)
        {
            var renderer = new HelpRenderer();
            renderer.RenderAsync();
            return Task.CompletedTask;
        }
    }
}