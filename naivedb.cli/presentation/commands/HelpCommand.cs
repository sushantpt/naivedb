using naivedb.cli.presentation.renderers;

namespace naivedb.cli.presentation.commands
{
    public class HelpCommand : ICommand
    {
        public Task ExecuteAsync(string[] args)
        {
            var renderer = new HelpRenderer();
            renderer.Render();
            return Task.CompletedTask;
        }
    }
}