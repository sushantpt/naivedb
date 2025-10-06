namespace naivedb.cli.presentation.commands
{
    public interface ICommand
    {
        Task ExecuteAsync(string[] args);
    }
}