namespace naivedb.cli.query.commands
{
    public interface ICommand
    {
        Task ExecuteAsync(string[] args);
    }
}