namespace naivedb.cli
{
    public class AutoCompletionHelper: IAutoCompleteHandler
    {
        public string[] GetSuggestions(string text, int index)
        {
            var commands = new[]
            {
                "create", "connect", "disconnect", "drop", "list", "query",
                "tables", "add", "update", "delete", "help", "clear", "exit",
                "import", "export", "info", "version", "root", "home",
                "-all", "-n", "-data", "where", "query create table -n ", 
                "query get -n ", "query add -n ", "query update -n ", "query delete -n "
            };
            return commands
                .Where(c => c.StartsWith(text, StringComparison.OrdinalIgnoreCase))
                .ToArray();
        }

        public char[] Separators { get; set; } = [' ', '-'];
    }
}