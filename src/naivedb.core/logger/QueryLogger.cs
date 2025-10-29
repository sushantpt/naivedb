using System.Diagnostics;
using naivedb.core.configs;
using naivedb.core.metadata;

namespace naivedb.core.logger
{
    public static class QueryLogger
    {
        private static readonly string LogDir = Path.Combine("logs", "queries");
        private static DbOptions _options = new();

        static QueryLogger()
        {
            Directory.CreateDirectory(LogDir);
        }
        
        public static void InitializeWithDbOption(DbOptions options) => _options = options;

        public static async Task LogAsync(QueryContext context, string dbName, string operation, Stopwatch? sw = null,
            string status = "Success", string? extraInfo = null)
        {
            try
            {
                if(!_options.EnableLogging) return;
            
                var logFile = Path.Combine(LogDir, $"{DateTime.UtcNow:yyyyMMdd}.log");
                var line = $"""
                            [{DateTime.UtcNow:O}]
                            Database  : {dbName}
                            Operation : {operation}
                            Query     : {context?.Query ?? "<N/A>"}
                            Started   : {context?.StartedAt:O ?? DateTime.UtcNow:O}
                            Duration  : {(sw?.ElapsedMilliseconds.ToString() ?? "<N/A>")} ms
                            Status    : {status}
                            {(!string.IsNullOrWhiteSpace(extraInfo) ? $"Info    : {extraInfo}" : "")}
                            """;
                await File.AppendAllTextAsync(logFile, line + Environment.NewLine + Environment.NewLine);
            }
            catch (Exception e)
            {
                Debug.WriteLine(e);
            }
        }

        public static async Task GenericLogAsync(string? dbName, string operation, Stopwatch? sw = null,
            string status = "Success", string? extraInfo = null)
        {
            try
            {
                if(!_options.EnableLogging) return;
                var logFile = Path.Combine(LogDir, $"{DateTime.UtcNow:yyyyMMdd}.log");
                var line = $"""
                            [{DateTime.UtcNow:O}]
                            Database  : {dbName}
                            Operation : {operation}
                            Started   : {DateTime.UtcNow:O}
                            Duration  : {(sw?.ElapsedMilliseconds.ToString() ?? "<N/A>")} ms
                            Status    : {status}
                            {(!string.IsNullOrWhiteSpace(extraInfo) ? $"Info    : {extraInfo}" : "")}
                            """;
                await File.AppendAllTextAsync(logFile, line + Environment.NewLine + Environment.NewLine);
            }
            catch (Exception e)
            {
                Debug.WriteLine(e);
            }
        }
    }
}