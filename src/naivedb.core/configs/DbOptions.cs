using NaiveDB.Core.Utils;

namespace naivedb.core.configs
{
    /// <summary>
    /// Represents configuration options for the database.
    /// </summary>
    public class DbOptions
    {
        private const int FourKb = 4096;
        private const int EightKb = 8192;
        private const int SixteenKb = 16384;
        private const int ThirtyTwoKb = 32768;
        
        /// <summary>
        /// Path to the directory where the database stores its data.
        /// </summary>
        public string DataPath { get; set; }

        /// <summary>
        /// Size of a database page in bytes.
        /// </summary>
        public int PageSizeBytes { get; set; } = SixteenKb;

        /// <summary>
        /// Specifies whether data compression is enabled in the database.
        /// </summary>
        public bool EnableCompression { get; set; } = true;
        
        /// <summary>
        /// Specifies whether logging is enabled in the database.
        /// </summary>
        public bool EnableLogging { get; set; } = false;
        
        /// <summary>
        /// required information for cold start and initialization.
        /// </summary>
        public string DbInfoFile { get; } = "naivedbinfo.msgpack";

        public string GetDbInfoFilePath() => Path.Combine(DataPath, DbInfoFile);
        
        public DbOptions()
        {
            DataPath = DbPathHelper.GetDefaultDbPath();
        }
    }
}