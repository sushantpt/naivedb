namespace naivedb.core.configs
{
    /// <summary>
    /// Represents configuration options for the database.
    /// </summary>
    public class DbOptions
    {
        /// <summary>
        /// Path to the directory where the database stores its data.
        /// </summary>
        public string DataPath { get; set; } = "data";
    }
}