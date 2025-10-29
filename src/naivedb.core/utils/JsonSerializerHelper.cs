using System.Text.Json;

namespace naivedb.core.utils
{
    /// <summary>
    /// Provides a static instance of JsonSerializerOptions configured with default settings.
    /// </summary>
    public static class JsonSerializerHelper
    {
        /// <summary>
        /// Represents a pre-configured static instance of JsonSerializerOptions with default settings (camel casing for naming policy and indented JSON output).
        /// </summary>
        public static readonly JsonSerializerOptions Options = new()
        {
            WriteIndented = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };
    }
}