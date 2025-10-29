using naivedb.core.serialization;

namespace naivedb.core.configs
{
    /*
     * represents db info file that is to be used for cold start, initialization, and overall db management operations.
     */
    public class DbInfo
    {
        public string? CurrentDatabase { get; set; }
        public DateTime? LastConnectedUtc { get; set; }
        
        private readonly MessagePackDataSerializer _serializer = new();
        
        public async Task<DbInfo?> LoadAsync(string filePath)
        {
            if (!File.Exists(filePath))
                return new DbInfo();
            var bytes = await File.ReadAllBytesAsync(filePath);
            return _serializer.Deserialize<DbInfo>(bytes);
        }
        
        public async Task SaveAsync(string filePath)
        {
            var bytes = _serializer.Serialize(this);
            await File.WriteAllBytesAsync(filePath, bytes);
        }
    }
}