using System.Text;
using System.Text.Json;
using naivedb.core.utils;

namespace naivedb.core.serialization
{
    public class JsonDataSerializer : IDataSerializer
    {
        public byte[] Serialize(object row)
        {
            return Encoding.UTF8.GetBytes(JsonSerializer.Serialize(row, JsonSerializerHelper.Options));
        }

        public T? Deserialize<T>(byte[] bytes)
        {
            return JsonSerializer.Deserialize<T>(bytes, JsonSerializerHelper.Options);
        }

        public string Format { get; } = "json";
    }
}