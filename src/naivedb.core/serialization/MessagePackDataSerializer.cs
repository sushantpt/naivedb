using MessagePack;
using naivedb.core.utils;

namespace naivedb.core.serialization
{
    public class MessagePackDataSerializer : IDataSerializer
    {
        public byte[] Serialize(object row)
        {
            return MessagePackSerializer.Serialize(row, MessagePackSerializerHelper.Options);
        }

        public T? Deserialize<T>(byte[] bytes)
        {
            return MessagePackSerializer.Deserialize<T>(bytes, MessagePackSerializerHelper.Options);
        }

        public string Format { get; } = "msgpack";
    }
}