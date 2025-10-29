namespace naivedb.core.serialization
{
    public interface IDataSerializer
    {
        byte[] Serialize(object row);
        T? Deserialize<T>(byte[] bytes);
        string Format { get; }
    }
}