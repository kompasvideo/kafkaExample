using System.Text.Json;
using Confluent.Kafka;
using System.Text.Json.Serialization;

namespace Kafka.Common;

public sealed class JsonValueSerializer<T> :ISerializer<T>, IDeserializer<T>
{
    private static readonly JsonSerializerOptions s_serializerOptions;

    static JsonValueSerializer()
    {
        s_serializerOptions = new JsonSerializerOptions();
        s_serializerOptions.Converters.Add(new JsonStringEnumConverter());
    }

    public byte[] Serialize(T data, SerializationContext context) 
        => JsonSerializer.SerializeToUtf8Bytes(data, s_serializerOptions);

    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        if(isNull)
            throw new ArgumentNullException(nameof(data), "Null data encountered");
        return JsonSerializer.Deserialize<T>(data, s_serializerOptions) 
               ?? throw new ArgumentNullException(nameof(data),"Null data encountered");
    }
}