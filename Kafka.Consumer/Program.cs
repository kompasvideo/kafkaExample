using Confluent.Kafka;
using Kafka.Common;

const string broker = "kafka:9092";
const string topic = "order_events";
using var consumer = new ConsumerBuilder<long, OrderEvent>(
        new ConsumerConfig
        {
            BootstrapServers = broker,
            GroupId = "user-notification-service-order-events-listener",
            AutoOffsetReset = AutoOffsetReset.Earliest
        })
    .SetValueDeserializer(new JsonValueSerializer<OrderEvent>())
    .Build();

consumer.Subscribe(topic);

while (consumer.Consume() is { } result)
{
    var message = result.Message;
    Console.WriteLine($"{result.Partition}:{result.Offset}:{message.Key}:{message.Value}");
}