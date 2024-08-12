using System.Threading.Channels;
using Confluent.Kafka;
using Kafka.Common;

const string broker = "kafka:9092";
const string topic = "order_events";
const int channelCapasity = 10;

using var consumer = new ConsumerBuilder<long, OrderEvent>(
        new ConsumerConfig
        {
            BootstrapServers = broker,
            GroupId = "user-notification-service-order-events-listener",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true,
            EnableAutoOffsetStore = false,
            //MaxPollIntervalMs = 5000,
        })
    .SetValueDeserializer(new JsonValueSerializer<OrderEvent>())
    .Build();
consumer.Subscribe(topic);

while (consumer.Consume() is { } result)
{
    var message = result.Message;
    Console.WriteLine($"{result.Partition}:{result.Offset}:{message.Key}:{message.Value}");
    // await Task.Delay(TimeSpan.FromSeconds(10));
    // throw new Exception("Анейбл ту консъюм эис мессадж");
}