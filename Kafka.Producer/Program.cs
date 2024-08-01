using AutoFixture;
using Confluent.Kafka;
using Kafka.Common;

const string broker = "kafka:9092";
const string topic = "order_events";
using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
var producer = new ProducerBuilder<long, OrderEvent>(
        new ProducerConfig
        {
            BootstrapServers = broker,
            Acks = Acks.All,
        })
    .SetValueSerializer(new JsonValueSerializer<OrderEvent>())
    .Build();

var fixture = new Fixture();
var events = fixture.CreateMany<OrderEvent>(10);
foreach (var orderEvent in events)
{
    cts.Token.ThrowIfCancellationRequested();
    await producer.ProduceAsync(topic, new Message<long, OrderEvent>
        {
            Key = orderEvent.OrderId,
            Value = orderEvent,
        },
        cts.Token);
}