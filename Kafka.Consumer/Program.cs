using Confluent.Kafka;
using Kafka.Common;

const string broker = "kafka:9092";
const string topic = "order_events";
using var consumer = new ConsumerBuilder<long, OrderEvent>(new ConsumerConfig
{
    BootstrapServers = broker,
    GroupId = "user-notfication-service"
})
.Build();