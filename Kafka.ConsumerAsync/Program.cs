using System.Threading.Channels;
using Confluent.Kafka;
using Kafka.Common;

const string broker = "kafka:9092";
const string topic = "order_events";
const int channelCapasity = 10;

var channel = Channel.CreateBounded<Message<long, OrderEvent>>(
    new BoundedChannelOptions(channelCapasity)
    {
        SingleWriter = true,
        SingleReader = false,
        AllowSynchronousContinuations = true,
    });
var consumeTask = Task.Factory.StartNew(() => { }, TaskCreationOptions.LongRunning);
var handleTask = Task.Factory.StartNew(() => { }, TaskCreationOptions.LongRunning);
await Task.WhenAll(consumeTask, handleTask);

async Task Consume(ChannelWriter<Message<long, OrderEvent>> channelWriter)
{
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
    
}