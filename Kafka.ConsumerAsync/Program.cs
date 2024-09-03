using System.Threading.Channels;
using Confluent.Kafka;
using Kafka.Common;

const string broker = "kafka:9092";
const string topic = "order_events";
const int channelCapasity = 10;

var channel = Channel.CreateBounded<ConsumeResult<long, OrderEvent>>(
    new BoundedChannelOptions(channelCapasity)
    {
        SingleWriter = true,
        SingleReader = true,
        AllowSynchronousContinuations = true,
        FullMode = BoundedChannelFullMode.Wait,
    });

using var kafkaConsumer = new ConsumerBuilder<long, OrderEvent>(
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

var consumeTask = Task.Factory.StartNew(() => Consume(channel.Writer, kafkaConsumer), 
    TaskCreationOptions.LongRunning).Unwrap();
var handleTask = Task.Factory.StartNew(() => Handle(channel.Reader, kafkaConsumer), 
    TaskCreationOptions.LongRunning).Unwrap();
await Task.WhenAll(consumeTask, handleTask);

async Task Handle(ChannelReader<ConsumeResult<long, OrderEvent>> channelReader,
    IConsumer<long, OrderEvent> consumer)
{
    await foreach (var consumerResult in channelReader.ReadAllAsync())
    {
        await Task.Delay(100);
        Console.WriteLine(
            "{0}:{1}:Handle:{2}:{3}",
            consumerResult.Partition.Value,
            consumerResult.Offset.Value,
            consumerResult.Message.Key,
            consumerResult.Message.Value);
        consumer.StoreOffset(consumerResult);
    }
}

async Task Consume(ChannelWriter<ConsumeResult<long, OrderEvent>> channelWriter,
    IConsumer<long, OrderEvent> consumer)
{
    consumer.Subscribe(topic);
    while (consumer.Consume() is { } result)
    {
        await channelWriter.WriteAsync(result);
        Console.WriteLine("{0}:{1}:WriteToChannel",
            result.Partition.Value,
            result.Offset.Value);
    }
    
    channelWriter.Complete();
}