namespace Kafka.Common;

public sealed record OrderEvent(long OrderId,long ClientId, 
    OrderEventType EventType, DateTimeOffset Timestamp);

public enum OrderEventType
{ 
    Created,
    Changed,
    Cancelled,
    Completed,
}