[![Build and test](https://github.com/nurkiewicz/kafka-scheduler/actions/workflows/maven.yml/badge.svg)](https://github.com/nurkiewicz/kafka-scheduler/actions/workflows/maven.yml)

Scheduling and sending delayed messages using Kafka as the only storage.

# What does it do?

This small library allows sending delayed messages to Kafka topics.
Typically, in Kafka, messages are delivered as fast as possible.
However, sometimes you want to schedule sending a message for later.
Example scenarios:

* when a process fails, schedule a retry in 5 seconds
* push notification today at 5 PM
* start business process in 30 days from now
* delete PII in 2 years
* ...

Only when a certain deadline is reached, message should be sent.


# How does it work

The naive approach is simply sending messages to the `pending` topic and continuously scan that topic.
If a message reached its deadline, it is delivered to target topic.
Otherwise, this message is sent back to a `pending` topic.
Such an implementation abuses system resources.
The same message is re-processed hundreds of times.
Especially if the deadline is far from now.

This library partitions messages by deadline.
Pending messages (to be sent later) are kept in time-indexed partitions.
Each partition holds only messages with a deadline in a specific range, e.g.:

| Partition | From | To  |
|-----------|-----:|-----|
| 0         |   0s | 1s  |
| 1         |   1s | 2s  |
| 2         |   2s | 4s  |
| 3         |   4s | 8s  |
| 4         |   8s | 16s |
| 5         |  16s | 32s |
| ...       |  ... | ... |
| 9         | 256s | +∞  |

As you can see each partition, known as *time bucket* holds a different time range.
The time ranges are exponentially growing.
When you send a message to be delivered later, it is first placed in the appropriate time bucket.
For example, if you want to send something after 9.2 seconds, it will land in bucket 4 (range 8-16 seconds).

But here's the crucial part.
We do not poll for messages in that partition continuously.
Instead, each partition has a separate scanner running at fixed frequency.
E.g. bucket 4 (range 8-16s), is only examined once every 8 seconds.
We can be almost sure that all messages in that bucket have a deadline of at least 8 seconds.
So polling every 8 seconds is fine.

When scanning discovers our message after 8 seconds, there is just 1.2 seconds left to the deadline.
At this point, the message is sent to bucket 1 (range 1-2 seconds).
The process repeats.
However, bucket 1 is obviously examined more frequently.
Not all the time, but once every second.

So after about 1 second the message is taken from bucket 1 and transferred to bucket 0.
It may have around 200ms left until deadline.
Bucket 0 is examined quite frequently, so it's possible that our message will be picked up before the deadline.
In that case the message is transferred back to bucket 0 - but at the end of it.
In the meantime, other messages may be eligible for delivery, reaching their respective deadline.

# API

The API is straightforward:

```java
public interface MessageScheduler extends AutoCloseable {
	void sendLater(byte[] key, byte[] value, String topic, Instant when);
	
}
```
