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

When scanner process discovers our message after about 8 seconds, there is still 1.2 seconds left to the deadline.
At this point, the message is sent to bucket 1 (range 1-2 seconds).
The process repeats.
However, bucket 1 is obviously examined more frequently.
Not all the time, but once every second.

So after about 1 second the message is taken from bucket 1 and transferred to bucket 0.
It may have around 200ms left until deadline.
Bucket 0 is examined quite frequently, so it's possible that our message will be picked up before the deadline.
In that case the message is transferred back to bucket 0 - but at the end of it.
In the meantime, other messages may be eligible for delivery, reaching their respective deadline.

When scanning of time bucket 0 reaches our message after its deadline, it is sent to destination and forgotten.

# API

The API is straightforward:

```java
public interface MessageScheduler extends AutoCloseable {
	void sendLater(byte[] key, byte[] value, String topic, Instant when);
	
}
```

# Time bucket sizing

The number and the size of each time bucket is quite hard to determine.
Heuristically I chose 0-1 seconds for the first bucket.
The last bucket covers the range starting from 1 year up to infinity.
The range of each intermediate bucket is growing exponentially.

More buckets means less messages to process and in each iteration and greater horizontal scalability.
On the other hand, messages will be transferred between buckets more frequently, until they reach bucket 0.

Also, assuming that the last bucket covers the range from 1 year to infinity is quite arbitrary.
After all, your workloads may be quite different.
This will result in partition size imbalance.
If all your messages are always scheduled much earlier, further partitions will be empty.
On the other hand, if you keep scheduling messages many months or years in advance, only the last partitions will be used most of the time.

Thus I plan to create a dynamic...
