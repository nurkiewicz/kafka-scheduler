[![Build and test](https://github.com/nurkiewicz/kafka-scheduler/actions/workflows/maven.yml/badge.svg)](https://github.com/nurkiewicz/kafka-scheduler/actions/workflows/maven.yml)

Scheduling and sending delayed messages using Kafka as the only storage.

# What does it do?

This small library allows sending delayed messages to Kafka.
Typically, messages are delivered as fast as possible.
However, sometimes you want to schedule sending a message for later.
Example scenarios:

* when process fails, schedule a retry in 5 seconds
* 

# How does it work

Pending messages to be sent later are kept in time-indexed partitions.
Each partition hold messages with a deadline in a specific range, e.g.:

| Partition | From | To  |
|-----------|------|-----|
| 0         | 0s   | 1s  |
| 1         | 1s   | 2s  |
| 2         | 2s   | 4s  |
| 3         | 4s   | 8s  |
| 3         | 8s   | 16s |
| 4         | 16s  | +∞  |

As you can see partition, known as *time bucket* holds a different time range.
The time range is exponentially growing.
When you send a message to be delivered later, it is first placed in the appropriate time bucket.
For example, if you want to send something after 10 seconds, it will land in bucket 3.

