# 9. Kafka vs RabbitMQ

Both are "message brokers", but they are built in opposite ways. Kafka is a **log**, RabbitMQ is a **queue with routing**.

## The main difference

```text
RabbitMQ:  producer ─► exchange ─(routing key, bindings)─► queue ─► consumer ─ ack ─► message deleted
Kafka:     producer ─► topic/partition (log) ◄─ the consumer reads from offset N and remembers its position itself; the message stays
```

- In RabbitMQ the broker **knows about every message**: who it was delivered to, whether it was acknowledged, how many times it was redelivered. "A smart broker, a simple consumer".
- In Kafka the broker just stores the log, and **the consumer (group) tracks the position**. "A simple broker, a smart consumer".

## Comparison

| | Kafka | RabbitMQ |
|---|---|---|
| Model | A replicated log, pub/sub via consumer groups | Queues + exchanges (direct, topic, fanout, headers) |
| After reading | Stays until retention | Deleted after the ack |
| Replay history | Yes (seek, reset offsets) | No (except RabbitMQ Streams) |
| Acknowledgement | A position (offset) per partition | Every message (ack/nack/reject) |
| Ordering | Within a partition | Within a queue (broken by requeues and multiple consumers) |
| Scaling reads | Partitions (≤ 1 consumer of a group per partition) | Competing consumers on one queue |
| Routing | Only topic + key → partition | Flexible: routing keys, patterns, headers |
| Priorities, TTL, delayed delivery | No (patterns on top) | Yes (priority queues, TTL, delayed plugin) |
| DLQ | An application pattern | Built in (dead letter exchange) |
| Throughput | Very high (millions of msg/s per cluster) | High, but lower; better for small volumes with low latency |
| Storage | Long-term, on disk, cheap | Short-term: a queue should be empty |
| Protocol | Its own binary protocol (TCP) | AMQP 0-9-1, AMQP 1.0, MQTT, STOMP |
| Replication | ISR, KRaft | Quorum queues (Raft), Streams |
| Ecosystem | Kafka Connect, Streams, ksqlDB, Schema Registry, CDC | Plugins, shovel, federation |

## Mapping the concepts

| RabbitMQ | Kafka |
|---|---|
| Queue | A partition + a consumer group (the group's position) |
| Exchange + binding | None; the producer picks the topic itself (routing is done by services or Kafka Streams) |
| Competing consumers on one queue | Consumers of one group on the topic's partitions |
| Fanout to several queues | Several consumer groups on one topic |
| ack / nack / requeue | commit offset (there is no separate nack — only a retry in code or a DLQ topic) |
| Dead letter exchange | A DLQ topic (a pattern) |
| prefetch | max.poll.records / fetch.max.bytes |

## When to choose which

**Kafka**, if:
- you need an **event stream** read by many independent consumers (event-driven, event sourcing, CDC);
- you need to **store and replay** history, build analytics and streaming;
- volumes are large and throughput is high;
- per-key (per-entity) event ordering matters.

**RabbitMQ**, if:
- you need a **task queue** with competing workers and per-message acks;
- complex **routing** by keys/headers, request-reply (RPC);
- priorities, TTL, delayed delivery, a built-in DLX;
- small volumes where minimal latency and simplicity matter.

> New in both worlds: RabbitMQ has **Streams** (a Kafka-like log), and Kafka has **share groups** (KIP-932): consumers share messages rather than partitions, with per-message acknowledgement — "queues for Kafka".

## Self-check

1. Why can't you "nack" a single message in Kafka, while you can in RabbitMQ?
2. How do you implement a fanout to three services in Kafka? And in RabbitMQ?
3. Why can ordering break in RabbitMQ with several consumers on one queue?
4. Which system would you pick for a PDF-report generation queue? And for an order change log?
