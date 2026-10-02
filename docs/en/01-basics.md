# 1. Kafka in 10 minutes

> **Kafka is a distributed, replicated, append-only log.** Producers append events to the end of the log, consumers read it from any position and remember for themselves where they stopped. Messages are not deleted after being read — they live until retention expires.

If you remember just one sentence, remember this one. Almost everything else in Kafka follows from it.

## Why you need it

- **Decouple services.** A producer doesn't know who will read an event or when. Adding a new consumer just means subscribing a new consumer group.
- **A buffer between the fast and the slow.** Load spikes pile up in the log and consumers work through them at their own pace (lag grows, but nothing is lost).
- **Many readers of one stream.** A single `orders` topic is read by order processing, analytics and auditing — independently.
- **Replay history.** Found a processing bug — rewind the group's offset and recompute. Launched a new service — feed it the history.
- **High throughput.** Hundreds of thousands to millions of messages per second per cluster thanks to batching, sequential writes and partitioning.

## The mental model

```text
                         topic "orders" = 3 partitions (each with its own log and its own offsets)
                       ┌──────────────────────────────────────────┐
  Producer ─key=c7──►  │ P0: [0][1][2][3][4][5][6] ← end of log   │ ──► group "order-processing": P0 → offset 5
          ─key=c2──►   │ P1: [0][1][2][3]                         │ ──► group "analytics":        P0 → offset 2
          ─key=c9──►   │ P2: [0][1][2][3][4]                      │       (every group has its own offsets)
                       └──────────────────────────────────────────┘
          every partition is stored in 3 copies (RF=3) on different brokers: 1 leader + 2 followers
```

- **Topic** — a named stream of events (`orders`, `payments`).
- **Partition** — a slice of a topic, a separate ordered log. Partitions are the unit of parallelism: different consumers read them at the same time.
- **Offset** — the number of a message within a partition. A (partition, offset) pair uniquely identifies a message.
- **The key** determines the partition: `murmur2(key) % partition_count`. Messages with the same key always land in the same partition → their order is preserved.
- **Broker** — a Kafka server that stores partition replicas. Several brokers = a cluster.
- **Replicas**: every partition has a **leader** (accepts writes and serves reads) and **followers** (copy the leader's log). The **ISR** are the replicas that keep up.
- **Controller** (KRaft) — the "brain" of the cluster: keeps the metadata, elects partition leaders, watches broker liveness.
- **Consumer group** — several consumers sharing a `group.id`. Partitions are split between them; each group reads the whole topic independently of other groups.

## Glossary

| Term | What it is | Where to see it in the lab |
|---|---|---|
| Broker | A Kafka server storing partition replicas | The Broker 1/2/3 cards |
| Topic | A named stream of messages | The orders, payments, clickstream… rows |
| Partition | An ordered log inside a topic | Chips numbered 0…5 |
| Replica / RF | A copy of a partition / how many copies | The same partition number appears on several brokers |
| Leader | The replica that takes the writes | A filled chip with a yellow dot |
| ISR | In-Sync Replicas — replicas that keep up | Outlined chips; dashed = out of ISR |
| Offset | A message number in a partition | The "Partitions & offsets" table |
| High Watermark | The offset up to which reads are allowed | The "end" column |
| Consumer group | Consumers sharing offsets | The order-processing and analytics cards |
| Coordinator | The broker that manages a group | "coord. b2" in the group card |
| Lag | How far the group is behind the end of the log | The big number in the group card |
| Controller | The leader of the metadata quorum (KRaft) | ★ "active controller" |

## The journey of one message

1. The producer serializes the key and the value into bytes.
2. The **partitioner** picks a partition: by key (murmur2) or, without a key, "stickily" — a whole batch goes to one partition.
3. The message goes into the producer's **local buffer**, into the batch of its partition. `Produce()` ends here — it is asynchronous.
4. A background thread sends the batch to the **partition leader** (the producer knows from metadata which broker is the leader).
5. The leader appends the batch to the end of the log (into the OS page cache) and assigns offsets.
6. Followers fetch the new data from the leader (fetch — just like regular consumers).
7. When every ISR replica has the write, the leader advances the **high watermark**. With `acks=all`, only now does the producer get the acknowledgement.
8. A consumer sends the leader a fetch request "give me data starting at offset N" and receives batches (only up to the high watermark).
9. After processing the messages, the consumer **commits the offset** — writes "group G has read partition P up to offset N" to the internal `__consumer_offsets` topic.

## What Kafka is NOT

- **Not a classic queue.** No ack/deletion of an individual message, no priorities, no "delayed delivery" out of the box. A consumer acknowledges not a message but *a position in the log*.
- **Not a database for queries.** You can't find a message by a field — only read the log (or build on top of it with Kafka Streams/ksqlDB/an external database).
- **Not globally ordered.** Ordering exists only within a partition.
- **Not "magically" exactly-once.** Duplicates and losses are a matter of producer and consumer settings and idempotent processing (see [delivery guarantees](06-guarantees.md)).

## What changed in Kafka 4.x

- **ZooKeeper is gone completely** — KRaft only (Raft-based controllers). The lab runs exactly like this.
- **KIP-848** — the new consumer group protocol (`group.protocol=consumer`): the broker computes the rebalance, no stop-the-world.
- **Share groups (KIP-932, "queues for Kafka")** — consumers share messages rather than partitions, with per-message acknowledgement. Early access in 4.0, preview in 4.1.
- The Java producer defaults to `linger.ms=5` (it used to be 0); `acks=all` and idempotence are on by default since 3.0.

## Self-check

1. Why doesn't a consumer need to "delete" a message after processing it?
2. Where is the information about how far a group has read stored?
3. How many consumers of one group can read one partition at the same time?
4. What do all messages of one partition have in common, and what does a key guarantee?
5. Why do two independent reader services need two different consumer groups?

Next: [The producer in detail](02-producer.md) · [Consumers and groups](03-consumer.md)
