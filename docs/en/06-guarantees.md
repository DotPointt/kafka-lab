# 6. Delivery guarantees and exactly-once

## Three semantics

| Semantics | Losses | Duplicates | How to get it |
|---|---|---|---|
| **At-most-once** | possible | none | `acks=0/1`, commit the offset before processing |
| **At-least-once** | none | possible | `acks=all` + retries, commit after processing |
| **Exactly-once** | none | none (in effect) | idempotent producer + transactions + `read_committed`, or at-least-once + idempotent processing |

The end-to-end guarantee = the weakest link of the chain **producer → broker → consumer → side effects**.

## Where messages get lost

| Where | Cause | Protection |
|---|---|---|
| Producer | `acks=0/1`, delivery.timeout expired, the process died with a non-empty buffer | `acks=all`, error handling in the callback, `Flush()` on shutdown |
| Broker | The leader acknowledged (acks=1) and died before replication; unclean election | `acks=all`, RF=3, `min.insync.replicas=2`, `unclean.leader.election.enable=false` |
| Retention | The consumer fell behind for longer than retention | Monitoring lag in time, retention with headroom |
| Consumer | Committed before processing (or librdkafka auto-store with asynchronous processing) | Commit/StoreOffset only after processing |

## Where duplicates appear

| Where | Cause | Protection |
|---|---|---|
| Producer | A retry after a lost response | `enable.idempotence=true` |
| Producer application | Resending after `PossiblyPersisted`/a restart | Transactions or an idempotent consumer |
| Consumer | Died after processing but before the commit; a rebalance | Idempotent processing, transactions |

## The idempotent producer

- The broker issues a **ProducerId** and an **epoch**; every batch to a partition gets an increasing **sequence**.
- The broker remembers the last 5 batches per (PID, partition): a repeat → `DUPLICATE_SEQUENCE` (success without a write), a gap → `OUT_OF_ORDER_SEQUENCE`.
- It protects against duplicates from **retries within one session**. It doesn't protect against the application sending a message twice itself, or against a producer restart (a new PID).

## Kafka transactions

An atomic write to several partitions/topics **plus** the consumer offset commit — "all or nothing".

```csharp
var producer = new ProducerBuilder<string, string>(new ProducerConfig
{
    BootstrapServers = "...",
    TransactionalId = "order-processor-1",   // a stable instance id: "zombies" are fenced by it
    EnableIdempotence = true,
}).Build();
producer.InitTransactions(TimeSpan.FromSeconds(30));

var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
{
    BootstrapServers = "...",
    GroupId = "order-processing",
    EnableAutoCommit = false,
    IsolationLevel = IsolationLevel.ReadCommitted,   // don't see uncommitted data
}).Build();

while (true)
{
    var cr = consumer.Consume(ct);
    producer.BeginTransaction();
    producer.Produce("payments", new Message<string, string> { Key = cr.Message.Key, Value = Transform(cr.Message.Value) });
    // the offset of the input message is committed IN THE SAME transaction
    producer.SendOffsetsToTransaction(
        new[] { new TopicPartitionOffset(cr.TopicPartition, cr.Offset + 1) },
        consumer.ConsumerGroupMetadata, TimeSpan.FromSeconds(10));
    producer.CommitTransaction();   // on error — AbortTransaction() and retry
}
```

How it works:
- The **transaction coordinator** (the leader of a `__transaction_state` partition) tracks the transaction state.
- Data is written to the partitions, followed by COMMIT/ABORT **markers**.
- A consumer with `read_committed` reads only up to the **LSO** (last stable offset) and skips aborted data.
- A new instance with the same `transactional.id` bumps the epoch and **fences** the old one (the "zombie"), preventing it from committing.

The limitation: transactions cover only **Kafka → Kafka** (Kafka Streams with `processing.guarantee=exactly_once_v2` uses exactly them).

## Exactly-once with the outside world

Kafka can't atomically commit an offset together with a write to PostgreSQL. Working patterns:

1. **An idempotent consumer** (the most common): at-least-once + processing that is safe to repeat.
   - `INSERT … ON CONFLICT (order_id) DO NOTHING/UPDATE`;
   - a processed-messages table (**inbox**) in the same database transaction as the business change;
   - natural keys and versions instead of "+1" increments.
2. **The offset in the database**: store the offset in the same transaction as the data; on startup `seek()` to the saved value.
3. **Transactional outbox** (for "database + event" writes): the change and the event are written in one database transaction (an outbox table), and a separate process or Debezium CDC publishes the outbox to Kafka. It solves the dual-write problem.

## Message ordering

- Guaranteed within a partition, given an idempotent producer (or `max.in.flight=1`).
- Consumer-side retries (sending to a retry topic) **break per-key ordering** — a deliberate trade-off.
- If per-entity ordering matters and a message can't be processed, it is sometimes better to stop the partition (and alert) than to skip it.

## In the lab

- "🔍 Delivery audit" in the order-service card checks every broker acknowledgement against what is actually in the log: you can see **losses** (the "Zombie leader" scenario) and **duplicates in the log** (the "Network latency and duplicates" scenario).
- The "duplicates" counter in the order-processing group is reprocessing after a consumer crash (at-least-once).

## Self-check

1. Which three settings (two on the topic, one on the producer) are needed so that an acknowledged write is never lost?
2. Why doesn't an idempotent producer give exactly-once for the whole pipeline?
3. What is the LSO, and who sees it?
4. How do you make order processing idempotent when writing to a database?
5. Why do you need a transactional outbox if Kafka has transactions?

Next: [Operations and CLI](07-operations.md)
