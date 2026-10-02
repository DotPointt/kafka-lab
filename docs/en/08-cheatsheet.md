# 8. A one-page cheat sheet

## 15 facts to know by heart

1. Kafka = a distributed, replicated **append-only log**. Reading doesn't delete messages.
2. A topic is split into **partitions**; ordering exists **only within a partition**.
3. Partition = `murmur2(key) % N`. One key → one partition. No key — the sticky partitioner.
4. **An offset** is a message number in a partition. A group stores its committed offset in `__consumer_offsets`.
5. Within a group, a partition is read by **one** consumer. Consumers > partitions → the extra ones sit idle.
6. Different groups read a topic **independently**, each with its own offsets.
7. A partition has one **leader** (writes/reads) and followers that **pull** the data themselves (fetch).
8. **ISR** — the replicas that keep up. Only an ISR replica can become the leader.
9. **HW**: a consumer sees only what every ISR replica has.
10. `acks=all` + `min.insync.replicas=2` + RF=3 = acknowledged data is never lost and survives losing a broker.
11. **The idempotent producer** removes retry duplicates (PID + sequence) and requires acks=all.
12. Committing **after** processing = at-least-once → processing must be idempotent.
13. `session.timeout` catches a dead process, `max.poll.interval` catches stuck processing.
14. **KRaft**: metadata is the `__cluster_metadata` log on a quorum of controllers (Raft); a majority is required. ZooKeeper was removed in 4.0.
15. Partitions can only be **added** — and that changes key routing.

## Defaults worth remembering (Java client, Kafka 4.x)

| Producer | | Consumer | | Broker / topic | |
|---|---|---|---|---|---|
| acks | all | session.timeout.ms | 45 s | default RF | 1 (use 3!) |
| enable.idempotence | true | heartbeat.interval.ms | 3 s | min.insync.replicas | 1 (use 2!) |
| linger.ms | 5 | max.poll.interval.ms | 5 min | unclean.leader.election | false |
| batch.size | 16 KB | max.poll.records | 500 | retention | 7 days |
| delivery.timeout.ms | 120 s | auto.offset.reset | latest | segment.bytes | 1 GB |
| request.timeout.ms | 30 s | enable.auto.commit | true (5 s) | replica.lag.time.max.ms | 30 s |
| max.in.flight | 5 | assignment | range, cooperative-sticky | broker.session.timeout.ms | 9 s |
| buffer.memory | 32 MB | isolation.level | read_uncommitted | message.max.bytes | ~1 MB |

**librdkafka (C#, Python) differs:** `enable.idempotence=false`, `partitioner=consistent_random` (not murmur2!), `message.timeout.ms=300 s`, `batch.size=1 MB`, `enable.auto.offset.store=true`.

## A turnkey reliable configuration

```text
Topic:     replication.factor=3, min.insync.replicas=2, unclean.leader.election.enable=false
Producer:  acks=all, enable.idempotence=true, delivery.timeout.ms with headroom, error handling in the callback, Flush on shutdown
           (C#: Partitioner = Murmur2Random if Java services write the same keys)
Consumer:  commit after processing (C#: EnableAutoOffsetStore=false + StoreOffset), idempotent processing,
           Close() on shutdown, a DLQ for "poison" messages, an alert on lag
Cluster:   ≥3 brokers in different zones, 3 controllers, monitoring of URP / ISR<min / offline / lag
```

## What happens if…

| …this happened | Short answer |
|---|---|
| The leader broker died | After ~the session timeout a leader is elected from the ISR, clients switch over |
| A broker died with RF=3, min.isr=2 | Still working, URP > 0 |
| 2 of 3 brokers died | acks=all can't write (ISR=1); in combined mode there is no KRaft quorum |
| A consumer died | A rebalance after session.timeout; unprocessed messages are repeated from the last commit |
| A consumer got stuck | It leaves the group by itself after max.poll.interval; its partitions are "lost" |
| More consumers than partitions | The extra ones sit idle |
| Partitions were added | The groups rebalance; some keys start landing in other partitions |
| A consumer fell behind for longer than retention | Some data is deleted, offset out of range → auto.offset.reset |
| A producer retries without idempotence | Duplicates and reordering are possible |
| The leader got isolated, acks=1 | The writes it acknowledged vanish after it returns (truncation) |

## Common interview questions

- How does Kafka guarantee ordering? *(within a partition; keys; idempotence with retries)*
- How do you avoid losing a message? *(acks=all, RF=3, min.isr=2, commit after processing, no unclean election)*
- How do you avoid duplicates? *(idempotent producer + idempotent processing / transactions)*
- How is Kafka different from RabbitMQ? *(a log vs a queue — see the [comparison](09-kafka-vs-rabbitmq.md))*
- What is a rebalance, why is it harmful, how do you fight it? *(cooperative-sticky, static membership, KIP-848, Close())*
- What are the ISR and the HW? Why doesn't a consumer see unacknowledged writes?
- How do you choose the number of partitions? Can you decrease it?
- How does exactly-once work in Kafka, and where are its limits?
- What is lag, and how do you monitor it?
- Why compaction, and what is a tombstone?
- What changed with the move to KRaft?

## Where to find it in the lab

| Topic | Where to look |
|---|---|
| Partitions, leaders, ISR | The broker cards, the "Partitions & offsets" table |
| Rebalancing | The order-processing card: ⏏ 💥 🧊 🔄, "+ consumer", strategy |
| Lag | The group cards, the charts, the partitions table |
| acks / idempotence | The order-service settings + "Delivery audit" |
| Failures | The ⏹ 💥 ⏸ 🌐 buttons on the brokers, "🎓 Scenarios" |
| KRaft | ★ active controller, "controller · lag" |
| Compaction | The customer-profiles topic, the tombstone button on the "Messages" tab |
