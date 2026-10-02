# 5. Storage: segments, retention, compaction

## How a partition sits on disk

```text
/var/lib/kafka/data/orders-3/
   00000000000000000000.log        ← a segment: message batches one after another
   00000000000000000000.index      ← a sparse offset → position-in-.log index
   00000000000000000000.timeindex  ← time → offset
   00000000000000052341.log        ← the active segment (name = its first offset)
   00000000000000052341.index
   leader-epoch-checkpoint         ← the history of leader epochs (for log truncation)
```

- Only the **active segment** is written to; the rest are immutable.
- A new segment starts by size (`segment.bytes`, 1 GB) or by time (`segment.ms`, 7 days).
- **Whole closed segments** are deleted and compacted — retention is "quantized" by segments.
- To look inside: `kafka-dump-log.sh --files .../00000000000000000000.log --print-data-log`.

## Why it is fast

- **Sequential writes** to the end of a file — disks (both SSDs and HDDs) love that.
- **The OS page cache** instead of its own cache: fresh data is read from memory. Kafka doesn't fsync every message — durability comes from **replication**, not from one node's disk.
- **Zero-copy** (`sendfile`): data goes from the page cache to the socket without copying into user space (without TLS).
- **Batching and compression** all the way: producer → disk → consumer see the same batches.

## Retention (cleanup.policy=delete)

| Setting | Default | Meaning |
|---|---|---|
| `retention.ms` | 7 days | Delete segments older than this |
| `retention.bytes` | −1 (no limit) | Size limit **per partition** |
| `segment.bytes` / `segment.ms` | 1 GB / 7 days | When to close a segment |
| `log.retention.check.interval.ms` | 5 min | How often to check |

A consumer whose offset "fell off" into deleted segments gets `OFFSET_OUT_OF_RANGE` and jumps according to `auto.offset.reset`.

In the lab `clickstream` has a 10-minute retention and 100 MB per partition — a load test won't eat the disk.

## Log compaction (cleanup.policy=compact)

The topic turns into a "table": the **latest value** is kept for every key.

```text
 before compaction:  k1=a  k2=b  k1=c  k3=d  k2=null  k1=e
 after:                                      k3=d  k2=null  k1=e      (k2=null is a tombstone, removed later)
```

- Only closed segments are compacted (the "dirty" part of the log). The active one never is.
- Separate **log cleaner** threads start when the share of dirty data is ≥ `min.cleanable.dirty.ratio` (0.5).
- **A tombstone** is a message with `value = null`: it deletes the key. The tombstone itself lives for another `delete.retention.ms` (1 day) so that consumers get a chance to see it.
- `min.compaction.lag.ms` — don't compact messages that are too fresh.
- Offsets are **not renumbered** — after compaction there are "holes" in them.
- You can use `cleanup.policy=compact,delete` — the latest value per key, but no longer than retention.

Where it is used: `__consumer_offsets`, Kafka Streams changelog topics (KTable), reference data, the current state of entities, CDC (Debezium).

In the lab: `customer-profiles` — order-service writes the customer profile there after every order. There are 50 keys and thousands of messages; after compaction one per key remains. The "🪦 tombstone" button deletes a key.

## Message size

- `message.max.bytes` (broker, ≈1 MB) / `max.message.bytes` (topic), `max.request.size` (producer), `fetch.max.bytes` / `max.partition.fetch.bytes` (consumer) — they must be consistent.
- Large objects (files, images) don't go into Kafka: store them in object storage and put a reference in the event (the claim check pattern).

## Tiered storage (KIP-405)

Old segments can be offloaded to object storage (S3 etc.), keeping only the "hot" tail on the brokers. Handy for long retention without huge disks.

## Self-check

1. Why does retention delete data in "steps" rather than exactly by time?
2. Why doesn't Kafka fsync every write, and why doesn't that make it unreliable?
3. What is a tombstone, and why is it kept for a while?
4. Why do offsets have gaps after compaction?
5. Which data suits compact, and which suits delete?

Next: [Delivery guarantees and exactly-once](06-guarantees.md)
