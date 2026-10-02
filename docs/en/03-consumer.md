# 3. Consumers and consumer groups

## The consumer loop

```text
 subscribe("orders")
 loop:
    records = poll()/Consume()      ← fetch from the leaders of the assigned partitions + heartbeat logic + rebalance callbacks
    process(records)
    commit(offset of the last processed record + 1)   ← "the group has read the partition up to here"
```

A consumer has two positions in every partition:
- **position** — the offset it will request next (lives in the process memory);
- **committed offset** — the group's position saved in `__consumer_offsets`. Whoever gets the partition after a rebalance or a restart starts from it.

The gap between them is the window for **repeats** (if you crash before the commit) or **losses** (if you commit before processing).

## The order of "process ↔ commit" defines the semantics

| Order | Semantics | Consequence of a failure |
|---|---|---|
| commit → process | at-most-once | Crashed after the commit — the message is lost |
| process → commit | **at-least-once** | Crashed before the commit — the message is processed again |
| process and commit atomically | exactly-once | Kafka transactions (Kafka→Kafka) or the offset in the same database transaction |

### Auto-commit — what it actually does

`enable.auto.commit=true` commits the positions "handed out" to the application every `auto.commit.interval.ms` (5 s). In Java this happens on the next `poll()`, so processing inside the poll loop gives at-least-once. In **librdkafka** the offset is stored **the moment the message is handed out** by default (`enable.auto.offset.store=true`) — with asynchronous processing you can lose data.

### The recommended librdkafka pattern (C#)

```csharp
var config = new ConsumerConfig
{
    BootstrapServers = "localhost:19092",
    GroupId = "order-processing",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = true,          // background commit every AutoCommitIntervalMs...
    EnableAutoOffsetStore = false,    // ...but only of what we explicitly marked
    PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
};

using var consumer = new ConsumerBuilder<string, string>(config)
    .SetPartitionsAssignedHandler((c, parts) => Console.WriteLine($"Assigned: {string.Join(",", parts)}"))
    .SetPartitionsRevokedHandler((c, parts) => Console.WriteLine($"Revoked: {string.Join(",", parts)}"))
    .Build();

consumer.Subscribe("orders");
while (!ct.IsCancellationRequested)
{
    var cr = consumer.Consume(ct);
    Handle(cr.Message);            // processing (idempotent!)
    consumer.StoreOffset(cr);      // "processed" → goes out with the next auto-commit
}
consumer.Close();                  // commit + LeaveGroup → a fast rebalance
```

This is how **order-processor** works in the lab. `Close()` matters: without it the group learns that the consumer is gone only after `session.timeout.ms` (the 💥 "crash" button shows exactly that).

## Consumer group

- Members with the same `group.id` split the partitions of the subscribed topics: **a partition → at most one consumer of the group**.
- More consumers than partitions? The extra ones sit idle. **Parallelism is capped by the partition count.**
- Different groups are completely independent: their own offsets, their own pace, their own history.
- The group **coordinator** is the broker leading the `__consumer_offsets` partition that `hash(group.id)` maps to. It accepts heartbeats and commits and runs the rebalance.

## Rebalancing

Triggers: a member joined; left (`Close`); died (no heartbeat for longer than `session.timeout.ms`); got stuck (didn't call poll for longer than `max.poll.interval.ms`); the subscription or the partition count changed.

### The classic protocol (group.protocol=classic)

1. Every member sends **JoinGroup** (the group state is `PreparingRebalance`).
2. The coordinator picks a group leader and hands it the member list; the leader computes the assignment **on the client** using the strategy.
3. **SyncGroup** — the assignment is distributed to everyone (`CompletingRebalance` → `Stable`).

| Strategy | Type | How it splits | Notes |
|---|---|---|---|
| `range` | eager | Contiguous ranges per topic | Uneven with several topics |
| `roundrobin` | eager | Round-robin over all partitions | Even, but shuffles everything |
| `sticky` | eager | Even, with minimal movement | Still stop-the-world |
| `cooperative-sticky` | cooperative | Like sticky | Only the partitions that move are revoked; the rest keep being read |

**Eager**: every member gives up ALL its partitions before a rebalance — during the rebalance the group reads nothing. **Cooperative** (KIP-429): a rebalance in several rounds that moves the minimum.

### The new KIP-848 protocol (group.protocol=consumer, Kafka 4.0+)

- The **broker** computes the assignment (the `uniform` or `range` assignor), and clients receive changes via regular heartbeats.
- No global JoinGroup/SyncGroup barrier: every member converges to the target assignment on its own — rebalances are faster and don't stop the group.
- Timeouts are set by the broker: `group.consumer.session.timeout.ms`, `group.consumer.heartbeat.interval.ms`. The client-side `session.timeout.ms` and `partition.assignment.strategy` aren't used with this protocol.

### Static membership

`group.instance.id` = a permanent instance name. On a restart within `session.timeout.ms` the coordinator gives it back the same partitions **without a rebalance**. The "Consumer crash" scenario → the 🔄 button with static membership.

## Consumer timeouts

| Setting | Java default | Meaning |
|---|---|---|
| `session.timeout.ms` | 45 s | No heartbeat for longer → the member is dead (catches a dead process and network) |
| `heartbeat.interval.ms` | 3 s | How often the background thread sends heartbeats; ≈ 1/3 of session.timeout |
| `max.poll.interval.ms` | 5 min | Didn't call poll for longer → the consumer leaves the group by itself (catches stuck processing) |
| `max.poll.records` | 500 | How many records one poll returns (Java) |
| `auto.offset.reset` | latest | Where to start without a commit: earliest / latest / none |
| `auto.commit.interval.ms` | 5 s | The auto-commit period |
| `isolation.level` | read_uncommitted | read_committed — hide data of uncommitted transactions |
| `fetch.min.bytes` / `fetch.max.wait.ms` | 1 B / 500 ms | How long the broker waits for data to accumulate (batching on reads) |

> Two "deaths" of a consumer: **session.timeout** — the process or the network died; **max.poll.interval** — the process is alive but processing is stuck. In the lab: 💥 (crash) and 🧊 (hang).

## Lag

**Lag = high watermark − committed offset**, per partition. This is the key consumer metric.

- It grows when processing is slower than writing.
- It helps to convert it into time: `lag / processing rate` ≈ how many seconds behind.
- Lag based on committed offsets saw-tooths with the auto-commit period — that's normal.
- Remedies: speed up processing, batch database work, add consumers (≤ the partition count), increase the partition count, move heavy work out asynchronously.

## Useful techniques

- **pause()/resume()** — stop reading partitions while staying in the group (backpressure from a slow downstream).
- **seek()** — jump to a specific offset (for example, by time via `offsetsForTimes`).
- **Replay history**: stop the group → `kafka-consumer-groups.sh --reset-offsets --to-earliest --execute` → start it.
- **Parallel processing within a partition** breaks ordering. If you need ordering per key, parallelize by key (like the Confluent Parallel Consumer), not by message.

## Poison pill, retries and DLQ

A message that can't be processed must not be retried forever: the offset doesn't move → the partition stalls.

```text
 orders ─► processing ── ok ──► payments
             │ error
             ├─ retry N times (sometimes via orders.retry-5s, orders.retry-1m topics)
             └─ still failing ──► orders.dlq  (+ headers: the reason, the original topic/partition/offset)
```

Kafka has no built-in DLQ (unlike RabbitMQ) — it is an application pattern (Spring Kafka, Kafka Connect and MassTransit support it out of the box).

## Self-check

1. How is the committed offset different from the position?
2. Why is at-least-once the most common choice, and what does it require from processing?
3. What happens to a group of 8 consumers on a topic with 6 partitions?
4. Why is cooperative-sticky better than range when you deploy often?
5. A consumer is alive, its heartbeats keep flowing, yet it was kicked out of the group. Why?
6. How do you re-read a topic from the start for an existing group?

Next: [Replication and KRaft](04-replication.md)
