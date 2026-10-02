# 2. The producer: how a write gets into Kafka

## Inside the producer

```text
 application ──Produce(key, value)──► Serializer ─► Partitioner ─► RecordAccumulator (buffer)
                                                                   ├─ batch for orders-0
                                                                   ├─ batch for orders-1  ──► Sender (background thread)
                                                                   └─ batch for orders-2         │ ProduceRequest to the leader
                                                                                                  ▼
             delivery report / callback ◄──────────── broker response (offset or error) ◄── Broker (partition leader)
```

- **Produce() is asynchronous.** It puts the message into the buffer and returns immediately. The result (an offset or an error) arrives later in a callback / delivery handler.
- Messages are grouped into **per-partition batches**. A batch is sent when it is full (`batch.size`) or `linger.ms` has expired.
- Sending and retries are handled by the client's **background thread**. librdkafka (C#, Python, the Go wrapper) runs its own threads per broker.

## Choosing a partition

| Situation | What happens |
|---|---|
| A partition is set explicitly | Write there |
| There is a key | `murmur2(key) & 0x7fffffff % N` — one key → one partition |
| No key | Sticky partitioner (KIP-480): a whole batch goes to one partition, then to the next |

> ⚠ **The .NET/Python pitfall:** librdkafka uses `consistent_random` (CRC32) by default, while Java uses murmur2. The same key from C# and from Java lands in **different** partitions. Fix it with `Partitioner = Partitioner.Murmur2Random` (order-service does this).

Ordering is guaranteed **only within a partition**. Need the events of one customer in order — make `customerId` the key. A skewed key distribution = a **hot partition** (the "Key → partition" scenario with a hot key).

## acks — how many acknowledgements to wait for

| acks | Who must write it | Speed | Risk |
|---|---|---|---|
| `0` | Nobody, we don't wait for a response | Maximum | Losses are invisible |
| `1` | The leader only | High | Lost if the leader changes before replication |
| `all` (`-1`) | All ISR replicas, and ISR ≥ `min.insync.replicas` | Lower | Acknowledged data is never lost (with RF≥3, min.isr≥2) |

`min.insync.replicas` is a **topic/broker** setting, not a producer one. If the ISR is smaller, an `acks=all` write gets `NOT_ENOUGH_REPLICAS`. Kafka prefers to refuse rather than lose.

## Retries, idempotence and ordering

A network request can get "lost" in two places: the request never arrived (a retry is safe) or **it arrived but the response got lost** (a retry = a duplicate). The producer can't tell these cases apart.

- **Without idempotence** retries create duplicates, and with `max.in.flight > 1` they also reorder data (batch 2 gets written before the retried batch 1).
- **`enable.idempotence=true`**: the broker gives the producer a **ProducerId (PID)** and an epoch, and every batch gets a **sequence number**. The broker recognizes a retry with the same number and doesn't write it twice; a gap in the numbers is an `OutOfOrderSequence` error. Ordering is preserved with `max.in.flight ≤ 5`.
- Requirements: `acks=all`, `retries > 0`, `max.in.flight ≤ 5`.
- Idempotence works within **one producer session** and one partition. Restarting the process = a new PID. For guarantees across restarts and across topics — transactions.
- Under mass timeouts librdkafka "bumps" the producer epoch (KIP-360) to restore ordering; after that the broker no longer recognizes a rare retry. In the lab (the "Network latency and duplicates" scenario) idempotence cuts duplicates several times over, but not to zero.
- Sometimes an idempotent producer gets a **fatal** error (it can't guarantee ordering anymore) — such an instance must be recreated. Handle `IsFatal` in the error handler (order-service does this).

## Timeouts

| Java | librdkafka (C#/Python) | Meaning |
|---|---|---|
| `linger.ms` (5) | `linger.ms` / `queue.buffering.max.ms` (5) | How long to wait while filling a batch |
| `batch.size` (16 KB) | `batch.size` (1 MB), `batch.num.messages` (10,000) | Maximum batch size |
| `request.timeout.ms` (30 s) | `socket.timeout.ms` (60 s) on the client; `request.timeout.ms` — how long the broker waits for replicas | How long to wait for a response to a request |
| `delivery.timeout.ms` (120 s) | `message.timeout.ms` / `delivery.timeout.ms` (300 s) | Total delivery time, retries included |
| `buffer.memory` (32 MB) | `queue.buffering.max.kbytes` (1 GB), `.max.messages` (100,000) | Local buffer size |
| `max.block.ms` (60 s) | — (immediately `Local_QueueFull`) | How long to block when the buffer is full |
| `max.in.flight.requests.per.connection` (5) | `max.in.flight` (1,000,000!) | Requests without a response per connection |
| `enable.idempotence` (true) | `enable.idempotence` (**false**) | Deduplication of retries |

> 💡 Mind the different defaults: in librdkafka idempotence is **off** and `max.in.flight` is huge. For reliable writes from C#, set `EnableIdempotence = true` explicitly.

## Batching and compression

- Compression (`compression.type`: gzip, snappy, lz4, zstd) is applied to **a whole batch** — the bigger the batch, the better the ratio.
- The broker usually stores the batch in the same compressed form, and the consumer decompresses it itself — saving network and disk.
- The trade-off: `linger.ms` ↑ → bigger batches → throughput ↑, latency ↑. Try it on clickstream-generator: linger 0 vs 50 ms, none vs zstd.

## Backpressure

If the brokers are unavailable or can't keep up, messages pile up in the buffer. When it is full:

- Java: `send()` blocks for up to `max.block.ms`, then throws.
- librdkafka: `Produce()` immediately throws `Local_QueueFull` — the application must slow down (the order-service generator does this).
- franz-go: `Produce()` blocks, `TryProduce()` returns `ErrMaxBuffered`.

## A C# example (Confluent.Kafka)

```csharp
var config = new ProducerConfig
{
    BootstrapServers = "localhost:19092,localhost:19093,localhost:19094",
    Acks = Acks.All,
    EnableIdempotence = true,                 // deduplicate retries
    LingerMs = 5,
    CompressionType = CompressionType.Lz4,
    MessageTimeoutMs = 30_000,                // delivery.timeout.ms
    Partitioner = Partitioner.Murmur2Random,  // same as the Java client
};

using var producer = new ProducerBuilder<string, string>(config).Build();

// 1) Asynchronously with a callback — maximum throughput
producer.Produce("orders", new Message<string, string> { Key = "customer-007", Value = json }, report =>
{
    if (report.Error.IsError)
        Console.WriteLine($"Not delivered: {report.Error.Reason}, status: {report.Status}"); // PossiblyPersisted?
    else
        Console.WriteLine($"OK: {report.TopicPartitionOffset}");
});

// 2) Awaiting — convenient, but slow if you await every message
var result = await producer.ProduceAsync("orders", new Message<string, string> { Key = "customer-007", Value = json });

producer.Flush(TimeSpan.FromSeconds(10)); // before exiting: wait for the buffer to be sent
```

`DeliveryReport.Status`:
- `Persisted` — the broker acknowledged it;
- `NotPersisted` — definitely not written (safe to retry);
- `PossiblyPersisted` — a timeout: it may have been written (a retry may create a duplicate).

## Errors: which ones to retry

| Error | Retried by the client | What it means |
|---|---|---|
| `NOT_LEADER_OR_FOLLOWER`, `LEADER_NOT_AVAILABLE` | Yes (+ a metadata refresh) | A leader election is in progress |
| `NOT_ENOUGH_REPLICAS(_AFTER_APPEND)` | Yes | ISR < min.insync.replicas |
| `REQUEST_TIMED_OUT`, network errors | Yes | The broker didn't answer in time |
| `MESSAGE_TOO_LARGE` | No | Larger than `max.request.size` / `message.max.bytes` |
| `TOPIC_AUTHORIZATION_FAILED`, `UNKNOWN_TOPIC` (with auto-creation off) | No | Permissions/configuration |
| `Local_MsgTimedOut` | — | delivery.timeout expired: the retries ran out |

## Ready-made profiles

**Reliable** (money, orders): `acks=all`, `enable.idempotence=true`, a topic with RF=3 + `min.insync.replicas=2`, `delivery.timeout.ms` with headroom, error handling in the callback.

**Fast** (metrics, clicks, logs): `acks=1` (or 0), `linger.ms=20…100`, `compression=lz4/zstd`, a large `batch.size`. Losing some events is acceptable.

## Self-check

1. Why does `Produce()` return before the broker has written the message? How do you find out the result?
2. What is dangerous about `acks=1`? When is it justified?
3. What does an idempotent producer guarantee, and what does it NOT guarantee?
4. Why can a key from C# land in a different partition than the same key from Java by default?
5. How does linger.ms affect latency and throughput?

Next: [Consumers and groups](03-consumer.md)
