# 7. Operations: metrics, troubleshooting, CLI

## What to monitor first

| Metric | Healthy | Why it matters |
|---|---|---|
| Under-replicated partitions | 0 | The safety margin is reduced; a broker died or is lagging |
| Under-min-ISR partitions | 0 | acks=all writes are rejected |
| Offline partitions | 0 | Data is unavailable |
| Active controller count (summed over the cluster) | exactly 1 | 0 — no quorum, >1 — a problem |
| ISR shrink/expand rate | ≈ 0 | "Flapping" replicas: network, GC, disk |
| Consumer lag (in messages and in time) | stable | Consumers can't keep up |
| Produce/Fetch request latency (p99) | stable | Overloaded brokers, disks, network |
| Request handler / network idle % | > 30% | The broker has hit the limit of its processing threads |
| Disk: free space, IO wait | — | A full disk = a dead broker |
| JVM GC pauses | short | Long pauses → fencing, leader elections |

Tools: a JMX exporter + Prometheus/Grafana, Burrow/kminion (lag), kafka-ui/AKHQ/Conduktor (browsing). In the lab control-center replaces all of this: it collects metrics via the AdminClient.

## Symptom → cause → what to do

| Symptom | Likely causes | Actions |
|---|---|---|
| Lag grows | Slow processing, too few consumers/partitions, frequent rebalances, a hot partition | Profile the processing, scale up to the partition count, check the key distribution |
| Constant rebalances | Processing takes longer than max.poll.interval, GC/network > session.timeout, unstable pods | Smaller batches/faster processing, a larger max.poll.interval, static membership, cooperative/KIP-848 |
| `NOT_ENOUGH_REPLICAS` on the producer | ISR < min.insync.replicas: a broker died or is lagging | Bring the broker back, check URP and the network/disk of the lagging replica |
| `Local_MsgTimedOut` / `TimeoutException` | The leader is unavailable for longer than delivery.timeout, the broker is overloaded | Check the cluster, increase the timeout, watch for backpressure |
| Duplicates at the consumers | Crashes/rebalances before the commit | Idempotent processing, commit more often, Close() on shutdown |
| Duplicates in the topic | Retries without idempotence, resending by the application | `enable.idempotence=true`, transactions |
| `OFFSET_OUT_OF_RANGE`, "lost" messages | The consumer fell behind for longer than retention | Increase retention, alert on lag in time |
| One broker is overloaded | Leaders aren't on the preferred replicas, uneven placement | Preferred leader election, reassign partitions |
| `MESSAGE_TOO_LARGE` | The message exceeds the limits | Align max.request.size / message.max.bytes / fetch, or use a claim check |
| URP "flaps" | Network, GC pauses, a slow disk on a follower | Look at GC, disk, network; replica.lag.time.max.ms |

## CLI cheat sheet

All commands run inside a lab broker:

```bash
docker exec -it -e KAFKA_HEAP_OPTS=-Xmx256m kafka-1 bash
cd /opt/kafka/bin
```

> 💡 Without `-e KAFKA_HEAP_OPTS=…` every tool inherits the broker heap (`-Xms512m`) and starts noticeably slower. Remember: a console consumer without `--max-messages` won't exit by itself while the topic is being written to — stop it with Ctrl+C.

### Topics

```bash
./kafka-topics.sh --bootstrap-server localhost:9092 --list
./kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders
./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic demo --partitions 3 --replication-factor 3 --config min.insync.replicas=2
./kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic demo --partitions 6          # increase only!
./kafka-topics.sh --bootstrap-server localhost:9092 --describe --under-replicated-partitions
./kafka-topics.sh --bootstrap-server localhost:9092 --describe --under-min-isr-partitions
./kafka-topics.sh --bootstrap-server localhost:9092 --describe --unavailable-partitions
```

### Writing and reading from the console

```bash
# a producer with keys: the line "customer-1:{...}"
./kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo \
  --property parse.key=true --property key.separator=:

# a consumer from the beginning, with keys, partitions, offsets and headers
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders --from-beginning \
  --property print.key=true --property print.partition=true --property print.offset=true \
  --property print.headers=true --max-messages 10

# read a specific partition from a specific offset
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders --partition 3 --offset 100 --max-messages 5

# in a group (shows up in kafka-consumer-groups)
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders --group cli-test
```

### Consumer groups

```bash
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group order-processing             # lag per partition
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group order-processing --members --verbose
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group order-processing --state

# rewinding (the group must be inactive; without --execute it only shows the plan)
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group analytics --reset-offsets --to-earliest --all-topics --execute
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group analytics --reset-offsets --shift-by -1000 --topic orders --execute
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group analytics --reset-offsets --to-datetime 2026-01-01T00:00:00.000 --all-topics --execute
```

### Configs, offsets, leaders

```bash
./kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name orders --describe
./kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name orders --alter --add-config retention.ms=3600000
./kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-name 1 --describe --all

./kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic orders                 # end of log per partition
./kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic orders --time -2       # start of log

./kafka-leader-election.sh --bootstrap-server localhost:9092 --election-type PREFERRED --all-topic-partitions
```

### KRaft and diagnostics

```bash
./kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status        # who leads the quorum, epoch, HW
./kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --replication   # controller lag
./kafka-broker-api-versions.sh --bootstrap-server localhost:9092 | grep "id:"         # live brokers
./kafka-log-dirs.sh --bootstrap-server localhost:9092 --describe --topic-list orders   # replica sizes on disk
./kafka-dump-log.sh --files /var/lib/kafka/data/orders-0/00000000000000000000.log --print-data-log | head
```

### Load testing

```bash
./kafka-producer-perf-test.sh --topic demo --num-records 1000000 --record-size 200 --throughput -1 \
  --producer-props bootstrap.servers=localhost:9092 acks=all linger.ms=20 compression.type=lz4
./kafka-consumer-perf-test.sh --bootstrap-server localhost:9092 --topic demo --messages 1000000
```

### From the host

The lab brokers are reachable from the host too: `localhost:19092,localhost:19093,localhost:19094` — you can connect your own programs, an IDE or kcat:

```bash
kcat -b localhost:19092 -L                       # metadata
kcat -b localhost:19092 -t orders -C -o -5 -e    # the last 5 messages of every partition
```

## Rules of thumb for parameters

- **Partitions**: ≥ the maximum number of consumers in a group; ≈ target throughput / throughput of one partition; with headroom (you can't decrease them). Very many partitions = more files and memory, slower recovery.
- **RF=3, min.insync.replicas=2** for important data. RF=2 — only for unimportant data.
- **Retention**: longer than the longest possible consumer downtime + headroom for replays.
- **Controllers**: 3 (or 5 for large clusters), on dedicated nodes in production.
- **Keys**: stable and evenly distributed; if there are "whale" keys, think about composite keys.

## Self-check

1. Which three metrics would you put on a dashboard first?
2. How do you see a group's lag from the console? How do you rewind it by an hour?
3. What do you do if one broker leads almost all partitions?
4. Why can consumers "lose" messages even though Kafka didn't lose them?

Next: [A one-page cheat sheet](08-cheatsheet.md)
