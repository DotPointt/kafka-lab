# 10. How this lab is built

## Architecture

```text
                ┌───────────────────────────── docker network kafka-lab-net ─────────────────────────────┐
                │                                                                                        │
 order-service  │──orders, customer-profiles──►┐                     ┌──► order-processor (C#)  ── payments, orders.dlq ──┐
   (C#, :8081)  │                              │   kafka-1 ┐         │     group "order-processing"                      │
                │                              ├──►kafka-2 ├ KRaft ──┤                                                   │
 clickstream-   │──clickstream────────────────►┘   kafka-3 ┘ quorum  └──► analytics (Python)                            │
 generator      │                                  (broker +            group "analytics": orders, payments, clickstream │
 (Go, :8084)    │                                   controller)    ◄──────────────────────────────────────────────────┘
                │                                      ▲
                │   control-center (C#, :8080) ────────┤ AdminClient: metadata, offsets, groups
                │      UI, SSE stream, scenarios        │ Docker API (docker.sock): stop / kill / pause, exec tc netem
                │                                      └─ HTTP: /api/stats and /api/config of every service
                └────────────────────────────────────────────────────────────────────────────────────────┘
```

## Services

| Service | Language / client | Role | What to look at in the code |
|---|---|---|---|
| `order-service` | C#, Confluent.Kafka | Producer of orders (key = customerId) and profiles (compacted) | `OrderProducer.cs` — producer settings, delivery reports, librdkafka statistics; `DeliveryVerifier.cs` — checking acks ↔ the log |
| `order-processor` | C#, Confluent.Kafka | Consumer group `order-processing`, N consumers in one process, consume → transform → produce, retries, DLQ | `ConsumerWorker.cs` — the Consume/StoreOffset loop, rebalance callbacks, a crash via Dispose without Close; `ConsumerManager.cs` |
| `analytics` | Python, confluent-kafka | Consumer group `analytics`, pause/resume, scales with `--scale` | `app.py` — on_assign/on_revoke callbacks, pause() |
| `clickstream-generator` | Go, franz-go | A high-throughput producer, batching/compression/acks | `main.go` — kgo options, TryProduce and backpressure |
| `control-center` | C#, ASP.NET + Confluent.Kafka AdminClient | Monitoring, failure injection, UI | `Monitoring/ClusterMonitor.cs`, `KafkaInspector.cs`, `GroupProbe.cs`, `EventDetector.cs`, `Chaos/ChaosManager.cs` |
| `kafka-1..3` | Apache Kafka 4.1 (KRaft, combined) | Brokers + controllers | `docker-compose.yml`, `infra/kafka/` |
| `kafka-ui` (optional) | kafbat/kafka-ui | A third-party UI for browsing topics | `docker compose --profile ui up -d` → http://localhost:8090 |

## Topics

| Topic | Partitions | RF | min.isr | Notes |
|---|---|---|---|---|
| `orders` | 6 | 3 | 2 | Key = customerId, the "reliable" configuration |
| `payments` | 3 | 3 | 2 | Written by order-processor |
| `orders.dlq` | 1 | 3 | 2 | Dead Letter Queue |
| `customer-profiles` | 3 | 3 | 2 | `cleanup.policy=compact` |
| `clickstream` | 6 | 2 | 1 | The "fast" configuration, 10-minute retention |

## Shortened timeouts

So that effects show up within seconds: `replica.lag.time.max.ms=10000` (30 s by default), `leader.imbalance.check.interval.seconds=30` (300 s), consumers with `session.timeout.ms=10000` (45 s), `max.poll.interval.ms=20000` (5 min), `offsets.topic.num.partitions=10` (50).

## How the failures are made

- **Stop / Kill / Pause** — the Docker Engine API via the mounted `/var/run/docker.sock` (`docker stop` = SIGTERM, `kill` = SIGKILL, `pause` = the cgroup freezer).
- **Network** — `tc qdisc` (netem) inside the broker's network namespace via `docker exec` (the broker containers run with `cap_add: NET_ADMIN`):
  - latency/loss: `netem delay 300ms 50ms loss 10%`;
  - full isolation: `netem loss 100%`;
  - split brain: `prio` + `u32` filters on the IPs of the other brokers → `netem loss 100%` only for inter-broker traffic.
- The rules disappear when the container restarts — control-center keeps track of that.

## How control-center sees the cluster

Once a second:
1. The Docker API — container states.
2. `GetMetadata` — brokers, leaders, replicas, ISR.
3. `ListOffsets` (earliest/latest) — log boundaries; rates are computed from the increments.
4. `ListConsumerGroups` + `DescribeConsumerGroups` + `ListConsumerGroupOffsets` — members, assignments, commits → lag. This polling runs in a separate child process (`GroupProbe`, see the pitfalls below).
5. `/api/stats` of the services — what the clients themselves see.
6. Every 4 s — `kafka-metadata-quorum.sh describe --replication` inside a broker (who the active controller is).
7. `EventDetector` compares the snapshot with the previous one and writes events to the log; the browser gets everything via SSE (`/api/stream`).

## Running the C# services from an IDE

The brokers are published to the host as `localhost:19092,19093,19094` (the `EXTERNAL` listener), so you can stop a container and run the service from Visual Studio / Rider:

```bash
docker compose stop order-processor
cd services/order-processor
dotnet run            # appsettings.json already points at localhost:19092…
```

Control-center will show such consumers in the group (as external members), but won't be able to control them from the UI.

## Useful commands

```bash
docker compose up -d --build                    # build and start everything
docker compose logs -f order-processor          # service logs
docker compose up -d --scale analytics=3        # 3 instances of the Python consumer → a rebalance
docker compose restart control-center           # restart the UI backend
docker compose down                             # stop (Kafka data stays in the volumes)
docker compose down -v                          # stop and wipe all data
```

The UI (`services/control-center/wwwroot`) and the cheat sheets (`docs/en`, `docs/ru`) are mounted into the container — edits show up after a page refresh.

## The online version on GitHub Pages

A static copy of the UI is published by the `.github/workflows/pages.yml` workflow. There is no backend there, so `config.js` switches the UI into demo mode: instead of control-center's SSE stream it replays `demo/recording.json` — a recording of the real lab (snapshots once a second + the event log + the latest topic messages) with notes on the key moments. The recording is made by `tools/record-demo.py`: it connects to `/api/stream` and, following a timeline, kills and restarts a broker, adds and "crashes" consumers, pauses analytics and adds network latency. To refresh the demo, run the script against a running lab and commit the new file.

The interface and the cheat sheets come in two languages: English (the default) and Russian — switch with EN/RU in the header.

## Pitfalls found while building the lab

Real problems that came up during development — good lessons in themselves:

1. **librdkafka crashing in the admin API.** Control-center died with `SIGSEGV` / `double free detected` exactly when a broker was being stopped. Elimination (disabling AdminClient calls one by one while breaking brokers) showed that the consumer-group admin requests (`ListConsumerGroups`, `DescribeConsumerGroups`, `ListConsumerGroupOffsets`) take the process down if a broker goes away mid-request. A similar bug was reported upstream ([librdkafka#5611](https://github.com/confluentinc/librdkafka/pull/5611), [klag-exporter#111](https://github.com/softwaremill/klag-exporter/issues/111)). The fix: group polling was moved into a separate child process (`GroupProbe.cs`) — if it dies, a supervisor restarts it, while control-center and the UI keep working (the groups are marked stale for a second). The moral: monitoring code must be tested **under failures**, not on a healthy cluster.
2. **Different partitioners.** librdkafka hashes the key with CRC32 by default, Java with murmur2. Until order-service enabled `Partitioner.Murmur2Random`, the UI's partition prediction (murmur2, like Java) didn't match reality.
3. **Broker data inside the container.** The first version kept the log in the container file system; `docker compose up --build` recreated the brokers — the topics vanished, offsets restarted from zero, and the "delivery audit" honestly screamed about losses. Now the data lives on named volumes.
4. **Fatal errors of the idempotent producer.** On retries during timeouts or `NOT_ENOUGH_REPLICAS`, librdkafka sometimes reports `Fatal error: Unable to reconstruct MessageSet … unable to guarantee consistency`. After that the producer instance is useless — order-service catches `IsFatal` in the error handler and creates a new one. Without it the service would silently stop sending orders.
5. **A false "everything delivered".** The first version of the delivery audit missed a loss during split brain: the acknowledgement of a new order at the same offset arrived before the auditor read the record and overwrote the old expectation. The rule "two different acknowledgements for one offset = the first one is lost" fixed it.
6. **Commit-based lag saw-tooths.** The Python consumer commits every 5 s, so lag in messages jumped above the threshold and produced false alarms. The alarm is now computed in time (lag / read rate).

## If something goes wrong

- **"No connection"** in the header — control-center is restarting or has died: `docker compose logs control-center`.
- **A broker won't start** after experiments — `docker compose up -d kafka-1 kafka-2 kafka-3`.
- **Everything is broken** — the "🩹 Heal all" button, then "🎓 Scenarios" → "↺ Reset the lab". As a last resort: `docker compose down -v && docker compose up -d --build`.
- **You added partitions and want 6 back** — only by recreating: `docker compose down -v`.
