# 4. Replication, ISR and KRaft

## The leader and the followers

Every partition has `replication.factor` replicas on different brokers. One of them is the **leader**: it accepts writes and serves reads (by default). The rest are **followers**: they pull data from the leader themselves with Fetch requests (a pull model, just like regular consumers).

```text
            LEO = log end offset (the next offset to write on a replica)
            HW  = high watermark  (every ISR replica has the data up to it → it can be served to consumers)

 leader    [0][1][2][3][4][5][6][7]        LEO=8
 follower1 [0][1][2][3][4][5][6]           LEO=7
 follower2 [0][1][2][3][4][5]              LEO=6     →  HW = min(LEO over the ISR) = 6
                         ▲
 consumers see offsets 0…5; 6 and 7 are still "unconfirmed"
```

- A consumer **never sees data above the HW** — so it can't read a write that might disappear on a leader change.
- `acks=all` acknowledges a write once the HW has reached it.

## ISR — In-Sync Replicas

- A replica is in the ISR if it has caught up with the leader's log within the last `replica.lag.time.max.ms` (30 s by default, 10 s in the lab).
- Fell behind (the broker died, froze, the network is slow) → the leader **shrinks the ISR** via the controller.
- Caught up → the ISR **expands**.
- **Under-replicated partition (URP)**: ISR < RF. It works, but the safety margin is reduced. The main operations alert.

### min.insync.replicas

The minimum ISR size for an `acks=all` write. The classic setup is **RF=3, min.insync.replicas=2**:

| Live replicas in the ISR | acks=all | acks=1 |
|---|---|---|
| 3 | ✅ | ✅ |
| 2 | ✅ (we tolerate losing one broker) | ✅ |
| 1 | ❌ `NOT_ENOUGH_REPLICAS` | ✅ (but at risk of loss) |
| 0 (no leader) | ❌ offline | ❌ offline |

## Leader election

1. The leader broker died / was fenced / was stopped.
2. The controller picks a new leader **from the ISR** (usually the first live one in the replicas list).
3. The new leader gets a new **leader epoch**; the metadata is propagated to the brokers.
4. Clients get `NOT_LEADER_OR_FOLLOWER`, refresh their metadata and switch over. The producer retries, the consumer continues from its offset.

**Controlled shutdown** (SIGTERM, `docker stop`): the broker asks the controller in advance to move leadership away from it — clients barely notice. **A crash** (SIGKILL): the cluster waits until the controller notices the missing heartbeats (`broker.session.timeout.ms`, 9 s).

**The preferred leader** is the first replica in the replicas list. After a broker returns, leadership doesn't come back to it immediately: it happens every `leader.imbalance.check.interval.seconds` (300 s, 30 s in the lab) or on the command `kafka-leader-election.sh --election-type PREFERRED`.

### Unclean leader election

If the ISR is empty (all in-sync replicas are dead), a lagging replica can be elected leader — the partition comes back to life, but acknowledged data is lost. With `unclean.leader.election.enable=false` (the default) Kafka chooses **unavailability** over loss.

### Leader epoch and log truncation

Every leader change increments the epoch. A returning replica asks the new leader: "up to which offset do our logs match in my last epoch?" (`OffsetsForLeaderEpoch`, KIP-101) and **truncates** the divergence. If the old leader managed to acknowledge something with `acks=1`, those writes vanish — the "Zombie leader" scenario shows it live.

### ELR (KIP-966, Kafka 4.x)

**Eligible Leader Replicas** — replicas removed from the ISR because the ISR dropped below `min.insync.replicas`, but guaranteed to contain all data up to the HW. They can safely be elected leader when the ISR is empty. Visible in `kafka-topics.sh --describe` (the `Elr` and `LastKnownElr` fields).

## KRaft: controllers instead of ZooKeeper

Since Kafka 4.0 ZooKeeper is gone. Cluster metadata is a **log** too: the `__cluster_metadata` topic, replicated by the **controllers** using the Raft protocol.

```text
   controller 1 (follower) ◄─┐
   controller 2 (LEADER)  ───┼── metadata log: "topic created", "leader of orders-3 = 2", "ISR of orders-1 = [1,3]", "broker 2 fenced"…
   controller 3 (follower) ◄─┘
          ▲  heartbeats, AlterPartition (ISR changes)
          │
   brokers: receive metadata changes and apply them locally
```

- The **active controller** (the leader of the Raft quorum) makes all the decisions. The rest are hot standbys.
- The quorum needs a **majority**: 3 controllers survive losing 1, 5 survive losing 2. Lose the majority and the metadata freezes: you can't elect leaders, fence a broker or create a topic.
- **Broker registration and fencing**: a broker sends the controller heartbeats (`broker.heartbeat.interval.ms`, 2 s). No heartbeat for longer than `broker.session.timeout.ms` (9 s) → the broker is **fenced**: removed from the metadata, its replicas are dropped from ISRs, leadership moves away.
- A regular client doesn't see the active controller (it only talks to brokers). To look: `kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status` / `--replication` — this is how control-center does it.
- **Combined mode** (`process.roles=broker,controller`) — as in the lab. In production controllers usually run separately (`process.roles=controller`).

## What happens if… (RF=3, min.isr=2, 3 nodes)

| Event | What you see | acks=all writes | Reads |
|---|---|---|---|
| 1 broker stopped (graceful) | Leaders moved in advance, URP > 0 | ✅ | ✅ |
| 1 broker killed (SIGKILL) | ~9 s of unavailability for its partitions, then elections | ✅ after the elections | ✅ after the elections |
| A broker paused for 4 s | A latency spike | ✅ (slower) | ✅ |
| A broker paused for 20 s | Fencing, elections; after unfreezing it catches up | ✅ after the elections | ✅ |
| 2 brokers killed | The KRaft quorum is lost, ISR=1 | ❌ | Partially |
| The active controller isolated | A new quorum leader within seconds, the isolated broker gets fenced | ✅ after the elections | ✅ |
| A broker cut off from the cluster, clients still see it | A "zombie leader": acknowledges with acks=1, then truncates its log | ❌ on the zombie, ✅ on the new leader | ✅ |
| 300 ms network latency on a broker | acks=all p99 grows (we wait for its fetch) | ✅ slower | ✅ |

## Rack awareness and reassignment

- `broker.rack` + rack-aware replica placement: copies of a partition in different availability zones.
- `client.rack` + `replica.selector.class` (KIP-392): a consumer reads from the nearest follower replica, saving cross-zone traffic.
- `kafka-reassign-partitions.sh` — moves replicas between brokers (for example, when adding nodes). A new broker gets nothing by itself — partitions have to be reassigned.

## Self-check

1. Why can't a consumer read a message that hasn't been replicated to every ISR replica yet?
2. What happens to writes with RF=3, min.insync.replicas=2 and two brokers down?
3. How is a controlled shutdown different from a crash for clients?
4. What is a leader epoch, and why does a replica truncate its own log?
5. Why is the number of controllers odd?

Next: [Storage, retention, compaction](05-storage.md)
