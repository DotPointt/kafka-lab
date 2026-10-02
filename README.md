# Kafka Lab — a visual Apache Kafka learning lab

**English** · [Русский](README.ru.md)

A live map of a Kafka cluster that shows messages flowing from producers to partitions and consumer groups — and that you can **break**: stop and "kill" brokers, freeze processes, add latency and packet loss, stage a split brain, crash and hang consumers. Every change shows up on the map immediately and is explained in the event log ("why?").

**🌐 Online demo: https://dotpointt.github.io/kafka-lab/** — cheat sheets, flashcards and a recording of the lab in action (a broker failure, a rebalance, a consumer crash, lag). Breaking the cluster with your own hands works only in the local version — it starts with one command (see "Quick start").

![The Kafka Lab live map](docs/screenshot.png)

## What's inside

| Component | Language | What it does |
|---|---|---|
| **3 Kafka 4.1 brokers** (KRaft, no ZooKeeper) | — | Every node is a broker and a controller; RF=3, min.insync.replicas=2 |
| **order-service** | C# (.NET 10, Confluent.Kafka) | Order producer (key = customer) + profiles in a compacted topic; a "delivery audit" catches losses and duplicates |
| **order-processor** | C# | Consumer group `order-processing`: N consumers on the fly, crash/hang/restart, retries, DLQ, consume → produce |
| **analytics** | Python (confluent-kafka) | A second consumer group: pause/resume, replaying history, scaling with `--scale` |
| **clickstream-generator** | Go (franz-go) | A load producer up to 100k msg/s: batching, compression, acks, backpressure |
| **control-center** | C# (ASP.NET, AdminClient, Docker API) | Cluster monitoring, failure injection, the web UI |

In the UI:
- **Live map** — producers → brokers with partitions (leaders, ISR, offline) → consumer groups with members and lag; animated message and replication flows.
- **Failures** — on every broker: Stop (graceful), Kill (SIGKILL), Pause, Network (latency, loss, full isolation, split brain). Services: stop/kill/pause; consumers: crash, hang, restart.
- **Load** — sliders for rate, acks, idempotence, linger, compression, hot key, processing time, error rate, rebalance strategy, KIP-848, static membership.
- **🎓 Scenarios** — 15 step-by-step labs: keys and partitions, rebalancing, lag, broker failure, min.insync.replicas, data loss with acks=1 (the "zombie leader"), duplicates from retries, KRaft, DLQ, compaction…
- **Event log** — "Leader election: orders-3 (1→2)", "ISR shrank", "Group: Stable → PreparingRebalance"… every event has a "why?" button.
- **Partitions & offsets, Charts, Messages** — a per-partition lag table, 5-minute charts, reading the latest messages and sending your own (poison pill, tombstone).
- **Cheat sheets** — 10 study notes (basics, producer, consumer, replication and KRaft, storage, guarantees, operations and CLI, a one-page cheat sheet, Kafka vs RabbitMQ, how the lab works).
- **Flashcards** — 60+ questions with spaced repetition (the Leitner system); progress is saved in the browser.
- **Two languages** — English (default) and Russian, switch with EN/RU in the header.

## Quick start

You need Docker Desktop (≈ 4 GB of free memory for the containers).

```bash
docker compose up -d --build
```

Open **http://localhost:8080** in 1–2 minutes.

| Address | What |
|---|---|
| http://localhost:8080 | Kafka Lab (visualization, scenarios, cheat sheets) |
| localhost:19092, 19093, 19094 | Brokers for clients on the host (IDE, kcat) |
| http://localhost:8081 / 8082 / 8084 | HTTP API of order-service / order-processor / clickstream-generator |
| http://localhost:8090 | kafka-ui (only with `docker compose --profile ui up -d`) |

Stop: `docker compose down` (the data is kept) or `docker compose down -v` (wipe everything).

Rebuild one service after changes without touching the brokers: `docker compose up -d --build --no-deps order-service`
(a plain `docker compose up -d --build` recreates the brokers too — the data stays on the volumes, but the cluster restarts for a minute).

## Where to start

1. Open **Cheat sheets → "Kafka in 10 minutes"** (a 5-minute read). The **Kafka Lab** logo in the header always takes you there.
2. Go back to the **Live map** and hover over partition chips, group members and metrics — there are tooltips everywhere.
3. Click **🎓 Scenarios** and go through them in order: "basics" first, then "failures", then "guarantees".
4. Reinforce it on the **Flashcards** tab (keys: space — flip, 1/2/3 — rate yourself).

## The online version (GitHub Pages)

The [`.github/workflows/pages.yml`](.github/workflows/pages.yml) workflow publishes a static copy of the UI. Pages has no backend (Kafka, Docker, control-center), so:

- the cheat sheets and flashcards work fully;
- the live map replays **a recording of the real lab** — real cluster snapshots once a second and the real event log ([`demo/recording.json`](demo/recording.json)), with pause, seeking and notes on every key moment;
- the failure buttons and settings show a hint about running the lab locally.

Re-record the demo on your own lab: `python tools/record-demo.py` (about 4 minutes), then commit `demo/recording.json`.

## Repository layout

```text
docker-compose.yml           cluster + services
infra/kafka/                 broker image (+ iproute2 for tc) and the topic creation script
services/order-service/      C# producer
services/order-processor/    C# consumer group
services/analytics/          Python consumer group
services/clickstream-generator/  Go producer
services/control-center/     C# monitoring + failures + UI (wwwroot)
docs/en/, docs/ru/           cheat sheets (Markdown, shown on the "Cheat sheets" tab)
demo/recording.json          the lab recording for the online demo
tools/record-demo.py         the demo recording script
.github/workflows/pages.yml  publishing to GitHub Pages
```

More on how it works: [docs/en/10-about-lab.md](docs/en/10-about-lab.md).
