#!/usr/bin/env python3
"""
Records the demo for the static copy of the site (GitHub Pages).

Connects to the SSE stream of a running control-center (http://localhost:8080/api/stream), performs actions
along a timeline (killing a broker, a rebalance, a consumer crash...) and saves REAL cluster snapshots
and the event log to demo/recording.json. On Pages the UI replays this recording instead of the live stream.
Annotations are stored in both UI languages: {"en": ..., "ru": ...}.

    docker compose up -d --build        # the lab must be running
    python tools/record-demo.py         # ~4 minutes
"""

import json
import os
import sys
import threading
import time
import urllib.request
from datetime import datetime, timezone

BASE = os.environ.get("LAB_URL", "http://localhost:8080")
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "demo", "recording.json")

latest = {"snap": None}
annotations = []
started_at = None


def call(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method,
                                 headers={"content-type": "application/json"} if data else {})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return json.loads(r.read() or b"{}")
    except Exception as e:  # noqa: BLE001
        print(f"  ! {method} {path}: {e}", file=sys.stderr)
        return {}


def busiest_broker(avoid_controller=True):
    snap = latest["snap"]
    ctrl = (snap["cluster"].get("quorum") or {}).get("leaderId")
    orders = next(t for t in snap["topics"] if t["name"] == "orders")
    counts = {}
    for p in orders["partitions"]:
        counts[p["leader"]] = counts.get(p["leader"], 0) + 1
    ranked = sorted((b for b in counts if b >= 0), key=lambda b: -counts[b])
    for b in ranked:
        if not avoid_controller or b != ctrl:
            return b
    return ranked[0]


state = {}


def note(title_en, title_ru, text_en, text_ru):
    """An annotation on the demo timeline, in both UI languages."""
    t = round(time.time() - started_at, 1)
    annotations.append({"t": t, "title": {"en": title_en, "ru": title_ru}, "text": {"en": text_en, "ru": text_ru}})
    print(f"[{t:6.1f}s] {title_en}")


# ---------------------------------------------------------------- the recording timeline
def a_start():
    note("Normal operation", "Штатная работа",
         "order-service (C#) writes orders keyed by customer, clickstream-generator (Go) writes clicks. "
         "The <b>order-processing</b> (C#) and <b>analytics</b> (Python) groups read them independently. "
         "Particles: orders are yellow, payments green, clicks blue; thin particles between brokers are replication to followers. "
         "Hover over the partition chips: the leader (filled), replicas in the ISR, the end of the log.",
         "order-service (C#) пишет заказы с ключом-клиентом, clickstream-generator (Go) — клики. "
         "Группа <b>order-processing</b> (C#) и <b>analytics</b> (Python) читают их независимо. "
         "Частицы: заказы — жёлтые, платежи — зелёные, клики — синие; тонкие частицы между брокерами — репликация на follower-ы. "
         "Наведи мышь на чипы партиций: лидер (залитый), реплики в ISR, конец лога.")


def a_keys():
    results = [call("POST", "/api/svc/order-service/orders", {"customerId": "customer-007"}) for _ in range(3)]
    parts = ",".join(map(str, {r.get("partition") for r in results}))
    note("Key → partition", "Ключ → партиция",
         f"Three orders with the key customer-007 landed in the same partition orders-{parts}: "
         "partition = murmur2(key) % 6. That is why one customer's orders are always in order (see the event log).",
         f"Три заказа с ключом customer-007 легли в одну и ту же партицию orders-{parts}: "
         "партиция = murmur2(key) % 6. Поэтому заказы одного клиента всегда упорядочены (см. журнал событий).")


def a_kill():
    b = state["killed"] = busiest_broker()
    call("POST", f"/api/brokers/{b}/kill")
    note(f"💥 Kill broker {b} (SIGKILL)", f"💥 Kill broker {b} (SIGKILL)",
         "The broker is killed without a controlled shutdown. For about 9 s (broker.session.timeout.ms) the KRaft controller still considers it alive — "
         "the partitions it led are unavailable and order-service p99 spikes. Then the controller fences the broker, "
         "elects new leaders from the ISR, the ISRs shrink and URP (under-replicated partitions) grows in the header.",
         "Брокер убит без controlled shutdown. Около 9 с (broker.session.timeout.ms) контроллер KRaft ещё считает его живым — "
         "партиции, где он был лидером, недоступны, p99 у order-service подскакивает. Затем контроллер фенсит брокер, "
         "выбирает новых лидеров из ISR, ISR сжимаются, в шапке растёт URP (under-replicated partitions).")


def a_add_consumer():
    call("POST", "/api/svc/order-processor/instances")
    note("+1 consumer in order-processing", "+1 консюмер в order-processing",
         "A new member joins the group → a rebalance. The cooperative-sticky strategy takes only some partitions "
         "away from the old consumers; the rest keep being read without a pause.",
         "Новый участник вступает в группу → ребаланс. Стратегия cooperative-sticky забирает у старых консюмеров "
         "только часть партиций, остальные продолжают читаться без остановки.")


def a_start_broker():
    b = state["killed"]
    call("POST", f"/api/brokers/{b}/start")
    note(f"▶ Broker {b} is started again", f"▶ Broker {b} снова запущен",
         "The broker loads its log from disk, catches up with the leaders (fetch) and rejoins the ISR — URP drops to 0. "
         "It hasn't become a leader yet: that happens at the preferred leader election.",
         "Брокер поднимает лог с диска, догоняет лидеров (fetch) и возвращается в ISR — URP уходит в 0. "
         "Лидером он пока не стал: это произойдёт при выборах предпочтительных лидеров.")


def a_preferred():
    call("POST", "/api/leaders/preferred")
    note("⚖ Preferred leader election", "⚖ Выборы предпочтительных лидеров",
         "Leadership goes back to the \"preferred\" replicas (the first in the replicas list), and the load is even again. "
         "Often the controller gets there by itself — auto.leader.rebalance (every 30 s in the lab), and then a manual "
         "kafka-leader-election run reports \"0 re-elected, N already in place\". See both events in the log.",
         "Лидерство возвращается к «предпочтительным» репликам (первым в списке replicas), и нагрузка снова равномерна. "
         "Часто контроллер успевает сам — auto.leader.rebalance (на стенде раз в 30 с), тогда ручной запуск "
         "kafka-leader-election покажет «переизбрано 0, уже на месте N». Смотри оба события в журнале.")


def a_crash_consumer():
    stats = call("GET", "/api/svc/order-processor/stats")
    inst = (stats.get("instances") or [{}])[0]
    if inst.get("id") is not None:
        call("POST", f"/api/svc/order-processor/instances/{inst['id']}/crash")
    client = inst.get("clientId", "")
    note(f"💥 Consumer {client} crashed", f"💥 Крэш консюмера {client}",
         "The consumer died without a commit and without LeaveGroup. For about 10 s it stays in the group as a \"ghost\" (session.timeout.ms), "
         "and nobody reads its partitions. Then a rebalance, and the new owner re-reads the messages after the last commit — "
         "the \"duplicates\" counter grows: this is what at-least-once looks like.",
         "Консюмер умер без коммита и без LeaveGroup. Около 10 с он остаётся в группе «призраком» (session.timeout.ms), "
         "его партиции никто не читает. Потом ребаланс, а новый владелец перечитывает сообщения после последнего коммита — "
         "растёт счётчик «дубли»: так выглядит at-least-once.")


def a_pause():
    call("PUT", "/api/svc/analytics/config", {"paused": True})
    note("⏸ analytics: consumer.pause()", "⏸ analytics: consumer.pause()",
         "The Python consumer stopped fetching data but stays in the group. The analytics lag grows, while order-processing "
         "keeps working as if nothing happened: every consumer group has its own offsets.",
         "Python-консюмер перестал забирать данные, но остаётся в группе. Lag группы analytics растёт, а order-processing "
         "работает как ни в чём не бывало: у каждой consumer group свои offset-ы.")


def a_resume():
    call("PUT", "/api/svc/analytics/config", {"paused": False})
    note("▶ analytics: resume()", "▶ analytics: resume()",
         "The consumer continues from the same offset and catches up: its read rate is far above the write rate until the lag is gone.",
         "Консюмер продолжает с того же offset и догоняет поток: скорость чтения резко выше скорости записи, пока lag не уйдёт.")


def a_latency():
    b = state["slow"] = busiest_broker(avoid_controller=False)
    call("POST", f"/api/brokers/{b}/network", {"latencyMs": 300, "jitterMs": 50, "lossPct": 0, "isolated": False, "splitFromBrokers": False})
    note(f"🐢 300 ms network latency on broker {b}", f"🐢 Задержка сети 300 мс у broker {b}",
         "tc netem delays the broker's outgoing traffic. A producer with acks=all waits until the write reaches every ISR replica — "
         "the acknowledgement latency (p99 in the order-service card) grows by hundreds of milliseconds.",
         "tc netem задерживает исходящий трафик брокера. Producer с acks=all ждёт, пока запись дойдёт до всех реплик ISR, — "
         "задержка подтверждения (p99 в карточке order-service) вырастает на сотни миллисекунд.")


def a_heal():
    call("POST", f"/api/brokers/{state['slow']}/network", {"latencyMs": 0, "jitterMs": 0, "lossPct": 0, "isolated": False, "splitFromBrokers": False})
    note("✚ Network restored", "✚ Сеть восстановлена",
         "Acknowledgement latency is back to single-digit milliseconds. The recording ends here and will start over. "
         "The Partitions, Charts and Messages tabs also work from this recording.",
         "Задержка подтверждений возвращается к единицам миллисекунд. Конец записи — дальше она начнётся сначала. "
         "Вкладки «Партиции», «Графики» и «Сообщения» тоже работают по этой записи.")


TIMELINE = [
    (1, a_start), (12, a_keys), (25, a_kill), (60, a_add_consumer), (82, a_start_broker),
    (112, a_preferred), (127, a_crash_consumer), (162, a_pause), (187, a_resume),
    (202, a_latency), (227, a_heal),
]
DURATION = 245


def run_timeline():
    for at, action in TIMELINE:
        while time.time() - started_at < at:
            time.sleep(0.2)
        try:
            action()
        except Exception as e:  # noqa: BLE001
            print(f"  ! {action.__name__}: {e}", file=sys.stderr)


def compact(snap):
    """Strip what the UI doesn't use from a snapshot (service event lists, recent deliveries)."""
    for s in snap.get("services", []):
        for i in s.get("instances", []):
            st = i.get("stats")
            if isinstance(st, dict):
                st.pop("events", None)
                st.pop("recent", None)
    return snap


def main():
    global started_at
    frames, pending_events = [], []
    print(f"Connecting to {BASE}/api/stream …")
    resp = urllib.request.urlopen(BASE + "/api/stream", timeout=30)
    event_type, data_lines = None, []
    started_at = None
    worker = None
    while True:
        raw = resp.readline()
        if not raw:
            break
        line = raw.decode("utf-8").rstrip("\n").rstrip("\r")
        if line.startswith("event:"):
            event_type = line[6:].strip()
        elif line.startswith("data:"):
            data_lines.append(line[5:].lstrip())
        elif line == "" and event_type:
            payload = json.loads("\n".join(data_lines))
            if event_type == "snapshot":
                latest["snap"] = payload
                if started_at is None:
                    started_at = time.time()
                    start_ts = payload["ts"]
                    worker = threading.Thread(target=run_timeline, daemon=True)
                    worker.start()
                frames.append({"t": round((payload["ts"] - start_ts) / 1000, 1), "snap": compact(payload), "events": pending_events})
                pending_events = []
                if time.time() - started_at >= DURATION:
                    break
            elif event_type == "events" and started_at is not None:
                pending_events.extend(payload)
            event_type, data_lines = None, []

    print("Reading the latest topic messages …")
    messages = {}
    for t in [t["name"] for t in latest["snap"]["topics"] if not t["internal"]]:
        messages[t] = call("GET", f"/api/topics/{t}/messages?limit=30")

    recording = {
        "meta": {
            "recordedAt": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "durationSec": frames[-1]["t"],
            "frames": len(frames),
            "note": "Real Kafka Lab snapshots (control-center /api/stream), recorded by tools/record-demo.py",
        },
        "annotations": annotations,
        "frames": frames,
        "messages": messages,
    }
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(recording, f, ensure_ascii=False, separators=(",", ":"))
    print(f"Done: {OUT} — {len(frames)} frames, {os.path.getsize(OUT) / 1024 / 1024:.1f} MB")


if __name__ == "__main__":
    main()
