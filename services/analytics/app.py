"""
analytics (Python) — CONSUMER GROUP "analytics".

Читает сразу три топика: orders, payments, clickstream — и считает простую аналитику
(выручка, топ клиентов, популярные страницы, end-to-end задержка).

Главная идея для изучения: это ДРУГАЯ consumer group, чем order-processing (C#).
Каждая группа получает ВСЕ сообщения топика и хранит СВОИ offset-ы, поэтому
аналитика может отставать, стоять на паузе или перечитывать историю, не мешая обработке заказов.

Масштабирование: docker compose up -d --scale analytics=3 → партиции поделятся между экземплярами.
"""

import json
import os
import signal
import socket
import threading
import time
from collections import Counter, deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from confluent_kafka import Consumer, KafkaError, KafkaException

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092,localhost:19093,localhost:19094")
GROUP_ID = os.getenv("GROUP_ID", "analytics")
TOPICS = os.getenv("TOPICS", "orders,payments,clickstream").split(",")
PORT = int(os.getenv("PORT", "8080"))
INSTANCE = socket.gethostname()
CLIENT_ID = f"analytics-{INSTANCE[:12]}"
STARTED = time.time()


class Rate:
    """Счётчик + скорость за последние 3 полные секунды."""

    def __init__(self):
        self.total = 0
        self._buckets = [0] * 8
        self._sec = int(time.time())

    def _advance(self):
        now = int(time.time())
        if now == self._sec:
            return
        for i in range(1, min(now - self._sec, 8) + 1):
            self._buckets[(self._sec + i) % 8] = 0
        self._sec = now

    def add(self, n=1):
        self.total += n
        self._advance()
        self._buckets[self._sec % 8] += n

    @property
    def rate(self):
        self._advance()
        return round(sum(self._buckets[(self._sec - i) % 8] for i in range(1, 4)) / 3.0, 1)


class State:
    def __init__(self):
        self.lock = threading.Lock()
        self.config = {"paused": False, "processingDelayMs": 0}
        self.consumed = {t: Rate() for t in TOPICS}
        self.by_partition = Counter()
        self.assigned = set()
        self.revenue = Counter()
        self.orders = 0
        self.payments_amount = 0.0
        self.payments = 0
        self.top_customers = Counter()
        self.click_types = Counter()
        self.top_pages = Counter()
        self.e2e = {t: deque(maxlen=2000) for t in TOPICS}  # (when, latency_ms)
        self.bad_messages = 0
        self.rebalances = 0
        self.events = deque(maxlen=100)
        self._event_id = 0
        self._throttle = {}
        self.last_error = None

    def event(self, level, text, learn=None, throttle_key=None, throttle_s=5):
        now = time.time()
        with self.lock:
            if throttle_key:
                if now - self._throttle.get(throttle_key, 0) < throttle_s:
                    return
                self._throttle[throttle_key] = now
            self._event_id += 1
            self.events.append({"id": self._event_id, "ts": int(now * 1000), "level": level, "text": text, "learn": learn})
        print(f"[{level}] {text}", flush=True)


S = State()
running = True


def fmt(partitions):
    return ", ".join(f"{p.topic}-{p.partition}" for p in sorted(partitions, key=lambda p: (p.topic, p.partition)))


# ---------- колбэки ребаланса (вызываются внутри consume() в основном потоке) ----------
def on_assign(consumer, partitions):
    with S.lock:
        S.rebalances += 1
        for p in partitions:
            S.assigned.add((p.topic, p.partition))
    if partitions:
        S.event("info", f"{CLIENT_ID}: назначены [{fmt(partitions)}]", "rebalance")


def on_revoke(consumer, partitions):
    with S.lock:
        for p in partitions:
            S.assigned.discard((p.topic, p.partition))
    if partitions:
        S.event("info", f"{CLIENT_ID}: отозваны [{fmt(partitions)}]", "rebalance")


def on_lost(consumer, partitions):
    with S.lock:
        for p in partitions:
            S.assigned.discard((p.topic, p.partition))
    S.event("warn", f"{CLIENT_ID}: партиции потеряны [{fmt(partitions)}]", "max-poll-interval")


def process(msg):
    topic = msg.topic()
    ts_type, ts = msg.timestamp()
    now_ms = time.time() * 1000
    try:
        value = json.loads(msg.value()) if msg.value() else None
    except (ValueError, TypeError):
        with S.lock:
            S.bad_messages += 1
        S.event("warn", f"{CLIENT_ID}: не JSON в {topic}-{msg.partition()}@{msg.offset()} — пропускаем", "dlq", "bad-json", 5)
        value = None

    with S.lock:
        S.consumed[topic].add()
        S.by_partition[f"{topic}-{msg.partition()}"] += 1
        if ts > 0:
            S.e2e[topic].append((now_ms, now_ms - ts))
        if not isinstance(value, dict):
            return
        if topic == "orders":
            amount = float(value.get("amount", 0))
            S.orders += 1
            S.revenue[value.get("currency", "?")] += amount
            S.top_customers[value.get("customerId", "?")] += amount
        elif topic == "payments":
            S.payments += 1
            S.payments_amount += float(value.get("amount", 0))
        elif topic == "clickstream":
            S.click_types[value.get("event", "?")] += 1
            S.top_pages[value.get("page", "?")] += 1


def consume_loop():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "client.id": CLIENT_ID,
        "auto.offset.reset": "earliest",
        # Настройки «по умолчанию»: offset сохраняется в момент выдачи сообщения приложению (до обработки!)
        # и коммитится раз в 5 c. Сравни с C#-консюмером, где offset сохраняется ПОСЛЕ обработки (StoreOffset).
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 5000,
        "partition.assignment.strategy": "cooperative-sticky",
        "session.timeout.ms": 10000,
        "heartbeat.interval.ms": 2500,
        "max.poll.interval.ms": 30000,
        "fetch.wait.max.ms": 100,
        "error_cb": lambda err: S.event("warn", f"librdkafka: {err.str()}", None, f"err-{err.code()}", 8),
    })
    consumer.subscribe(TOPICS, on_assign=on_assign, on_revoke=on_revoke, on_lost=on_lost)
    S.event("info", f"{CLIENT_ID} подписался на {', '.join(TOPICS)} в группе «{GROUP_ID}»", "consumer-group")

    paused = False
    while running:
        want_pause = S.config["paused"]
        assignment = consumer.assignment()
        if want_pause and assignment:
            # pause() останавливает fetch, но консюмер остаётся в группе (heartbeat-ы идут, партиции за ним)
            consumer.pause(assignment)
            if not paused:
                S.event("warn", f"{CLIENT_ID}: pause() — чтение остановлено, партиции остаются за мной, lag растёт", "pause")
            paused = True
        elif not want_pause and paused:
            consumer.resume(assignment)
            paused = False
            S.event("info", f"{CLIENT_ID}: resume() — продолжаем с того же offset и догоняем lag", "pause")

        try:
            msgs = consumer.consume(num_messages=500, timeout=0.3)
        except KafkaException as e:
            S.last_error = str(e)
            continue

        delay_ms = S.config["processingDelayMs"]
        for msg in msgs:
            err = msg.error()
            if err:
                if err.code() != KafkaError._PARTITION_EOF:
                    S.last_error = err.str()
                continue
            process(msg)
            if delay_ms > 0:
                time.sleep(delay_ms / 1000.0)

    S.event("info", f"{CLIENT_ID}: close() — коммит offset-ов и выход из группы", "rebalance")
    consumer.close()


def percentile(values, q):
    if not values:
        return None
    values = sorted(values)
    return round(values[min(len(values) - 1, int(q * len(values)))], 1)


def stats():
    now_ms = time.time() * 1000
    with S.lock:
        e2e = {}
        for t, dq in S.e2e.items():
            recent = [lat for (at, lat) in dq if now_ms - at < 5000]
            e2e[t] = {"p50": percentile(recent, 0.5), "p99": percentile(recent, 0.99)}
        return {
            "service": "analytics",
            "lang": "Python",
            "role": "consumer",
            "instanceId": INSTANCE,
            "clientId": CLIENT_ID,
            "uptimeSec": int(time.time() - STARTED),
            "groupId": GROUP_ID,
            "topics": TOPICS,
            "config": dict(S.config),
            "assigned": [{"topic": t, "partition": p} for (t, p) in sorted(S.assigned)],
            "consumed": {t: r.total for t, r in S.consumed.items()},
            "rates": {t: r.rate for t, r in S.consumed.items()},
            "e2eLatencyMs": e2e,
            "rebalances": S.rebalances,
            "badMessages": S.bad_messages,
            "lastError": S.last_error,
            "metrics": {
                "orders": S.orders,
                "revenue": {k: round(v, 2) for k, v in S.revenue.items()},
                "payments": S.payments,
                "paymentsAmount": round(S.payments_amount, 2),
                "topCustomers": [[k, round(v, 2)] for k, v in S.top_customers.most_common(5)],
                "clickTypes": dict(S.click_types.most_common(6)),
                "topPages": S.top_pages.most_common(5),
            },
            "events": list(S.events)[-50:],
        }


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, body):
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path.startswith("/api/stats"):
            self._send(200, stats())
        elif self.path.startswith("/api/config"):
            self._send(200, S.config)
        elif self.path.startswith("/health"):
            self._send(200, {"ok": True})
        else:
            self._send(404, {"error": "not found"})

    def do_PUT(self):
        if not self.path.startswith("/api/config"):
            return self._send(404, {"error": "not found"})
        length = int(self.headers.get("Content-Length", 0))
        try:
            patch = json.loads(self.rfile.read(length) or b"{}")
        except ValueError:
            return self._send(400, {"error": "bad json"})
        with S.lock:
            if "paused" in patch and patch["paused"] is not None:
                S.config["paused"] = bool(patch["paused"])
            if "processingDelayMs" in patch and patch["processingDelayMs"] is not None:
                S.config["processingDelayMs"] = max(0, min(1000, float(patch["processingDelayMs"])))
            cfg = dict(S.config)
        S.event("info", f"{CLIENT_ID}: новая конфигурация {cfg}")
        self._send(200, {"settings": cfg})

    do_POST = do_PUT

    def log_message(self, *args):
        pass


def main():
    def stop(*_):
        global running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    print(f"analytics: HTTP на :{PORT}, bootstrap={BOOTSTRAP}", flush=True)

    while running:
        try:
            consume_loop()
        except Exception as e:  # noqa: BLE001 — переподключаемся при любой ошибке
            S.event("error", f"{CLIENT_ID}: {e}")
            time.sleep(3)
    server.shutdown()


if __name__ == "__main__":
    main()
