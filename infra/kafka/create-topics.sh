#!/usr/bin/env bash
# Создаёт топики демо-стенда. Запускается один раз контейнером kafka-init.
# Обрати внимание: каждая команда — это обычный CLI Kafka, его можно повторить руками:
#   docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe
set -euo pipefail

BS="kafka-1:9092,kafka-2:9092,kafka-3:9092"
BIN=/opt/kafka/bin

echo "⏳ Ждём, пока все 3 брокера зарегистрируются в кластере..."
for i in $(seq 1 90); do
  count=$($BIN/kafka-broker-api-versions.sh --bootstrap-server "$BS" 2>/dev/null | grep -c '(id: ' || true)
  if [ "$count" -ge 3 ]; then
    echo "✅ Брокеров в кластере: $count"
    break
  fi
  echo "   брокеров пока: $count (попытка $i)"
  sleep 2
done

create() {
  local topic=$1 partitions=$2 rf=$3
  shift 3
  echo "📦 Топик $topic: partitions=$partitions, replication-factor=$rf $*"
  $BIN/kafka-topics.sh --bootstrap-server "$BS" --create --if-not-exists \
    --topic "$topic" --partitions "$partitions" --replication-factor "$rf" "$@"
}

# Заказы: ключ = customerId, 6 партиций, RF=3, min.insync.replicas=2 — «надёжная» конфигурация.
create orders 6 3 --config min.insync.replicas=2

# Платежи: результат обработки заказов (consume → transform → produce).
create payments 3 3 --config min.insync.replicas=2

# Dead Letter Queue: сюда order-processor складывает заказы, которые не удалось обработать.
create orders.dlq 1 3 --config min.insync.replicas=2

# Клики: много мелких событий, RF=2 и min.insync.replicas=1 — «быстрая, но менее надёжная» конфигурация.
# Короткий retention, чтобы нагрузочный тест не съел диск.
create clickstream 6 2 \
  --config min.insync.replicas=1 \
  --config retention.ms=600000 \
  --config retention.bytes=104857600 \
  --config segment.bytes=16777216

# Профили клиентов: compacted-топик — хранит только последнее значение для каждого ключа.
create customer-profiles 3 3 \
  --config cleanup.policy=compact \
  --config min.insync.replicas=2 \
  --config segment.ms=60000 \
  --config min.cleanable.dirty.ratio=0.01

echo
$BIN/kafka-topics.sh --bootstrap-server "$BS" --describe --exclude-internal
echo "🎉 Топики готовы"
