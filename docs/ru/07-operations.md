# 7. Эксплуатация: метрики, диагностика, CLI

## Что мониторить в первую очередь

| Метрика | Норма | Почему важно |
|---|---|---|
| Under-replicated partitions | 0 | Запас прочности снижен; брокер умер/отстаёт |
| Under-min-ISR partitions | 0 | Запись с acks=all отклоняется |
| Offline partitions | 0 | Данные недоступны |
| Active controller count (сумма по кластеру) | ровно 1 | 0 — нет кворума, >1 — проблема |
| ISR shrink/expand rate | ≈ 0 | «Мигающие» реплики: сеть, GC, диск |
| Consumer lag (в сообщениях и во времени) | стабилен | Консюмеры не успевают |
| Produce/Fetch request latency (p99) | стабильна | Перегрузка брокеров, диска, сети |
| Request handler / network idle % | > 30% | Брокер упёрся в потоки обработки |
| Диск: свободное место, IO wait | — | Заполненный диск = упавший брокер |
| JVM GC паузы | короткие | Длинные паузы → фенсинг, выборы лидеров |

Инструменты: JMX-экспортёр + Prometheus/Grafana, Burrow/kminion (lag), kafka-ui/AKHQ/Conduktor (просмотр). На стенде всё это заменяет control-center: он собирает метрики через AdminClient.

## Симптом → причина → что делать

| Симптом | Вероятные причины | Действия |
|---|---|---|
| Lag растёт | Медленная обработка, мало консюмеров/партиций, частые ребалансы, горячая партиция | Профилировать обработку, масштабировать до числа партиций, проверить распределение ключей |
| Постоянные ребалансы | Обработка дольше max.poll.interval, GC/сеть > session.timeout, нестабильные поды | Уменьшить пачку/ускорить обработку, увеличить max.poll.interval, static membership, cooperative/KIP-848 |
| `NOT_ENOUGH_REPLICAS` у producer-а | ISR < min.insync.replicas: брокер упал или отстаёт | Вернуть брокер, проверить URP и сеть/диск отстающей реплики |
| `Local_MsgTimedOut` / `TimeoutException` | Лидер недоступен дольше delivery.timeout, брокер перегружен | Проверить кластер, увеличить таймаут, следить за backpressure |
| Дубли у консюмеров | Крэши/ребалансы до коммита | Идемпотентная обработка, чаще коммитить, Close() при остановке |
| Дубли в топике | Ретраи без идемпотентности, повторная отправка приложением | `enable.idempotence=true`, транзакции |
| `OFFSET_OUT_OF_RANGE`, «потерянные» сообщения | Консюмер отстал дольше retention | Увеличить retention, алерт на lag во времени |
| Один брокер перегружен | Лидеры не на preferred-репликах, неравномерное размещение | Preferred leader election, reassign partitions |
| `MESSAGE_TOO_LARGE` | Сообщение больше лимитов | Согласовать max.request.size / message.max.bytes / fetch, либо claim check |
| URP «мигает» | Сеть, GC-паузы, медленный диск у follower-а | Смотреть GC, диск, сеть; replica.lag.time.max.ms |

## CLI — шпаргалка

Все команды выполняются внутри брокера стенда:

```bash
docker exec -it -e KAFKA_HEAP_OPTS=-Xmx256m kafka-1 bash
cd /opt/kafka/bin
```

> 💡 Без `-e KAFKA_HEAP_OPTS=…` каждая утилита унаследует heap брокера (`-Xms512m`) и будет стартовать заметно дольше. Помни: консольный consumer без `--max-messages` не завершится сам, пока в топик пишут, — останавливай его Ctrl+C.

### Топики

```bash
./kafka-topics.sh --bootstrap-server localhost:9092 --list
./kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders
./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic demo --partitions 3 --replication-factor 3 --config min.insync.replicas=2
./kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic demo --partitions 6          # только увеличить!
./kafka-topics.sh --bootstrap-server localhost:9092 --describe --under-replicated-partitions
./kafka-topics.sh --bootstrap-server localhost:9092 --describe --under-min-isr-partitions
./kafka-topics.sh --bootstrap-server localhost:9092 --describe --unavailable-partitions
```

### Писать и читать из консоли

```bash
# producer с ключами: строка "customer-1:{...}"
./kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo \
  --property parse.key=true --property key.separator=:

# consumer с начала, с ключами, партициями, offset-ами и заголовками
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders --from-beginning \
  --property print.key=true --property print.partition=true --property print.offset=true \
  --property print.headers=true --max-messages 10

# прочитать конкретную партицию с конкретного offset
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders --partition 3 --offset 100 --max-messages 5

# в группе (появится в kafka-consumer-groups)
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders --group cli-test
```

### Consumer groups

```bash
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group order-processing             # lag по партициям
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group order-processing --members --verbose
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group order-processing --state

# перемотка (группа должна быть неактивна; без --execute — только показать план)
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group analytics --reset-offsets --to-earliest --all-topics --execute
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group analytics --reset-offsets --shift-by -1000 --topic orders --execute
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group analytics --reset-offsets --to-datetime 2026-01-01T00:00:00.000 --all-topics --execute
```

### Конфиги, offset-ы, лидеры

```bash
./kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name orders --describe
./kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name orders --alter --add-config retention.ms=3600000
./kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-name 1 --describe --all

./kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic orders                 # конец лога по партициям
./kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic orders --time -2       # начало лога

./kafka-leader-election.sh --bootstrap-server localhost:9092 --election-type PREFERRED --all-topic-partitions
```

### KRaft и диагностика

```bash
./kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status        # кто лидер кворума, epoch, HW
./kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --replication   # отставание контроллеров
./kafka-broker-api-versions.sh --bootstrap-server localhost:9092 | grep "id:"         # живые брокеры
./kafka-log-dirs.sh --bootstrap-server localhost:9092 --describe --topic-list orders   # размеры реплик на дисках
./kafka-dump-log.sh --files /var/lib/kafka/data/orders-0/00000000000000000000.log --print-data-log | head
```

### Нагрузочное тестирование

```bash
./kafka-producer-perf-test.sh --topic demo --num-records 1000000 --record-size 200 --throughput -1 \
  --producer-props bootstrap.servers=localhost:9092 acks=all linger.ms=20 compression.type=lz4
./kafka-consumer-perf-test.sh --bootstrap-server localhost:9092 --topic demo --messages 1000000
```

### С хоста

Брокеры стенда доступны и с хоста: `localhost:19092,localhost:19093,localhost:19094` — можно подключать свои программы, IDE или kcat:

```bash
kcat -b localhost:19092 -L                       # метаданные
kcat -b localhost:19092 -t orders -C -o -5 -e    # последние 5 сообщений каждой партиции
```

## Правила выбора параметров

- **Партиции**: ≥ максимального числа консюмеров в группе; ≈ целевой throughput / throughput одной партиции; с запасом (уменьшить нельзя). Очень много партиций = больше файлов, памяти, дольше восстановление.
- **RF=3, min.insync.replicas=2** для важных данных. RF=2 — только для неважного.
- **Retention**: дольше максимального возможного простоя консюмеров + запас на перечитывание.
- **Контроллеры**: 3 (или 5 для больших кластеров), на отдельных узлах в продакшене.
- **Ключи**: стабильные, с равномерным распределением; если есть «китовые» ключи — подумать о составных.

## Самопроверка

1. Какие три метрики ты добавишь на дашборд первыми?
2. Как посмотреть lag группы из консоли? Как перемотать её на час назад?
3. Что делать, если один брокер ведёт почти всех лидеров?
4. Почему консюмеры могут «терять» сообщения, хотя Kafka их не теряла?

Дальше: [Шпаргалка на одну страницу](08-cheatsheet.md)
