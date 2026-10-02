# 2. Producer: как запись попадает в Kafka

## Устройство producer-а

```text
 приложение ──Produce(key, value)──► Serializer ─► Partitioner ─► RecordAccumulator (буфер)
                                                                   ├─ batch для orders-0
                                                                   ├─ batch для orders-1  ──► Sender (фоновый поток)
                                                                   └─ batch для orders-2         │ ProduceRequest лидеру
                                                                                                  ▼
             delivery report / callback ◄──────────── ответ брокера (offset или ошибка) ◄── Broker (лидер партиции)
```

- **Produce() асинхронный.** Он кладёт сообщение в буфер и сразу возвращает управление. Результат (offset или ошибка) приходит позже в callback / delivery handler.
- Сообщения группируются в **batch по партициям**. Batch отправляется, когда наполнился (`batch.size`) или истекло `linger.ms`.
- Отправкой и ретраями занимается **фоновый поток** клиента. В librdkafka (C#, Python, Go-обёртка) — свои потоки на каждый брокер.

## Выбор партиции

| Ситуация | Что происходит |
|---|---|
| Указана партиция явно | Пишем туда |
| Есть ключ | `murmur2(key) & 0x7fffffff % N` — один ключ → одна партиция |
| Ключа нет | Sticky partitioner (KIP-480): batch целиком в одну партицию, потом в следующую |

> ⚠ **Подводный камень .NET/Python:** librdkafka по умолчанию использует `consistent_random` (CRC32), а Java — murmur2. Один ключ из C# и из Java уйдёт в **разные** партиции. Лечится `Partitioner = Partitioner.Murmur2Random` (так сделано в order-service).

Порядок гарантирован **только внутри партиции**. Нужен порядок событий одного клиента — делай `customerId` ключом. Неравномерное распределение ключей = **горячая партиция** (сценарий «Ключ → партиция» с горячим ключом).

## acks — сколько подтверждений ждать

| acks | Кто должен записать | Скорость | Риск |
|---|---|---|---|
| `0` | Никто, ответ не ждём | Максимум | Потери не видны вообще |
| `1` | Только лидер | Высокая | Потеря при смене лидера до репликации |
| `all` (`-1`) | Все реплики ISR, и ISR ≥ `min.insync.replicas` | Ниже | Подтверждённое не теряется (при RF≥3, min.isr≥2) |

`min.insync.replicas` — настройка **топика/брокера**, а не producer-а. Если ISR меньше — запись с `acks=all` получает `NOT_ENOUGH_REPLICAS`. Kafka предпочитает отказать, чем потерять.

## Ретраи, идемпотентность и порядок

Сетевой запрос может «потеряться» в двух местах: запрос не дошёл (повтор безопасен) или **дошёл, а ответ потерялся** (повтор = дубль). Producer не может различить эти случаи.

- **Без идемпотентности** ретраи дают дубли, а при `max.in.flight > 1` ещё и меняют порядок (batch 2 запишется раньше повторного batch 1).
- **`enable.idempotence=true`**: брокер выдаёт producer-у **ProducerId (PID)** и эпоху, каждый batch получает **sequence number**. Повтор с тем же номером брокер распознаёт и не пишет второй раз; пропуск номера — ошибка `OutOfOrderSequence`. Порядок сохраняется при `max.in.flight ≤ 5`.
- Требования: `acks=all`, `retries > 0`, `max.in.flight ≤ 5`.
- Идемпотентность действует в рамках **одной сессии producer-а** и одной партиции. Перезапуск процесса = новый PID. Для гарантий через перезапуски и между топиками — транзакции.
- При массовых таймаутах librdkafka «бампает» эпоху producer-а (KIP-360), чтобы восстановить порядок; редкий повтор после этого брокер уже не распознает. На стенде (сценарий «Сетевая задержка и дубли») дублей с идемпотентностью в разы меньше, но не ноль.
- Иногда идемпотентный producer получает **фатальную** ошибку (не может гарантировать порядок) — такой экземпляр нужно пересоздать. Обрабатывай `IsFatal` в error handler (так делает order-service).

## Таймауты

| Java | librdkafka (C#/Python) | Смысл |
|---|---|---|
| `linger.ms` (5) | `linger.ms` / `queue.buffering.max.ms` (5) | Сколько ждать, наполняя batch |
| `batch.size` (16 KB) | `batch.size` (1 MB), `batch.num.messages` (10 000) | Максимальный размер batch |
| `request.timeout.ms` (30 c) | `socket.timeout.ms` (60 c) на клиенте; `request.timeout.ms` — ожидание брокером реплик | Сколько ждать ответа на запрос |
| `delivery.timeout.ms` (120 c) | `message.timeout.ms` / `delivery.timeout.ms` (300 c) | Общее время на доставку, включая ретраи |
| `buffer.memory` (32 MB) | `queue.buffering.max.kbytes` (1 GB), `.max.messages` (100 000) | Размер локального буфера |
| `max.block.ms` (60 c) | — (сразу `Local_QueueFull`) | Сколько блокироваться, если буфер полон |
| `max.in.flight.requests.per.connection` (5) | `max.in.flight` (1 000 000!) | Сколько запросов без ответа на соединение |
| `enable.idempotence` (true) | `enable.idempotence` (**false**) | Дедупликация ретраев |

> 💡 Обрати внимание на различия дефолтов: в librdkafka идемпотентность **выключена** и `max.in.flight` огромный. Для надёжной записи из C# включай `EnableIdempotence = true` явно.

## Batching и сжатие

- Сжатие (`compression.type`: gzip, snappy, lz4, zstd) применяется к **batch-у целиком** — чем больше batch, тем лучше степень сжатия.
- Брокер обычно хранит batch в том же сжатом виде, а консюмер распаковывает сам — экономится сеть и диск.
- Компромисс: `linger.ms` ↑ → batch-и крупнее → throughput ↑, latency ↑. Попробуй на clickstream-generator: linger 0 vs 50 мс, none vs zstd.

## Backpressure

Если брокеры недоступны или не успевают, сообщения копятся в буфере. Когда он полон:

- Java: `send()` блокируется до `max.block.ms`, затем исключение.
- librdkafka: `Produce()` сразу бросает `Local_QueueFull` — приложение должно притормозить (так делает генератор order-service).
- franz-go: `Produce()` блокируется, `TryProduce()` возвращает `ErrMaxBuffered`.

## Пример на C# (Confluent.Kafka)

```csharp
var config = new ProducerConfig
{
    BootstrapServers = "localhost:19092,localhost:19093,localhost:19094",
    Acks = Acks.All,
    EnableIdempotence = true,                 // дедупликация ретраев
    LingerMs = 5,
    CompressionType = CompressionType.Lz4,
    MessageTimeoutMs = 30_000,                // delivery.timeout.ms
    Partitioner = Partitioner.Murmur2Random,  // как у Java-клиента
};

using var producer = new ProducerBuilder<string, string>(config).Build();

// 1) Асинхронно с callback — максимальная пропускная способность
producer.Produce("orders", new Message<string, string> { Key = "customer-007", Value = json }, report =>
{
    if (report.Error.IsError)
        Console.WriteLine($"Не доставлено: {report.Error.Reason}, статус: {report.Status}"); // PossiblyPersisted?
    else
        Console.WriteLine($"OK: {report.TopicPartitionOffset}");
});

// 2) С ожиданием — удобно, но медленно, если ждать каждое сообщение
var result = await producer.ProduceAsync("orders", new Message<string, string> { Key = "customer-007", Value = json });

producer.Flush(TimeSpan.FromSeconds(10)); // перед выходом: дождаться отправки буфера
```

`DeliveryReport.Status`:
- `Persisted` — брокер подтвердил;
- `NotPersisted` — точно не записано (можно повторить);
- `PossiblyPersisted` — таймаут: могло записаться (повтор может дать дубль).

## Ошибки: какие ретраить

| Ошибка | Ретраится клиентом | Что значит |
|---|---|---|
| `NOT_LEADER_OR_FOLLOWER`, `LEADER_NOT_AVAILABLE` | Да (+ обновление метаданных) | Идут выборы лидера |
| `NOT_ENOUGH_REPLICAS(_AFTER_APPEND)` | Да | ISR < min.insync.replicas |
| `REQUEST_TIMED_OUT`, сетевые ошибки | Да | Брокер не ответил вовремя |
| `MESSAGE_TOO_LARGE` | Нет | Больше `max.request.size` / `message.max.bytes` |
| `TOPIC_AUTHORIZATION_FAILED`, `UNKNOWN_TOPIC` (при выключенном автосоздании) | Нет | Права/конфигурация |
| `Local_MsgTimedOut` | — | Истёк delivery.timeout: ретраи кончились |

## Готовые профили

**Надёжный** (деньги, заказы): `acks=all`, `enable.idempotence=true`, топик RF=3 + `min.insync.replicas=2`, `delivery.timeout.ms` с запасом, обработка ошибок в callback.

**Быстрый** (метрики, клики, логи): `acks=1` (или 0), `linger.ms=20…100`, `compression=lz4/zstd`, крупный `batch.size`. Потеря части событий допустима.

## Самопроверка

1. Почему `Produce()` возвращается раньше, чем брокер записал сообщение? Как узнать результат?
2. Чем опасен `acks=1`? Когда он оправдан?
3. Что гарантирует идемпотентный producer и чего он НЕ гарантирует?
4. Почему из C# по умолчанию ключ может попасть в другую партицию, чем из Java?
5. Как linger.ms влияет на задержку и пропускную способность?

Дальше: [Consumer и группы](03-consumer.md)
