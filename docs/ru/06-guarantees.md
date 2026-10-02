# 6. Гарантии доставки и exactly-once

## Три семантики

| Семантика | Потери | Дубли | Как получить |
|---|---|---|---|
| **At-most-once** | возможны | нет | `acks=0/1`, коммит offset-а до обработки |
| **At-least-once** | нет | возможны | `acks=all` + ретраи, коммит после обработки |
| **Exactly-once** | нет | нет (по эффекту) | идемпотентный producer + транзакции + `read_committed`, либо at-least-once + идемпотентная обработка |

Сквозная гарантия = самое слабое звено цепочки **producer → брокер → consumer → побочные эффекты**.

## Где теряются сообщения

| Где | Причина | Защита |
|---|---|---|
| Producer | `acks=0/1`, delivery.timeout истёк, процесс упал с непустым буфером | `acks=all`, обработка ошибок в callback, `Flush()` при остановке |
| Брокер | Лидер подтвердил (acks=1) и умер до репликации; unclean election | `acks=all`, RF=3, `min.insync.replicas=2`, `unclean.leader.election.enable=false` |
| Retention | Консюмер отстал дольше retention | Мониторинг lag во времени, запас retention |
| Consumer | Закоммитил до обработки (или librdkafka-автостор при асинхронной обработке) | Коммит/StoreOffset только после обработки |

## Где появляются дубли

| Где | Причина | Защита |
|---|---|---|
| Producer | Ретрай после потерянного ответа | `enable.idempotence=true` |
| Producer-приложение | Повторная отправка после `PossiblyPersisted`/рестарта | Транзакции или идемпотентный консюмер |
| Consumer | Упал после обработки, но до коммита; ребаланс | Идемпотентная обработка, транзакции |

## Идемпотентный producer

- Брокер выдаёт **ProducerId** и **epoch**; каждый batch в партицию получает возрастающий **sequence**.
- Брокер помнит последние 5 batch-ей на (PID, партиция): повтор → `DUPLICATE_SEQUENCE` (успех без записи), пропуск → `OUT_OF_ORDER_SEQUENCE`.
- Защищает от дублей **ретраев одной сессии**. Не защищает от того, что приложение само отправит сообщение дважды, и от перезапуска producer-а (новый PID).

## Транзакции Kafka

Атомарная запись в несколько партиций/топиков **плюс** коммит offset-ов консюмера — «всё или ничего».

```csharp
var producer = new ProducerBuilder<string, string>(new ProducerConfig
{
    BootstrapServers = "...",
    TransactionalId = "order-processor-1",   // стабильный id экземпляра: по нему фенсятся «зомби»
    EnableIdempotence = true,
}).Build();
producer.InitTransactions(TimeSpan.FromSeconds(30));

var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
{
    BootstrapServers = "...",
    GroupId = "order-processing",
    EnableAutoCommit = false,
    IsolationLevel = IsolationLevel.ReadCommitted,   // не видеть незакоммиченное
}).Build();

while (true)
{
    var cr = consumer.Consume(ct);
    producer.BeginTransaction();
    producer.Produce("payments", new Message<string, string> { Key = cr.Message.Key, Value = Transform(cr.Message.Value) });
    // offset входного сообщения коммитится В ТОЙ ЖЕ транзакции
    producer.SendOffsetsToTransaction(
        new[] { new TopicPartitionOffset(cr.TopicPartition, cr.Offset + 1) },
        consumer.ConsumerGroupMetadata, TimeSpan.FromSeconds(10));
    producer.CommitTransaction();   // при ошибке — AbortTransaction() и повтор
}
```

Как это работает:
- **Transaction coordinator** (лидер партиции `__transaction_state`) ведёт состояние транзакции.
- В партиции пишутся данные, затем **маркеры** COMMIT/ABORT.
- Консюмер с `read_committed` читает только до **LSO** (last stable offset) и пропускает отменённое.
- Новый экземпляр с тем же `transactional.id` увеличивает epoch и **фенсит** старый («зомби»), не давая ему закоммитить.

Ограничение: транзакции покрывают только **Kafka → Kafka** (Kafka Streams с `processing.guarantee=exactly_once_v2` использует именно их).

## Exactly-once с внешним миром

Kafka не может атомарно закоммитить offset и запись в PostgreSQL. Рабочие паттерны:

1. **Идемпотентный консюмер** (самый частый): at-least-once + обработка, которую безопасно повторить.
   - `INSERT … ON CONFLICT (order_id) DO NOTHING/UPDATE`;
   - таблица обработанных сообщений (**inbox**) в той же транзакции БД, что и бизнес-изменение;
   - естественные ключи и версии вместо инкрементов «+1».
2. **Offset в БД**: хранить offset в той же транзакции, что и данные; при старте делать `seek()` на сохранённое значение.
3. **Transactional outbox** (для записи «БД + событие»): изменение и событие пишутся в одну транзакцию БД (таблица outbox), отдельный процесс или Debezium CDC публикует outbox в Kafka. Решает проблему dual write.

## Порядок сообщений

- Гарантирован внутри партиции при условии идемпотентного producer-а (или `max.in.flight=1`).
- Ретраи на консюмере (отправка в retry-топик) **ломают порядок** по ключу — это осознанный компромисс.
- Если важен порядок по сущности, а сообщение не обрабатывается — иногда правильнее остановить партицию (и алертить), чем пропустить.

## На стенде

- «🔍 Аудит доставки» в карточке order-service сверяет каждое подтверждение брокера с тем, что реально лежит в логе: видно **потери** (сценарий «Зомби-лидер») и **дубли в логе** (сценарий «Сетевая задержка и дубли»).
- Счётчик «дубли» в группе order-processing — повторная обработка после крэша консюмера (at-least-once).

## Самопроверка

1. Какие три настройки (две на топике, одна у producer-а) нужны, чтобы подтверждённая запись не терялась?
2. Почему идемпотентный producer не даёт exactly-once для всего пайплайна?
3. Что такое LSO и кто его видит?
4. Как сделать обработку заказа идемпотентной при записи в БД?
5. Зачем нужен transactional outbox, если есть транзакции Kafka?

Дальше: [Эксплуатация и CLI](07-operations.md)
