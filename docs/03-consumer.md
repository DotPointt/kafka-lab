# 3. Consumer и consumer groups

## Цикл консюмера

```text
 subscribe("orders")
 loop:
    records = poll()/Consume()      ← fetch у лидеров назначенных партиций + heartbeat-логика + колбэки ребаланса
    обработать(records)
    commit(offset последнего обработанного + 1)   ← «группа дочитала партицию до сюда»
```

У консюмера две позиции в каждой партиции:
- **position** — какой offset он запросит следующим (живёт в памяти процесса);
- **committed offset** — сохранённая в `__consumer_offsets` позиция группы. С неё начнёт тот, кто получит партицию после ребаланса или рестарта.

Разница между ними — окно, в котором возможны **повторы** (если упасть до коммита) или **потери** (если закоммитить до обработки).

## Семантика определяется порядком «обработка ↔ коммит»

| Порядок | Семантика | Последствия сбоя |
|---|---|---|
| коммит → обработка | at-most-once | Упали после коммита — сообщение потеряно |
| обработка → коммит | **at-least-once** | Упали до коммита — сообщение обработается ещё раз |
| обработка и коммит атомарно | exactly-once | Транзакции Kafka (Kafka→Kafka) или offset в той же транзакции БД |

### Автокоммит — что он на самом деле делает

`enable.auto.commit=true` коммитит раз в `auto.commit.interval.ms` (5 с) позиции, «выданные» приложению. В Java — при следующем `poll()`, поэтому обработка в цикле poll даёт at-least-once. В **librdkafka** по умолчанию offset сохраняется **в момент выдачи** сообщения (`enable.auto.offset.store=true`) — если обработка асинхронная, возможна потеря.

### Рекомендуемый паттерн librdkafka (C#)

```csharp
var config = new ConsumerConfig
{
    BootstrapServers = "localhost:19092",
    GroupId = "order-processing",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = true,          // фоновый коммит раз в AutoCommitIntervalMs...
    EnableAutoOffsetStore = false,    // ...но только того, что мы явно отметили
    PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
};

using var consumer = new ConsumerBuilder<string, string>(config)
    .SetPartitionsAssignedHandler((c, parts) => Console.WriteLine($"Получил: {string.Join(",", parts)}"))
    .SetPartitionsRevokedHandler((c, parts) => Console.WriteLine($"Отдаю: {string.Join(",", parts)}"))
    .Build();

consumer.Subscribe("orders");
while (!ct.IsCancellationRequested)
{
    var cr = consumer.Consume(ct);
    Handle(cr.Message);            // обработка (идемпотентная!)
    consumer.StoreOffset(cr);      // «обработано» → уйдёт в следующий автокоммит
}
consumer.Close();                  // коммит + LeaveGroup → быстрый ребаланс
```

Так устроен **order-processor** на стенде. `Close()` важен: без него группа узнает об уходе консюмера только через `session.timeout.ms` (кнопка 💥 «крэш» показывает именно это).

## Consumer group

- Участники с одинаковым `group.id` делят партиции подписанных топиков: **партиция → максимум один консюмер группы**.
- Консюмеров больше, чем партиций? Лишние простаивают. **Параллелизм ограничен числом партиций.**
- Разные группы полностью независимы: свои offset-ы, свой темп, своя история.
- **Координатор** группы — брокер-лидер партиции `__consumer_offsets`, в которую попадает `hash(group.id)`. Он принимает heartbeat-ы, коммиты и проводит ребаланс.

## Ребаланс

Триггеры: участник вошёл; вышел (`Close`); умер (нет heartbeat дольше `session.timeout.ms`); завис (не вызывал poll дольше `max.poll.interval.ms`); изменилась подписка или число партиций.

### Классический протокол (group.protocol=classic)

1. Все участники шлют **JoinGroup** (состояние группы `PreparingRebalance`).
2. Координатор выбирает лидера группы и отдаёт ему список участников; лидер **на клиенте** считает назначение по стратегии.
3. **SyncGroup** — назначение раздаётся всем (`CompletingRebalance` → `Stable`).

| Стратегия | Тип | Как делит | Особенность |
|---|---|---|---|
| `range` | eager | Подряд идущие куски по каждому топику | Неравномерно при нескольких топиках |
| `roundrobin` | eager | По кругу по всем партициям | Равномерно, но всё перемешивает |
| `sticky` | eager | Равномерно и с минимумом перемещений | Всё равно stop-the-world |
| `cooperative-sticky` | cooperative | Как sticky | Отзываются только переезжающие партиции, остальные читаются дальше |

**Eager**: каждый участник отдаёт ВСЕ партиции перед ребалансом — на время ребаланса группа не читает ничего. **Cooperative** (KIP-429): ребаланс в несколько раундов, двигается минимум.

### Новый протокол KIP-848 (group.protocol=consumer, Kafka 4.0+)

- Назначение считает **брокер** (assignor `uniform` или `range`), клиенты получают изменения через обычные heartbeat-ы.
- Нет глобального барьера JoinGroup/SyncGroup: каждый участник сходится к целевому назначению сам — ребалансы быстрее и не останавливают группу.
- Таймауты задаёт брокер: `group.consumer.session.timeout.ms`, `group.consumer.heartbeat.interval.ms`. Клиентские `session.timeout.ms` и `partition.assignment.strategy` с этим протоколом не используются.

### Static membership

`group.instance.id` = постоянное имя экземпляра. При перезапуске в пределах `session.timeout.ms` координатор возвращает ему те же партиции **без ребаланса**. Сценарий «Крэш консюмера» → кнопка 🔄 со static membership.

## Таймауты консюмера

| Настройка | Java по умолчанию | Смысл |
|---|---|---|
| `session.timeout.ms` | 45 c | Нет heartbeat дольше → участник мёртв (ловит смерть процесса и сети) |
| `heartbeat.interval.ms` | 3 c | Как часто фоновый поток шлёт heartbeat; ≈ 1/3 session.timeout |
| `max.poll.interval.ms` | 5 мин | Не вызывал poll дольше → консюмер сам выходит из группы (ловит зависшую обработку) |
| `max.poll.records` | 500 | Сколько записей отдаёт один poll (Java) |
| `auto.offset.reset` | latest | Откуда начинать без коммита: earliest / latest / none |
| `auto.commit.interval.ms` | 5 c | Период автокоммита |
| `isolation.level` | read_uncommitted | read_committed — не показывать данные незакоммиченных транзакций |
| `fetch.min.bytes` / `fetch.max.wait.ms` | 1 B / 500 мс | Сколько ждать накопления данных на брокере (batching на чтении) |

> Две «смерти» консюмера: **session.timeout** — процесс или сеть умерли; **max.poll.interval** — процесс жив, но обработка зависла. На стенде: 💥 (крэш) и 🧊 (зависание).

## Lag

**Lag = high watermark − committed offset**, по каждой партиции. Это главная метрика консюмеров.

- Растёт, когда обработка медленнее записи.
- Удобно переводить во время: `lag / скорость обработки` ≈ сколько секунд отставания.
- Lag по закоммиченным offset-ам «пилит» с периодом автокоммита — это нормально.
- Лечение: ускорить обработку, батчить работу с БД, добавить консюмеров (≤ числа партиций), увеличить число партиций, вынести тяжёлую работу асинхронно.

## Полезные приёмы

- **pause()/resume()** — остановить чтение партиций, оставаясь в группе (backpressure от медленного downstream).
- **seek()** — перейти к конкретному offset (например, по времени через `offsetsForTimes`).
- **Перечитать историю**: остановить группу → `kafka-consumer-groups.sh --reset-offsets --to-earliest --execute` → запустить.
- **Параллельная обработка внутри партиции** ломает порядок. Если нужен порядок по ключу — параллелить по ключам (как Confluent Parallel Consumer), а не по сообщениям.

## Poison pill, ретраи и DLQ

Сообщение, которое нельзя обработать, нельзя ретраить бесконечно: offset не двигается → партиция встаёт.

```text
 orders ─► обработка ── ok ──► payments
             │ ошибка
             ├─ ретрай N раз (иногда через topics orders.retry-5s, orders.retry-1m)
             └─ не вышло ──► orders.dlq  (+ заголовки: причина, исходные topic/partition/offset)
```

В Kafka нет встроенной DLQ (в отличие от RabbitMQ) — это паттерн приложения (Spring Kafka, Kafka Connect, MassTransit умеют из коробки).

## Самопроверка

1. Чем committed offset отличается от position?
2. Почему at-least-once — самый распространённый выбор и что он требует от обработки?
3. Что произойдёт с группой из 8 консюмеров на топике с 6 партициями?
4. Чем cooperative-sticky лучше range при частых деплоях?
5. Консюмер жив, heartbeat-ы идут, а его выкинули из группы. Почему?
6. Как перечитать топик заново для существующей группы?

Дальше: [Репликация и KRaft](04-replication.md)
