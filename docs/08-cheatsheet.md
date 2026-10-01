# 8. Шпаргалка на одну страницу

## 15 фактов, которые надо знать наизусть

1. Kafka = распределённый реплицированный **append-only лог**. Чтение не удаляет сообщения.
2. Топик делится на **партиции**; порядок — **только внутри партиции**.
3. Партиция = `murmur2(key) % N`. Один ключ → одна партиция. Без ключа — sticky partitioner.
4. **Offset** — номер сообщения в партиции. Группа хранит committed offset в `__consumer_offsets`.
5. В группе одна партиция читается **одним** консюмером. Консюмеров > партиций → лишние простаивают.
6. Разные группы читают топик **независимо**, у каждой свои offset-ы.
7. У партиции один **лидер** (запись/чтение) и follower-ы, которые **сами тянут** данные (fetch).
8. **ISR** — не отстающие реплики. Лидером становится только реплика из ISR.
9. **HW**: консюмер видит только то, что есть на всех ISR.
10. `acks=all` + `min.insync.replicas=2` + RF=3 = подтверждённое не теряется и переживает потерю брокера.
11. **Идемпотентный producer** убирает дубли ретраев (PID + sequence), требует acks=all.
12. Коммит **после** обработки = at-least-once → обработка должна быть идемпотентной.
13. `session.timeout` ловит смерть процесса, `max.poll.interval` — зависшую обработку.
14. **KRaft**: метаданные — лог `__cluster_metadata` на кворуме контроллеров (Raft), нужно большинство. ZooKeeper удалён в 4.0.
15. Партиции можно только **добавить** — и это меняет маршрутизацию ключей.

## Дефолты, которые полезно помнить (Java-клиент, Kafka 4.x)

| Producer | | Consumer | | Broker / топик | |
|---|---|---|---|---|---|
| acks | all | session.timeout.ms | 45 c | default RF | 1 (ставь 3!) |
| enable.idempotence | true | heartbeat.interval.ms | 3 c | min.insync.replicas | 1 (ставь 2!) |
| linger.ms | 5 | max.poll.interval.ms | 5 мин | unclean.leader.election | false |
| batch.size | 16 KB | max.poll.records | 500 | retention | 7 дней |
| delivery.timeout.ms | 120 c | auto.offset.reset | latest | segment.bytes | 1 GB |
| request.timeout.ms | 30 c | enable.auto.commit | true (5 c) | replica.lag.time.max.ms | 30 c |
| max.in.flight | 5 | assignment | range, cooperative-sticky | broker.session.timeout.ms | 9 c |
| buffer.memory | 32 MB | isolation.level | read_uncommitted | message.max.bytes | ~1 MB |

**librdkafka (C#, Python) отличается:** `enable.idempotence=false`, `partitioner=consistent_random` (не murmur2!), `message.timeout.ms=300 c`, `batch.size=1 MB`, `enable.auto.offset.store=true`.

## Надёжная конфигурация «под ключ»

```text
Топик:     replication.factor=3, min.insync.replicas=2, unclean.leader.election.enable=false
Producer:  acks=all, enable.idempotence=true, delivery.timeout.ms с запасом, обработка ошибок в callback, Flush при остановке
           (C#: Partitioner = Murmur2Random, если ключи пишут и Java-сервисы)
Consumer:  коммит после обработки (C#: EnableAutoOffsetStore=false + StoreOffset), идемпотентная обработка,
           Close() при остановке, DLQ для «ядовитых» сообщений, алерт на lag
Кластер:   ≥3 брокера в разных зонах, 3 контроллера, мониторинг URP / ISR<min / offline / lag
```

## Что будет, если…

| …произошло | Короткий ответ |
|---|---|
| Упал брокер-лидер | Через ~session timeout выборы лидера из ISR, клиенты переключатся |
| Упал брокер при RF=3, min.isr=2 | Работаем, URP > 0 |
| Упало 2 брокера из 3 | acks=all не пишет (ISR=1), в combined-режиме нет кворума KRaft |
| Консюмер упал | Через session.timeout ребаланс, повтор необработанного с последнего коммита |
| Консюмер завис | Через max.poll.interval сам выйдет из группы, партиции «lost» |
| Консюмеров больше партиций | Лишние простаивают |
| Добавили партиции | Ребаланс групп; ключи частично начнут попадать в другие партиции |
| Консюмер отстал дольше retention | Часть данных удалена, offset out of range → auto.offset.reset |
| Producer ретраит без идемпотентности | Возможны дубли и нарушение порядка |
| Лидер изолирован, acks=1 | Подтверждённые им записи пропадут после возвращения (truncation) |

## Частые вопросы на собеседовании

- Как Kafka обеспечивает порядок? *(внутри партиции; ключи; идемпотентность при ретраях)*
- Как не потерять сообщение? *(acks=all, RF=3, min.isr=2, коммит после обработки, без unclean election)*
- Как избежать дублей? *(идемпотентный producer + идемпотентная обработка / транзакции)*
- Чем Kafka отличается от RabbitMQ? *(лог vs очередь — см. [сравнение](09-kafka-vs-rabbitmq.md))*
- Что такое ребаланс, почему он вреден, как с ним бороться? *(cooperative-sticky, static membership, KIP-848, Close())*
- Что такое ISR и HW? Почему консюмер не видит неподтверждённые записи?
- Как выбрать число партиций? Можно ли его уменьшить?
- Как работает exactly-once в Kafka и где его границы?
- Что такое lag и как его мониторить?
- Зачем compaction и что такое tombstone?
- Что изменилось с переходом на KRaft?

## Где это на стенде

| Тема | Где смотреть |
|---|---|
| Партиции, лидеры, ISR | Карточки брокеров, таблица «Партиции и offset-ы» |
| Ребаланс | Карточка order-processing: ⏏ 💥 🧊 🔄, «+ консюмер», стратегия |
| Lag | Карточки групп, графики, таблица партиций |
| acks / идемпотентность | Настройки order-service + «Аудит доставки» |
| Сбои | Кнопки ⏹ 💥 ⏸ 🌐 у брокеров, «🎓 Сценарии» |
| KRaft | ★ активный контроллер, «контроллер · lag» |
| Compaction | Топик customer-profiles, кнопка tombstone во вкладке «Сообщения» |
