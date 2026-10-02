# 10. Как устроен этот стенд

## Архитектура

```text
                ┌───────────────────────────── docker network kafka-lab-net ─────────────────────────────┐
                │                                                                                        │
 order-service  │──orders, customer-profiles──►┐                     ┌──► order-processor (C#)  ── payments, orders.dlq ──┐
   (C#, :8081)  │                              │   kafka-1 ┐         │     group "order-processing"                      │
                │                              ├──►kafka-2 ├ KRaft ──┤                                                   │
 clickstream-   │──clickstream────────────────►┘   kafka-3 ┘ кворум  └──► analytics (Python)                            │
 generator      │                                  (брокер +            group "analytics": orders, payments, clickstream │
 (Go, :8084)    │                                   контроллер)    ◄──────────────────────────────────────────────────┘
                │                                      ▲
                │   control-center (C#, :8080) ────────┤ AdminClient: метаданные, offset-ы, группы
                │      UI, SSE-стрим, сценарии          │ Docker API (docker.sock): stop / kill / pause, exec tc netem
                │                                      └─ HTTP: /api/stats и /api/config всех сервисов
                └────────────────────────────────────────────────────────────────────────────────────────┘
```

## Сервисы

| Сервис | Язык / клиент | Роль | Что посмотреть в коде |
|---|---|---|---|
| `order-service` | C#, Confluent.Kafka | Producer заказов (ключ = customerId) и профилей (compacted) | `OrderProducer.cs` — настройки producer, delivery report, статистика librdkafka; `DeliveryVerifier.cs` — сверка ack ↔ лог |
| `order-processor` | C#, Confluent.Kafka | Consumer group `order-processing`, N консюмеров в одном процессе, consume → transform → produce, ретраи, DLQ | `ConsumerWorker.cs` — цикл Consume/StoreOffset, колбэки ребаланса, крэш через Dispose без Close; `ConsumerManager.cs` |
| `analytics` | Python, confluent-kafka | Consumer group `analytics`, pause/resume, масштабируется `--scale` | `app.py` — колбэки on_assign/on_revoke, pause() |
| `clickstream-generator` | Go, franz-go | Высоконагруженный producer, batching/compression/acks | `main.go` — kgo-опции, TryProduce и backpressure |
| `control-center` | C#, ASP.NET + Confluent.Kafka AdminClient | Мониторинг, имитация сбоев, UI | `Monitoring/ClusterMonitor.cs`, `KafkaInspector.cs`, `GroupProbe.cs`, `EventDetector.cs`, `Chaos/ChaosManager.cs` |
| `kafka-1..3` | Apache Kafka 4.1 (KRaft, combined) | Брокеры + контроллеры | `docker-compose.yml`, `infra/kafka/` |
| `kafka-ui` (опционально) | kafbat/kafka-ui | Сторонний UI для просмотра топиков | `docker compose --profile ui up -d` → http://localhost:8090 |

## Топики

| Топик | Партиций | RF | min.isr | Особенность |
|---|---|---|---|---|
| `orders` | 6 | 3 | 2 | Ключ = customerId, «надёжная» конфигурация |
| `payments` | 3 | 3 | 2 | Пишет order-processor |
| `orders.dlq` | 1 | 3 | 2 | Dead Letter Queue |
| `customer-profiles` | 3 | 3 | 2 | `cleanup.policy=compact` |
| `clickstream` | 6 | 2 | 1 | «Быстрая» конфигурация, retention 10 мин |

## Ускоренные таймауты

Чтобы эффекты было видно за секунды: `replica.lag.time.max.ms=10000` (по умолчанию 30 c), `leader.imbalance.check.interval.seconds=30` (300 c), консюмеры с `session.timeout.ms=10000` (45 c), `max.poll.interval.ms=20000` (5 мин), `offsets.topic.num.partitions=10` (50).

## Как сделаны сбои

- **Stop / Kill / Pause** — Docker Engine API через смонтированный `/var/run/docker.sock` (`docker stop` = SIGTERM, `kill` = SIGKILL, `pause` = cgroup freezer).
- **Сеть** — `tc qdisc` (netem) внутри сетевого namespace брокера через `docker exec` (контейнеры брокеров запущены с `cap_add: NET_ADMIN`):
  - задержка/потери: `netem delay 300ms 50ms loss 10%`;
  - полная изоляция: `netem loss 100%`;
  - split brain: `prio` + `u32`-фильтры по IP других брокеров → `netem loss 100%` только для межброкерного трафика.
- Правила исчезают при перезапуске контейнера — control-center это отслеживает.

## Как control-center видит кластер

Раз в секунду:
1. Docker API — состояние контейнеров.
2. `GetMetadata` — брокеры, лидеры, реплики, ISR.
3. `ListOffsets` (earliest/latest) — границы логов; скорости считаются по приращению.
4. `ListConsumerGroups` + `DescribeConsumerGroups` + `ListConsumerGroupOffsets` — участники, назначения, коммиты → lag. Этот опрос идёт в отдельном дочернем процессе (`GroupProbe`, см. грабли ниже).
5. `/api/stats` сервисов — то, что видят сами клиенты.
6. Раз в 4 c — `kafka-metadata-quorum.sh describe --replication` внутри брокера (кто активный контроллер).
7. `EventDetector` сравнивает снимок с предыдущим и пишет события в журнал; браузер получает всё по SSE (`/api/stream`).

## Запуск C#-сервисов из IDE

Брокеры опубликованы на хост как `localhost:19092,19093,19094` (listener `EXTERNAL`), поэтому можно остановить контейнер и запустить сервис из Visual Studio / Rider:

```bash
docker compose stop order-processor
cd services/order-processor
dotnet run            # appsettings.json уже смотрит на localhost:19092…
```

Control-center будет показывать такие консюмеры в группе (как внешних участников), но управлять ими через UI не сможет.

## Полезные команды

```bash
docker compose up -d --build                    # собрать и поднять всё
docker compose logs -f order-processor          # логи сервиса
docker compose up -d --scale analytics=3        # 3 экземпляра Python-консюмера → ребаланс
docker compose restart control-center           # перезапустить UI-бэкенд
docker compose down                             # остановить (данные Kafka сохранятся в томах)
docker compose down -v                          # остановить и стереть все данные
```

UI (`services/control-center/wwwroot`) и памятки (`docs/en`, `docs/ru`) смонтированы в контейнер — правки видны после обновления страницы.

## Онлайн-версия на GitHub Pages

Статическая копия UI публикуется workflow `.github/workflows/pages.yml`. Бэкенда там нет, поэтому `config.js` переключает UI в демо-режим: вместо SSE-стрима control-center проигрывается `demo/recording.json` — запись настоящего стенда (снимки раз в секунду + журнал событий + последние сообщения топиков) с пояснениями к ключевым моментам. Запись делает `tools/record-demo.py`: подключается к `/api/stream`, по таймлайну убивает и поднимает брокер, добавляет и «роняет» консюмеров, ставит analytics на паузу, добавляет задержку сети. Чтобы обновить демо — запусти скрипт на работающем стенде и закоммить новый файл.

Интерфейс и памятки есть на двух языках: английском (по умолчанию) и русском — переключатель EN/RU в шапке.

## Грабли, найденные при создании стенда

Реальные проблемы, которые всплыли при разработке, — сами по себе хорошие уроки:

1. **Падение librdkafka в admin-API.** Control-center падал с `SIGSEGV` / `double free detected` ровно в момент остановки брокера. Поиск методом исключения (отключали вызовы AdminClient по одному под сбоями брокеров) показал: процесс рушат admin-запросы к координатору групп (`ListConsumerGroups`, `DescribeConsumerGroups`, `ListConsumerGroupOffsets`), если брокер пропадает посреди запроса. Похожий баг описан и в апстриме ([librdkafka#5611](https://github.com/confluentinc/librdkafka/pull/5611), [klag-exporter#111](https://github.com/softwaremill/klag-exporter/issues/111)). Решение: опрос групп вынесен в отдельный дочерний процесс (`GroupProbe.cs`) — если он падает, супервизор запускает его снова, а control-center и UI продолжают работать (группы на секунду помечаются устаревшими). Мораль: код мониторинга обязательно тестируют **под сбоями**, а не на здоровом кластере.
2. **Разные partitioner-ы.** librdkafka по умолчанию хеширует ключ CRC32, Java — murmur2. Пока в order-service не включили `Partitioner.Murmur2Random`, предсказание партиции в UI (murmur2, как в Java) не совпадало с реальностью.
3. **Данные брокера внутри контейнера.** Первая версия хранила лог в файловой системе контейнера; `docker compose up --build` пересоздал брокеры — топики пропали, offset-ы начались с нуля, а «аудит доставки» честно закричал о потерях. Теперь данные на именованных томах.
4. **Фатальные ошибки идемпотентного producer-а.** При ретраях на фоне таймаутов или `NOT_ENOUGH_REPLICAS` librdkafka иногда сообщает `Fatal error: Unable to reconstruct MessageSet … unable to guarantee consistency`. После этого экземпляр producer-а бесполезен — order-service ловит `IsFatal` в error handler и создаёт новый. Без этого сервис молча перестал бы отправлять заказы.
5. **Ложное «всё доставлено».** Первая версия аудита доставки не замечала потерю при split brain: подтверждение нового заказа на тот же offset приходило раньше, чем аудитор читал запись, и затирало старое ожидание. Правило «два разных подтверждения на один offset = потеря первого» это исправило.
6. **Lag по коммитам «пилит».** Python-консюмер коммитит раз в 5 с, поэтому lag в штуках прыгал выше порога и генерировал ложные тревоги. Тревога теперь считается во времени (lag / скорость чтения).

## Если что-то пошло не так

- **«Нет соединения»** в шапке — control-center перезапускается или упал: `docker compose logs control-center`.
- **Брокер не стартует** после экспериментов — `docker compose up -d kafka-1 kafka-2 kafka-3`.
- **Всё сломалось** — кнопка «🩹 Починить всё», затем в «🎓 Сценарии» → «↺ Сбросить стенд». Крайний случай: `docker compose down -v && docker compose up -d --build`.
- **Добавили партиции и хотите вернуть 6** — только пересоздание: `docker compose down -v`.
