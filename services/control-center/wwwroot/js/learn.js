// Short "why?" explanations for journal events. Key = the event's `learn` field. Details live in the Cheat sheets tab.
import { T } from './i18n.js';

const L = (enTitle, ruTitle, enHtml, ruHtml) => ({ title: T(enTitle, ruTitle), html: T(enHtml, ruHtml) });

export const LEARN = {
  'leader-election': L('Partition leader election', 'Выборы лидера партиции',
    `Every partition has exactly one <b>leader</b> — all writes and (by default) reads go through it.
      The other replicas are <b>followers</b>: they keep fetching new data from the leader.
      When the leader's broker dies or gets fenced, the <b>controller (KRaft)</b> picks a new leader
      <b>only from the ISR</b> — replicas guaranteed to hold every acknowledged record.
      Clients get <code>NOT_LEADER_OR_FOLLOWER</code>, refresh metadata and switch over.
      For those seconds writes to the partition stall and the producer retries on its own.`,
    `У каждой партиции ровно один <b>лидер</b> — через него идут все записи и (по умолчанию) чтения.
      Остальные реплики — <b>followers</b>, они постоянно забирают (fetch) новые данные у лидера.
      Когда брокер-лидер падает или контроллер его фенсит, <b>контроллер (KRaft)</b> выбирает нового лидера
      <b>только из ISR</b> — реплик, у которых гарантированно есть все подтверждённые записи.
      Клиенты получают <code>NOT_LEADER_OR_FOLLOWER</code>, обновляют метаданные и переключаются.
      На эти секунды запись в партицию «залипает», producer сам ретраит.`),
  'preferred-leader': L('Preferred leader', 'Предпочтительный лидер',
    `The first replica in the <code>replicas</code> list is the "preferred leader". Kafka tries to keep leadership there
      so the load stays balanced. A returning broker doesn't take leadership back immediately: the controller checks the imbalance
      every <code>leader.imbalance.check.interval.seconds</code> (30 s here, 300 s by default),
      or you can run <code>kafka-leader-election.sh --election-type PREFERRED</code> — the "⚖ Preferred leaders" button.`,
    `Первая реплика в списке <code>replicas</code> — «предпочтительный лидер». Kafka старается держать лидерство на нём,
      чтобы нагрузка была равномерной. Вернувшийся брокер не забирает лидерство сразу: контроллер проверяет дисбаланс
      раз в <code>leader.imbalance.check.interval.seconds</code> (у нас 30 с, по умолчанию 300 с)
      или можно запустить <code>kafka-leader-election.sh --election-type PREFERRED</code> — кнопка «⚖ Предпочтительные лидеры».`),
  'isr': L('ISR — In-Sync Replicas', 'ISR — In-Sync Replicas',
    `The ISR are replicas that keep up with the leader: they fetched its log within the last
      <code>replica.lag.time.max.ms</code> (10 s here, 30 s by default). The leader is always in the ISR.
      The <b>high watermark</b> (how far consumers may read) advances only once a record is on every ISR replica.
      A lagging replica is dropped from the ISR (shrink); one that catches up is added back (expand).
      Only an ISR replica can become the new leader — that's why acknowledged data is not lost.`,
    `ISR — реплики, которые «не отстают» от лидера: успели дочитать его лог за последние
      <code>replica.lag.time.max.ms</code> (у нас 10 с, по умолчанию 30 с). Лидер всегда в ISR.
      <b>High Watermark</b> (до какого offset консюмеры могут читать) продвигается, только когда запись есть у всех реплик ISR.
      Отставшая реплика выкидывается из ISR (shrink), догнавшая — возвращается (expand).
      Новым лидером может стать только реплика из ISR — поэтому подтверждённые данные не теряются.`),
  'min-isr': L('min.insync.replicas and acks=all', 'min.insync.replicas и acks=all',
    `With <code>acks=all</code> the leader acknowledges a write only when it is on <b>all</b> ISR replicas,
      and only if the ISR has at least <code>min.insync.replicas</code> members. Otherwise the producer gets
      <code>NOT_ENOUGH_REPLICAS</code>: Kafka prefers <b>refusing the write</b> to risking its loss.
      The classic setup: RF=3, min.insync.replicas=2 → survive the loss of one broker without stopping writes.
      <code>acks=1</code> ignores this limit — writes go on, but may be lost when the leader changes.`,
    `При <code>acks=all</code> лидер подтверждает запись, только когда она есть у <b>всех</b> реплик ISR,
      и только если в ISR не меньше <code>min.insync.replicas</code> реплик. Иначе — ошибка
      <code>NOT_ENOUGH_REPLICAS</code>: Kafka предпочитает <b>отказать в записи</b>, чем рискнуть её потерять.
      Классика: RF=3, min.insync.replicas=2 → переживаем потерю одного брокера без остановки записи.
      <code>acks=1</code> этот лимит игнорирует — запись продолжится, но может потеряться при смене лидера.`),
  'offline-partition': L('Offline partition', 'Offline-партиция',
    `The partition has no leader: every ISR replica is unavailable. It can be neither read nor written.
      Kafka won't elect a replica outside the ISR while <code>unclean.leader.election.enable=false</code> (the default) —
      that could lose acknowledged data. The fix is to bring back a broker with an up-to-date replica.
      With RF=2, losing 2 brokers is enough to take some partitions offline.`,
    `У партиции нет лидера: все реплики из ISR недоступны. Читать и писать её нельзя.
      Kafka не выберет лидером реплику не из ISR, если <code>unclean.leader.election.enable=false</code> (по умолчанию) —
      иначе можно потерять подтверждённые данные. Лечение — вернуть брокер с актуальной репликой.
      Для топика с RF=2 достаточно потерять 2 брокера, чтобы часть партиций ушла в offline.`),
  'broker-fencing': L('Broker fencing (KRaft)', 'Фенсинг брокера (KRaft)',
    `Every broker sends heartbeats to the active controller. With no heartbeat for longer than
      <code>broker.session.timeout.ms</code> (9 s here), the controller <b>fences</b> the broker: removes it from metadata,
      drops its replicas from ISRs and moves leadership away. This protects the cluster from a half-dead node.
      Once connectivity returns, the broker registers again (unfence), catches up and rejoins the ISR.`,
    `Каждый брокер шлёт активному контроллеру heartbeat. Если heartbeat нет дольше
      <code>broker.session.timeout.ms</code> (у нас 9 с), контроллер <b>фенсит</b> брокер: убирает из метаданных,
      выкидывает его реплики из ISR и переносит лидерство. Так кластер защищается от «полуживого» узла.
      После восстановления связи брокер регистрируется снова (unfence), догоняет лог и возвращается в ISR.`),
  'kraft': L('KRaft: controllers and the quorum', 'KRaft: контроллеры и кворум',
    `Since Kafka 4.0 there is no ZooKeeper. Cluster metadata (topics, partitions, leaders, ISR, configs) lives in a
      separate replicated log, <code>__cluster_metadata</code>, maintained by <b>controllers</b> using Raft.
      One of them is active (the quorum leader), the rest are hot standbys. A majority is required:
      2 of 3 controllers must be alive. Here every node is both a broker and a controller (combined mode).`,
    `С Kafka 4.0 ZooKeeper больше нет. Метаданные кластера (топики, партиции, лидеры, ISR, конфиги) лежат в
      отдельном реплицируемом логе <code>__cluster_metadata</code>, который ведут <b>контроллеры</b> по протоколу Raft.
      Один из них — активный (лидер кворума), остальные — горячий резерв. Для работы нужно большинство:
      из 3 контроллеров должны быть живы 2. У нас каждый узел — и брокер, и контроллер (combined mode).`),
  'broker-down': L('A broker went down', 'Брокер упал',
    `A dead broker stops sending heartbeats → the controller fences it → leadership of its partitions moves
      to ISR replicas → those ISRs shrink → under-replicated partitions appear.
      Producers get connection errors, refresh metadata and keep writing to the new leaders.
      With SIGKILL there is no controlled shutdown, so the failover is slower than with <code>docker stop</code>.`,
    `Упавший брокер перестаёт слать heartbeat → контроллер фенсит его → лидерство его партиций переезжает
      на реплики из ISR → ISR этих партиций сокращаются → появляются under-replicated partitions.
      Producer-ы получают ошибки соединения, обновляют метаданные и продолжают писать новым лидерам.
      При SIGKILL нет controlled shutdown, поэтому переключение медленнее, чем при <code>docker stop</code>.`),
  'graceful-shutdown': L('Controlled shutdown', 'Controlled shutdown',
    `On a graceful stop (SIGTERM) the broker first asks the controller to move leadership away,
      waits for it and only then exits. Clients barely notice — this is how rolling restarts are done
      during upgrades. Compare with <b>Kill</b> (SIGKILL): there the cluster has to wait for timeouts.`,
    `При корректной остановке (SIGTERM) брокер сначала просит контроллер перенести с него лидерство,
      дожидается этого и только потом выходит. Клиенты почти не замечают остановку — так делают rolling restart
      при обновлениях. Сравни с <b>Kill</b> (SIGKILL): там кластеру приходится ждать таймаутов.`),
  'broker-pause': L('A frozen process', 'Заморозка процесса',
    `<code>docker pause</code> freezes the process: it does nothing, but its TCP connections stay open.
      It's like a very long GC pause or a stuck disk. A short pause (under the session timeout) is almost invisible — only latency grows.
      A long one gets the broker fenced and leadership moves away. After unfreezing, the broker finds the world has changed and catches up.`,
    `<code>docker pause</code> замораживает процесс: он ничего не делает, но TCP-соединения не закрыты.
      Это похоже на очень долгую GC-паузу или зависание диска. Короткая пауза (меньше session timeout) проходит почти незаметно — лишь растёт задержка.
      Длинная — брокер фенсится, лидерство уходит. После «разморозки» брокер обнаруживает, что мир изменился, и догоняет.`),
  'rebalance': L('Consumer group rebalance', 'Ребаланс consumer group',
    `A topic's partitions are split among the group's members: one partition — at most one consumer of the group.
      When a member joins, leaves or dies, the coordinator starts a <b>rebalance</b> and hands the partitions out again.
      <b>Eager</b> (range, roundrobin): everyone gives up all partitions and waits — "stop the world".
      <b>Cooperative-sticky</b>: only the partitions that must move are moved, the rest keep being consumed.
      <b>KIP-848</b> (group.protocol=consumer): the broker computes the assignment, rebalances are incremental.`,
    `Партиции топика делятся между участниками группы: одна партиция — максимум одному консюмеру группы.
      Когда участник входит, выходит или умирает, координатор запускает <b>ребаланс</b> и раздаёт партиции заново.
      <b>Eager</b> (range, roundrobin): все отдают все партиции и ждут — «stop the world».
      <b>Cooperative-sticky</b>: двигаются только нужные партиции, остальные читаются без остановки.
      <b>KIP-848</b> (group.protocol=consumer): назначение считает сам брокер, ребаланс инкрементальный.`),
  'consumer-group': L('Consumer group', 'Consumer group',
    `A group is a logical "subscriber" sharing one <code>group.id</code>. Within a group partitions are split (scaling),
      while different groups read the same topic independently — each with its own offsets in <code>__consumer_offsets</code>.
      More consumers than partitions? The extra ones sit idle: parallelism is capped by the partition count.`,
    `Группа = логический «подписчик» с общим <code>group.id</code>. Внутри группы партиции делятся (масштабирование),
      а разные группы читают один и тот же топик независимо — у каждой свои offset-ы в <code>__consumer_offsets</code>.
      Консюмеров больше, чем партиций? Лишние будут простаивать: параллелизм ограничен числом партиций.`),
  'consumer-crash': L('A consumer crashed', 'Консюмер упал',
    `A consumer that died without <code>Close()</code> sent no LeaveGroup and didn't commit its latest offsets.
      The coordinator notices only after <code>session.timeout.ms</code> (10 s here) — meanwhile nobody reads its partitions.
      Then a rebalance: the new owner starts from the <b>last committed</b> offset, so some messages are processed again
      (at-least-once).`,
    `Упавший без <code>Close()</code> консюмер не отправил LeaveGroup и не закоммитил последние offset-ы.
      Координатор заметит пропажу только через <code>session.timeout.ms</code> (у нас 10 с) — всё это время его партиции никто не читает.
      Потом ребаланс, новый владелец начинает с <b>последнего закоммиченного</b> offset — часть сообщений будет обработана повторно
      (at-least-once).`),
  'max-poll-interval': L('session.timeout vs max.poll.interval', 'session.timeout vs max.poll.interval',
    `Two different "death detectors":
      <b>session.timeout.ms</b> — heartbeats are sent by the client's background thread; if the process or network died, they stop.
      <b>max.poll.interval.ms</b> — if the application doesn't call poll()/Consume() for too long (stuck in processing),
      the consumer leaves the group by itself, even though the process is alive and heartbeats were flowing. Its partitions are declared lost,
      and committing is no longer possible.`,
    `Два разных «детектора смерти»:
      <b>session.timeout.ms</b> — heartbeat-ы шлёт фоновый поток клиента; если процесс умер или сеть пропала — их нет.
      <b>max.poll.interval.ms</b> — если приложение слишком долго не вызывает poll()/Consume() (зависло на обработке),
      консюмер сам выходит из группы, хотя процесс жив и heartbeat-ы шли. Партиции объявляются «потерянными» (lost),
      коммит уже невозможен.`),
  'static-membership': L('Static membership', 'Static membership',
    `With <code>group.instance.id</code> set, the coordinator remembers the member "by name".
      A restart within <code>session.timeout.ms</code> causes no rebalance — the instance gets its own partitions back.
      Handy for rolling restarts in Kubernetes: fewer rebalances and reshuffles.`,
    `Если задать <code>group.instance.id</code>, координатор запоминает участника «по имени».
      Перезапуск в пределах <code>session.timeout.ms</code> не вызывает ребаланс — экземпляр получает свои же партиции обратно.
      Полезно для rolling restart в Kubernetes: меньше ребалансов и «перетасовок».`),
  'coordinator': L('Group coordinator', 'Координатор группы',
    `Each group maps to a <code>__consumer_offsets</code> partition (hash(group.id) % 50; % 10 here).
      That partition's leader is the group's <b>coordinator</b>: it receives heartbeats and offset commits and runs rebalances.
      If the coordinator's broker dies, leadership of that __consumer_offsets partition moves — and the coordinator moves with it.`,
    `Каждой группе соответствует партиция <code>__consumer_offsets</code> (hash(group.id) % 50, у нас % 10).
      Её лидер — <b>координатор</b> группы: принимает heartbeat-ы, коммиты offset-ов и проводит ребалансы.
      Упал брокер-координатор — лидерство партиции __consumer_offsets переезжает, вместе с ним переезжает и координатор.`),
  'at-least-once': L('At-least-once and duplicates', 'At-least-once и дубликаты',
    `The order "process → commit offset" gives <b>at-least-once</b>: a crash between the two steps means the message
      is processed again. Duplicates are inevitable — so processing must be <b>idempotent</b>:
      upsert by key, a table of processed ids (inbox), unique constraints in the database.
      The reverse order ("commit → process") gives at-most-once: no duplicates, but possible losses.`,
    `Порядок «обработал → закоммитил offset» даёт <b>at-least-once</b>: при падении между этими шагами сообщение
      обработается ещё раз. Дубли неизбежны — значит, обработка должна быть <b>идемпотентной</b>:
      upsert по ключу, таблица уже обработанных id (inbox), уникальные ограничения в БД.
      Обратный порядок («закоммитил → обработал») даёт at-most-once: дублей нет, но возможны потери.`),
  'idempotence': L('Idempotent producer', 'Идемпотентный producer',
    `A broker response can get lost, and the producer resends the batch — without protection the log gets a duplicate.
      With <code>enable.idempotence=true</code> the producer gets a ProducerId and numbers its batches (sequence);
      the broker drops a repeat with the same number and keeps order even with <code>max.in.flight ≤ 5</code>.
      Since Kafka 3.0 idempotence is on by default in the Java client (it requires acks=all).
      Limitation: the protection covers one producer session and epoch. After a restart or an epoch bump
      caused by timeouts (KIP-360), a retry may pass as a new message — so consumers still need idempotent processing.`,
    `Ответ брокера может потеряться, и producer отправит batch повторно — без защиты в логе окажется дубликат.
      С <code>enable.idempotence=true</code> producer получает ProducerId и нумерует batch-и (sequence);
      брокер отбрасывает повтор с тем же номером и сохраняет порядок даже при <code>max.in.flight ≤ 5</code>.
      Начиная с Kafka 3.0 идемпотентность включена по умолчанию в Java-клиенте (требует acks=all).
      Ограничение: защита действует в пределах одной сессии и эпохи producer-а. После перезапуска или «бампа» эпохи
      из-за таймаутов (KIP-360) повтор может пройти как новое сообщение — поэтому консюмеру всё равно нужна идемпотентная обработка.`),
  'data-loss': L('How Kafka loses acknowledged data', 'Как Kafka теряет подтверждённые данные',
    `Scenario: the leader is cut off from the cluster, but clients still reach it. With <code>acks=1</code> it acknowledges writes itself
      without waiting for replicas. Meanwhile the controller elects a new leader, and the old one, once back,
      <b>truncates its log</b> to the divergence point (leader-epoch truncation) — the acknowledged records vanish.
      With <code>acks=all</code> + <code>min.insync.replicas=2</code> such a write would simply never have been acknowledged.`,
    `Сценарий: лидер изолирован от кластера, но клиенты его видят. С <code>acks=1</code> он подтверждает записи сам,
      не дожидаясь реплик. Тем временем контроллер выбирает нового лидера, а старый, вернувшись,
      <b>обрезает свой лог</b> до точки расхождения (truncation по leader epoch) — подтверждённые записи исчезают.
      С <code>acks=all</code> + <code>min.insync.replicas=2</code> такая запись просто не подтвердилась бы.`),
  'split-brain': L('Network partition (split brain)', 'Сетевое разделение (split brain)',
    `The broker can't see the other brokers or the controller, but clients still reach it. It still believes it's the leader
      (a "zombie leader") while the cluster has already elected another. Kafka's defences: with acks=all the zombie can't gather its ISR and
      doesn't acknowledge writes; after broker.session.timeout it gets fenced; once connectivity is back it truncates its log.
      That's exactly why acks=1 is dangerous.`,
    `Брокер не видит других брокеров и контроллер, но клиенты до него достают. Он ещё считает себя лидером
      («зомби-лидер»), а кластер уже выбрал другого. Kafka защищается так: при acks=all зомби не может собрать ISR и
      не подтверждает записи; через broker.session.timeout его фенсят; после восстановления связи он обрезает свой лог.
      Именно поэтому acks=1 опасен.`),
  'delivery-timeout': L('delivery.timeout.ms', 'delivery.timeout.ms',
    `How long the producer keeps trying to deliver a message overall, including time in the buffer and every retry
      (<code>message.timeout.ms</code> in librdkafka). While there is no leader or the ISR is too small, the producer silently retries;
      if time runs out, the callback gets an error (<code>Local_MsgTimedOut</code>).
      The status <code>PossiblyPersisted</code> means "maybe written" — resending may create a duplicate.`,
    `Сколько всего producer пытается доставить сообщение, включая ожидание в буфере и все ретраи
      (в librdkafka — <code>message.timeout.ms</code>). Пока лидера нет или ISR мал, producer молча ретраит;
      если за это время не вышло — callback получает ошибку (<code>Local_MsgTimedOut</code>).
      Статус <code>PossiblyPersisted</code> значит «возможно, записалось» — повторная отправка может дать дубль.`),
  'fatal-producer': L('Fatal producer error', 'Фатальная ошибка producer-а',
    `A Kafka client survives most errors on its own (retries, reconnects). But an idempotent producer can end up
      in a state where it can no longer guarantee ordering and no duplicates (e.g. after a series of timeouts
      or OUT_OF_ORDER_SEQUENCE). Then the error is marked <b>fatal</b>: this instance stops sending forever,
      and the application must create a new producer (it gets a new ProducerId). Handling <code>IsFatal</code> in the error handler is mandatory.`,
    `Большинство ошибок клиент Kafka переживает сам (ретраи, переподключение). Но идемпотентный producer может попасть
      в состояние, где он больше не способен гарантировать порядок и отсутствие дублей (например, после череды таймаутов
      или OUT_OF_ORDER_SEQUENCE). Тогда ошибка помечается как <b>fatal</b>: этот экземпляр навсегда перестаёт отправлять,
      и приложение обязано создать новый producer (он получит новый ProducerId). Обработка <code>IsFatal</code> в error handler — обязательна.`),
  'request-timeout': L('Request timeout', 'Таймаут запроса',
    `If the broker doesn't answer within <code>request.timeout.ms</code>, the producer treats the request as failed and retries.
      But the request may have arrived and been written — the response was just late. Without idempotence that means duplicates in the log.`,
    `Если брокер не ответил за <code>request.timeout.ms</code>, producer считает запрос неудачным и повторяет.
      Но запрос мог дойти и записаться — просто ответ задержался. Без идемпотентности это даёт дубли в логе.`),
  'backpressure': L('Backpressure', 'Backpressure',
    `The producer is asynchronous: <code>Produce()</code> only puts the message into a local buffer, a background thread sends it.
      If brokers are down or too slow, the buffer grows up to its limit (<code>buffer.memory</code> / <code>queue.buffering.max.messages</code>),
      after which Produce blocks or throws <code>QueueFull</code>. It's a natural brake for the data source.`,
    `Producer асинхронный: <code>Produce()</code> только кладёт сообщение в локальный буфер, отправкой занимается фоновый поток.
      Если брокеры недоступны или не успевают, буфер растёт до лимита (<code>buffer.memory</code> / <code>queue.buffering.max.messages</code>),
      после чего Produce блокируется или бросает <code>QueueFull</code>. Это естественный «тормоз» для источника данных.`),
  'dlq': L('Dead Letter Queue', 'Dead Letter Queue',
    `A message that can't be processed (a broken format — a "poison pill" — or failures after N retries)
      can't be retried forever: the partition would stall because the offset doesn't move. It's parked in a separate topic (DLQ)
      with headers explaining why, and processing moves on. Kafka has no built-in DLQ (unlike RabbitMQ) — it's an application pattern.`,
    `Сообщение, которое не удаётся обработать (битый формат — «poison pill», или ошибки после N ретраев),
      нельзя бесконечно повторять: партиция встанет, ведь offset не двигается. Его откладывают в отдельный топик (DLQ)
      с заголовками-причинами и идут дальше. В Kafka нет встроенной DLQ (в отличие от RabbitMQ) — это паттерн приложения.`),
  'pause': L('pause() / resume()', 'pause() / resume()',
    `<code>consumer.pause(partitions)</code> stops fetching, but the consumer stays in the group:
      heartbeats keep flowing and the partitions stay assigned. Used for backpressure (e.g. when downstream is overloaded).
      Lag grows while paused; after resume the consumer continues from the same offset.`,
    `<code>consumer.pause(partitions)</code> останавливает выборку данных, но консюмер остаётся в группе:
      heartbeat-ы идут, партиции за ним. Используют для backpressure (например, когда downstream перегружен).
      Lag на паузе растёт, после resume консюмер продолжает с того же offset.`),
  'lag': L('Consumer lag', 'Consumer lag',
    `Lag = log end (high watermark) − the group's committed offset. It's the key health metric of consumers.
      It grows when processing is slower than writing. Remedies: speed up processing, add consumers (up to the partition count!),
      add partitions, batch the work. It's handy to convert lag into time: lag / processing rate.`,
    `Lag = конец лога (high watermark) − закоммиченный offset группы. Это главная метрика здоровья консюмеров.
      Растёт, когда обработка медленнее записи. Лечится: ускорить обработку, добавить консюмеров (до числа партиций!),
      увеличить число партиций, батчить работу. Lag в штуках удобно переводить во время: lag / скорость обработки.`),
  'key-partition': L('Key → partition', 'Ключ → партиция',
    `Partition = <code>murmur2(key) % partitions</code>. One key always maps to one partition, so
      one customer's messages are ordered. Ordering is guaranteed <b>only within a partition</b>.
      Without a key the producer uses the sticky partitioner: it fills a batch for one partition, then switches.`,
    `Партиция = <code>murmur2(key) % число_партиций</code>. Один ключ — всегда одна партиция, поэтому
      сообщения одного клиента упорядочены. Порядок гарантируется <b>только внутри партиции</b>.
      Без ключа producer использует sticky partitioner: наполняет batch для одной партиции, потом переключается.`),
  'batching': L('Batching', 'Batching',
    `The producer groups messages of one partition into a batch: it waits up to <code>linger.ms</code> or until <code>batch.size</code> bytes.
      Bigger batches = fewer requests, better compression and higher throughput, at the cost of slightly higher latency.`,
    `Producer группирует сообщения одной партиции в batch: ждёт до <code>linger.ms</code> или до <code>batch.size</code> байт.
      Большие batch-и = меньше запросов, лучше сжатие, выше пропускная способность, но чуть выше задержка.`),
  'producer-config': L('Producer settings', 'Настройки producer',
    `acks, idempotence, compression and linger are set when the producer is created — changing them means recreating the client.
      The old producer waits (flush) for already sent messages before closing.`,
    `acks, идемпотентность, compression, linger задаются при создании producer — чтобы их поменять, клиента пересоздают.
      Уже отправленные сообщения старый producer дожидается (flush) перед закрытием.`),
  'compaction': L('Log compaction', 'Log compaction',
    `<code>cleanup.policy=compact</code>: Kafka keeps not every message but the latest value for each key
      (like a key → value table). A message with <code>value = null</code> is a tombstone: it deletes the key at the next cleanup.
      Used for state, configs, Kafka Streams changelogs and __consumer_offsets.`,
    `<code>cleanup.policy=compact</code>: Kafka хранит не все сообщения, а последнее значение для каждого ключа
      (как таблица key → value). Сообщение с <code>value = null</code> — tombstone, удаляет ключ при следующей чистке.
      Используется для состояний, конфигов, changelog-ов Kafka Streams и __consumer_offsets.`),
  'replay': L('Replaying history', 'Перечитать историю',
    `Kafka keeps messages after they are read (until retention), so a group can be "rewound":
      <code>kafka-consumer-groups.sh --reset-offsets --to-earliest --execute</code>. Only for an inactive group — stop its consumers first.
      This is how analytics gets recomputed, processing bugs get fixed, and new services get bootstrapped from history.`,
    `Kafka хранит сообщения после прочтения (до retention), поэтому группу можно «перемотать»:
      <code>kafka-consumer-groups.sh --reset-offsets --to-earliest --execute</code>. Только для неактивной группы — сначала останови консюмеров.
      Так пересчитывают аналитику, чинят баги обработки, поднимают новый сервис на исторических данных.`),
  'network-chaos': L('Network impairments', 'Сетевые помехи',
    `Latency and packet loss are implemented with Linux <code>tc netem</code> inside the broker's network namespace.
      Latency stretches acks=all (we wait for replicas), loss causes TCP retransmits and timeouts,
      100% loss is full isolation, after which the broker gets fenced.`,
    `Задержка и потери пакетов реализованы через Linux <code>tc netem</code> в сетевом namespace брокера.
      Задержка растягивает acks=all (ждём реплики), потери вызывают ретраи TCP и таймауты,
      100% потерь — полная изоляция, после которой брокер фенсится.`),
  'partitions-increase': L('Adding partitions', 'Увеличение числа партиций',
    `Partitions can only be added. But <code>murmur2(key) % N</code> changes with N — new messages
      for existing keys may go to another partition, breaking the "old → new" order for that key.
      That's why keyed topics get their partition count with headroom from the start.`,
    `Партиции можно только добавлять. Но <code>murmur2(key) % N</code> меняется вместе с N — новые сообщения
      существующих ключей могут уйти в другую партицию, и порядок «старое → новое» для ключа нарушится.
      Поэтому число партиций для топиков с ключами закладывают с запасом сразу.`),
};
