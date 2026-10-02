// Step-by-step labs. Each one: goal → steps with buttons → "what to watch" → takeaways.
import { h, $, toast, partitionFor } from './util.js';
import { store } from './store.js';
import { svc, actions, demoGuard } from './api.js';
import { showSubtab } from './panels.js';
import { T } from './i18n.js';

// ------------------------------------------------------------------ helpers

const sleep = ms => new Promise(r => setTimeout(r, ms));
const order = patch => svc.config('order-service', patch);
const proc = patch => svc.config('order-processor', patch);
const click = patch => svc.config('clickstream-generator', patch);
const analytics = patch => svc.config('analytics', patch);
const topicConfig = (topic, key, value) => actions.topicConfig(topic, key, value);
const NET_OK = { latencyMs: 0, jitterMs: 0, lossPct: 0, isolated: false, splitFromBrokers: false };

function controllerId() { return store.snap?.cluster?.quorum?.leaderId ?? null; }

function leadersOf(topic) {
  const counts = new Map();
  for (const p of store.topic(topic)?.partitions ?? []) if (p.leader >= 0) counts.set(p.leader, (counts.get(p.leader) ?? 0) + 1);
  return counts;
}

/** The broker leading the most partitions of the topic (preferably not the active controller). */
function pickBroker(topic = 'orders', avoidController = true) {
  const online = store.snap?.brokers?.filter(b => b.status === 'online').map(b => b.id) ?? [];
  const counts = leadersOf(topic);
  const ranked = online.sort((a, b) => (counts.get(b) ?? 0) - (counts.get(a) ?? 0));
  const ctrl = controllerId();
  return (avoidController ? ranked.find(id => id !== ctrl && (counts.get(id) ?? 0) > 0) : null) ?? ranked[0] ?? 1;
}

function processorInstances() { return store.stats('order-processor')?.instances ?? []; }

async function setProcessorCount(n) {
  let inst = processorInstances().filter(i => ['running', 'stuck', 'starting'].includes(i.state));
  while (inst.length < n) { await svc.post('order-processor', 'instances'); inst.push({}); await sleep(300); }
  inst = processorInstances().filter(i => ['running', 'stuck', 'starting'].includes(i.state)).sort((a, b) => b.id - a.id);
  for (let i = 0; i < inst.length - n; i++) { await svc.del('order-processor', `instances/${inst[i].id}`); await sleep(300); }
}

export async function resetLab() {
  if (demoGuard()) return;
  toast(T('Restoring the lab to its default settings…', 'Возвращаю стенд к исходным настройкам…'));
  await actions.healAll();
  await order({ ratePerSec: 20, acks: 'all', enableIdempotence: true, lingerMs: 5, compression: 'none', requestTimeoutMs: 10000, deliveryTimeoutMs: 30000, hotKeyPercent: 0, maxInFlight: 5 });
  await proc({ processingDelayMs: 5, failureRatePercent: 0, maxRetries: 2, assignmentStrategy: 'cooperative-sticky', sessionTimeoutMs: 10000, maxPollIntervalMs: 20000, autoCommitIntervalMs: 5000, staticMembership: false });
  await click({ rate: 1000, sizeBytes: 200, acks: '1', compression: 'lz4', lingerMs: 10, keyed: false });
  await analytics({ paused: false, processingDelayMs: 0 });
  await topicConfig('orders', 'min.insync.replicas', '2');
  for (const i of processorInstances().filter(i => i.state === 'stuck')) await svc.post('order-processor', `instances/${i.id}/stuck?stuck=false`);
  await setProcessorCount(2);
  toast(T('The lab is back to its default state', 'Стенд в исходном состоянии'), 'ok');
}

// An action inside a scenario: { label, run(ctx), kind }
const A = (label, run, kind = '') => ({ label, run, kind });

const crashFirst = () => { const i = processorInstances()[0]; return i && svc.post('order-processor', `instances/${i.id}/crash`); };
const addConsumer = () => svc.post('order-processor', 'instances');

// ------------------------------------------------------------------ scenarios

const SCENARIOS = [
  {
    id: 'key-partition', level: 1, title: T('Key → partition and ordering', 'Ключ → партиция и порядок'),
    goal: T('Understand how the producer picks a partition, why ordering is guaranteed only within a partition, and why a "hot" key is dangerous.',
      'Понять, как producer выбирает партицию, почему порядок гарантирован только внутри партиции и чем опасен «горячий» ключ.'),
    steps: [
      {
        text: T(`The <b>order-service</b> card has a key field (<code>customer-007</code> by default). Next to it the UI computes
          <code>murmur2(key) % 6</code> — the same hash Kafka uses. Send an order with this key three times in a row.`,
          `В карточке <b>order-service</b> есть поле ключа (по умолчанию <code>customer-007</code>). Рядом UI сам считает
          <code>murmur2(key) % 6</code> — тот же хеш, что использует Kafka. Отправь заказ с этим ключом три раза подряд.`),
        watch: T("All three orders land in the same partition, and the offset grows by 1 each time. The UI's prediction matches the broker's answer.",
          'Все три заказа попадут в одну и ту же партицию, а offset будет расти на 1. Предсказание UI совпадёт с ответом брокера.'),
        actions: [A(T('Send 3 orders for customer-007', 'Отправить 3 заказа customer-007'), async () => {
          for (let i = 0; i < 3; i++) {
            const r = await svc.post('order-service', 'orders', { customerId: 'customer-007' });
            if (r?.ok) toast(T(`customer-007 → partition ${r.partition}, offset ${r.offset}`, `customer-007 → партиция ${r.partition}, offset ${r.offset}`), 'ok', 6000);
          }
        })],
      },
      {
        text: T('Now different keys. Every customer is "pinned" to its partition — so its orders are processed strictly in order.',
          'Теперь разные ключи. Каждый клиент «закреплён» за своей партицией — значит, его заказы обработаются строго по порядку.'),
        watch: T('Different keys → different (or colliding modulo N) partitions. Prediction = reality.',
          'Разные ключи → разные (или совпадающие по модулю) партиции. Предсказание = факт.'),
        actions: [A(T('Send customer-001…004', 'Отправить customer-001…004'), async () => {
          const n = store.topic('orders')?.partitions.length ?? 6;
          for (const k of ['customer-001', 'customer-002', 'customer-003', 'customer-004']) {
            const r = await svc.post('order-service', 'orders', { customerId: k });
            if (r?.ok) toast(T(`${k}: predicted P${partitionFor(k, n)}, the broker answered P${r.partition}`, `${k}: предсказано P${partitionFor(k, n)}, брокер ответил P${r.partition}`), 'ok', 7000);
          }
        })],
      },
      {
        text: T('Turn on the "hot key": 80% of orders come from <code>customer-007</code>, and raise the load to 200/s.',
          'Включим «горячий ключ»: 80% заказов от <code>customer-007</code> и поднимем нагрузку до 200/с.'),
        watch: T('The leader chip of one orders partition starts glowing (a hot partition). In the "Partitions" table its rate is several times higher than the others, and the consumer that owns it builds up lag — while the rest sit idle.',
          'Чип лидера одной партиции orders начнёт светиться (горячая партиция). В таблице «Партиции» её скорость будет в разы выше остальных, а у консюмера, которому она досталась, вырастет lag — остальные при этом простаивают.'),
        actions: [A(T('Hot key 80%, 200/s', 'Горячий ключ 80%, 200/с'), () => order({ hotKeyPercent: 80, ratePerSec: 200 })),
          A(T('Open the partitions table', 'Открыть таблицу партиций'), () => showSubtab('partitions'))],
      },
      {
        text: T('Bring the load back. Think: how would you relieve a hot key? (Hint: a composite key customerId+orderId loses per-customer ordering — it is a trade-off.)',
          'Вернём нагрузку. Подумай: как бы ты разгрузил горячий ключ? (Подсказка: составной ключ customerId+orderId теряет порядок для клиента — это компромисс.)'),
        actions: [A(T('Reset: 0%, 20/s', 'Сбросить: 0%, 20/с'), () => order({ hotKeyPercent: 0, ratePerSec: 20 }))],
      },
    ],
    summary: T(`<ul><li>Partition = <code>murmur2(key) % N</code>. One key — always one partition (as long as N doesn't change).</li>
      <li>Ordering is guaranteed only <b>within a partition</b>. Need ordering per entity — use its id as the key.</li>
      <li>No key — the sticky partitioner: a whole batch goes to one partition, then the next one.</li>
      <li>Skewed keys → hot partitions → one consumer is overloaded, and scaling out doesn't help.</li>
      <li>librdkafka uses CRC32 by default (<code>consistent_random</code>), not murmur2 — set <code>partitioner=murmur2_random</code> for Java compatibility.</li></ul>`,
      `<ul><li>Партиция = <code>murmur2(key) % N</code>. Один ключ — всегда одна партиция (пока N не меняется).</li>
      <li>Порядок гарантирован только <b>внутри партиции</b>. Нужен порядок по сущности — используй её id как ключ.</li>
      <li>Без ключа — sticky partitioner: batch целиком в одну партицию, потом следующая.</li>
      <li>Неравномерные ключи → горячие партиции → один консюмер перегружен, масштабирование не помогает.</li>
      <li>librdkafka по умолчанию использует CRC32 (<code>consistent_random</code>), а не murmur2 — для совместимости с Java ставят <code>partitioner=murmur2_random</code>.</li></ul>`),
  },
  {
    id: 'rebalance', level: 1, title: T('Consumer group: scaling and rebalancing', 'Consumer group: масштабирование и ребаланс'),
    goal: T('See how partitions are split between consumers, what happens when a member joins or leaves, and how eager differs from cooperative.',
      'Увидеть, как партиции делятся между консюмерами, что происходит при входе/выходе участника и чем eager отличается от cooperative.'),
    steps: [
      {
        text: T('Look at the <b>order-processing</b> group on the right: every consumer has its own set of orders partitions (o0…o5). One partition — one consumer of the group.',
          'Посмотри на группу <b>order-processing</b> справа: у каждого консюмера свой набор партиций orders (o0…o5). Одна партиция — одному консюмеру группы.'),
        watch: T('Each of the 6 partitions belongs to exactly one member.', 'Каждая из 6 партиций принадлежит ровно одному участнику.'),
      },
      {
        text: T('Add a third consumer.', 'Добавим третий консюмер.'),
        watch: T('In the event log: "joined processor-3", the group goes PreparingRebalance → Stable. With cooperative-sticky only some partitions are taken from the old consumers — the rest keep being read without a pause.',
          'В журнале: «вступили processor-3», состояние группы PreparingRebalance → Stable. При cooperative-sticky у старых консюмеров забирают только часть партиций — остальные читаются без остановки.'),
        actions: [A(T('+1 consumer', '+1 консюмер'), addConsumer)],
      },
      {
        text: T('Bring the consumer count up to 7 — more than there are partitions.', 'Доведём число консюмеров до 7 — больше, чем партиций.'),
        watch: T('The seventh consumer gets "no partitions — idle". Group parallelism is capped by the partition count!',
          'Седьмой консюмер получит «нет партиций — простаивает». Параллелизм группы ограничен числом партиций!'),
        actions: [A(T('Make 7 consumers', 'Сделать 7 консюмеров'), () => setProcessorCount(7))],
      },
      {
        text: T('Switch the strategy to <b>range</b> (the eager protocol). All consumers restart.',
          'Переключим стратегию на <b>range</b> (eager-протокол). Все консюмеры перезапустятся.'),
        watch: T('In an eager rebalance every member first gives up ALL its partitions ("revoked …") and only then receives new ones — stop-the-world. Compare with cooperative-sticky.',
          'При eager-ребалансе каждый участник сначала отдаёт ВСЕ свои партиции («отозваны …»), и только потом получает новые — stop-the-world. Сравни с cooperative-sticky.'),
        actions: [A(T('Strategy: range', 'Стратегия range'), () => proc({ assignmentStrategy: 'range' }))],
      },
      {
        text: T('Remove 4 consumers gracefully (Close → LeaveGroup).', 'Уберём 4 консюмера корректно (Close → LeaveGroup).'),
        watch: T('The rebalance happens right away, without waiting for timeouts — because the consumers said goodbye.',
          'Ребаланс происходит сразу, без ожидания таймаутов — потому что консюмеры попрощались.'),
        actions: [A(T('Keep 3', 'Оставить 3'), () => setProcessorCount(3))],
      },
      {
        text: T('Try the new <b>KIP-848</b> protocol (group.protocol=consumer): the coordinator broker computes the assignment, and clients receive changes incrementally.',
          'Попробуй новый протокол <b>KIP-848</b> (group.protocol=consumer): назначение партиций считает брокер-координатор, клиенты получают изменения инкрементально.'),
        watch: T('The group card shows type Consumer and assignor uniform/range (server-side). Add/remove a consumer and see how quickly the group stabilizes.',
          'В карточке группы тип станет Consumer, assignor — uniform/range (серверный). Добавь/убери консюмер и посмотри, как быстро стабилизируется группа.'),
        actions: [A('KIP-848', () => proc({ assignmentStrategy: 'consumer' })),
          A(T('Back to cooperative-sticky', 'Вернуть cooperative-sticky'), () => proc({ assignmentStrategy: 'cooperative-sticky' })),
          A(T('Keep 2 consumers', 'Оставить 2 консюмера'), () => setProcessorCount(2))],
      },
    ],
    summary: T(`<ul><li>Within a group a partition is read by one consumer → keeping more consumers than partitions is pointless.</li>
      <li>A rebalance is triggered when a member joins, leaves or dies, or when the subscription or partition count changes.</li>
      <li>Eager (range/roundrobin) — everyone gives up everything; cooperative-sticky — the minimum moves; KIP-848 — rebalancing on the broker side.</li>
      <li>A graceful Close() speeds up the rebalance: no need to wait for session.timeout.</li></ul>`,
      `<ul><li>Внутри группы партиция читается одним консюмером → консюмеров больше партиций держать бессмысленно.</li>
      <li>Ребаланс запускается при входе/выходе/смерти участника, изменении подписки или числа партиций.</li>
      <li>Eager (range/roundrobin) — все отдают всё; cooperative-sticky — двигается минимум; KIP-848 — ребаланс на стороне брокера.</li>
      <li>Корректный Close() ускоряет ребаланс: не нужно ждать session.timeout.</li></ul>`),
  },
  {
    id: 'two-groups', level: 1, title: T('Two groups read one topic', 'Две группы читают один топик'),
    goal: T("Understand Kafka's pub/sub model: every consumer group receives all messages and keeps its own offsets. Replay the history.",
      'Понять модель pub/sub в Kafka: каждая consumer group получает все сообщения и хранит свои offset-ы. Перечитать историю.'),
    steps: [
      {
        text: T('The orders topic is read by two groups: <b>order-processing</b> (C#) and <b>analytics</b> (Python). Open the partitions table: every partition has two lag columns — one per group.',
          'Топик orders читают две группы: <b>order-processing</b> (C#) и <b>analytics</b> (Python). Открой таблицу партиций: у каждой партиции два столбца lag — по одному на группу.'),
        actions: [A(T('Partitions table', 'Таблица партиций'), () => showSubtab('partitions'))],
      },
      {
        text: T('Pause analytics — <code>consumer.pause()</code>.', 'Поставим analytics на паузу — <code>consumer.pause()</code>.'),
        watch: T("The analytics lag grows while order-processing keeps working as if nothing happened. The analytics members stay in the group — they keep their partitions.",
          'Lag analytics растёт, а order-processing продолжает работать как ни в чём не бывало. Участники analytics остаются в группе — партиции за ними.'),
        actions: [A(T('pause() analytics', 'pause() analytics'), () => analytics({ paused: true }))],
      },
      {
        text: T('Resume it.', 'Снимем паузу.'),
        watch: T('Analytics catches up: its read rate is far above the write rate until the lag drops to 0.',
          'Analytics догоняет поток: скорость чтения резко выше скорости записи, пока lag не уйдёт в 0.'),
        actions: [A('resume()', () => analytics({ paused: false }))],
      },
      {
        text: T("Kafka doesn't delete what was read. Replay the whole history: stop analytics → <code>kafka-consumer-groups.sh --reset-offsets --to-earliest --execute</code> → start it.",
          'Kafka не удаляет прочитанное. Перечитаем всю историю: остановить analytics → <code>kafka-consumer-groups.sh --reset-offsets --to-earliest --execute</code> → запустить.'),
        watch: T("The analytics lag jumps to the size of all the topics, then drains. order-processing isn't affected.",
          'Lag analytics подскочит до размера всех топиков, затем рассосётся. order-processing это не затронет.'),
        actions: [A(T('⏪ Replay history', '⏪ Перечитать историю'), () => $('.card.group[data-group="analytics"] .btn-warn')?.click())],
      },
    ],
    summary: T(`<ul><li>A topic ≠ a queue: messages aren't deleted after being read, they live until retention.</li>
      <li>Different groups = independent subscribers with their own offsets in __consumer_offsets.</li>
      <li>Offsets can be rewound (reset-offsets) — only for an inactive group.</li>
      <li>pause()/resume() slows reading down without leaving the group.</li></ul>`,
      `<ul><li>Топик ≠ очередь: сообщения не удаляются после чтения, а живут до retention.</li>
      <li>Разные группы = независимые подписчики со своими offset-ами в __consumer_offsets.</li>
      <li>Offset-ы можно перемотать (reset-offsets) — только у неактивной группы.</li>
      <li>pause()/resume() — способ притормозить чтение, не выходя из группы.</li></ul>`),
  },
  {
    id: 'lag', level: 1, title: T('A slow consumer and lag', 'Медленный консюмер и lag'),
    goal: T('See lag grow, and why scaling consumers hits the partition count.',
      'Увидеть, как растёт lag, и почему масштабирование консюмеров упирается в число партиций.'),
    steps: [
      {
        text: T('Raise writes to 500 orders/s. Two consumers with 5 ms processing handle ~400/s.',
          'Поднимем запись до 500 заказов/с. Два консюмера с обработкой 5 мс тянут ~400/с.'),
        watch: T('The order-processing lag starts growing (see the chart in the group card).', 'Lag order-processing начинает расти (график в карточке группы).'),
        actions: [A(T('500 orders/s', '500 заказов/с'), () => order({ ratePerSec: 500 }))],
      },
      {
        text: T('Scale the group to 6 consumers.', 'Масштабируем группу до 6 консюмеров.'),
        watch: T('After the rebalance the lag starts shrinking: 6 × 200/s > 500/s.', 'После ребаланса lag начнёт уменьшаться: 6 × 200/с > 500/с.'),
        actions: [A(T('6 consumers', '6 консюмеров'), () => setProcessorCount(6))],
      },
      {
        text: T('Now processing got slower — 20 ms per order: 6 × 50 = 300/s < 500/s.', 'Теперь обработка стала медленнее — 20 мс на заказ: 6 × 50 = 300/с < 500/с.'),
        watch: T("The lag grows again. A seventh consumer won't help — orders has only 6 partitions.",
          'Lag снова растёт. Добавить седьмой консюмер не поможет — у orders всего 6 партиций.'),
        actions: [A(T('Processing 20 ms', 'Обработка 20 мс'), () => proc({ processingDelayMs: 20 })),
          A(T('+1 consumer (useless)', '+1 консюмер (бесполезно)'), addConsumer)],
      },
      {
        text: T('Put everything back.', 'Вернём всё назад.'),
        actions: [A(T('Reset', 'Сбросить'), async () => { await order({ ratePerSec: 20 }); await proc({ processingDelayMs: 5 }); await setProcessorCount(2); })],
      },
    ],
    summary: T(`<ul><li>Lag = high watermark − committed offset. The key consumer metric.</li>
      <li>Group throughput ≈ min(consumers, partitions) × the speed of one consumer.</li>
      <li>Partitions are the unit of parallelism. Plan their count with headroom.</li>
      <li>Other levers: batch processing, asynchronous processing inside the consumer, optimizing downstream systems.</li></ul>`,
      `<ul><li>Lag = high watermark − committed offset. Главная метрика консюмеров.</li>
      <li>Пропускная способность группы ≈ min(консюмеры, партиции) × скорость одного консюмера.</li>
      <li>Партиции — единица параллелизма. Их число закладывают с запасом.</li>
      <li>Другие рычаги: батчевая обработка, асинхронная обработка внутри консюмера, оптимизация downstream.</li></ul>`),
  },
  {
    id: 'broker-failure', level: 2, title: T('Broker failure: graceful vs kill', 'Падение брокера: graceful vs kill'),
    goal: T('See leader election, ISR shrinking, under-replicated partitions, and the difference between a graceful shutdown and a crash.',
      'Увидеть выборы лидера, сжатие ISR, under-replicated partitions и разницу между корректной остановкой и крэшем.'),
    steps: [
      {
        text: T('Gracefully stop (SIGTERM) the broker that leads the most orders partitions.',
          'Остановим корректно (SIGTERM) брокер с наибольшим числом лидеров orders.'),
        watch: T('Controlled shutdown: leadership moves almost instantly, then the broker disappears. Partition ISRs shrink, URP grows in the header. The producer keeps writing: with RF=3 and min.insync.replicas=2 two replicas are enough.',
          'Controlled shutdown: лидерство переезжает почти мгновенно, затем брокер исчезает. ISR партиций сжимается, в шапке растёт URP. Producer продолжает писать: при RF=3 и min.insync.replicas=2 двух реплик достаточно.'),
        actions: [A(T('Stop the busiest broker', 'Stop самого нагруженного'), async ctx => {
          ctx.b1 = pickBroker('orders', false);
          toast(T(`Stopping broker ${ctx.b1}`, `Останавливаю broker ${ctx.b1}`));
          await actions.broker(ctx.b1, 'stop');
        })],
      },
      {
        text: T('Start it again.', 'Запустим его обратно.'),
        watch: T('The broker starts, catches up with the leaders and rejoins the ISR (event log: "back in ISR"). Leadership returns to it within ~30 s (auto.leader.rebalance) or right away with the ⚖ button.',
          'Брокер стартует, догоняет лидеров и возвращается в ISR (журнал: «вернулись в ISR»). Лидерство вернётся к нему в течение ~30 с (auto.leader.rebalance) или сразу по кнопке ⚖.'),
        actions: [A('Start', ctx => actions.broker(ctx.b1 ?? pickBroker(), 'start')),
          A(T('⚖ Preferred leaders', '⚖ Предпочтительные лидеры'), () => actions.preferred())],
      },
      {
        text: T('Now "kill" (SIGKILL) another broker — no controlled shutdown.', 'Теперь «убьём» (SIGKILL) другой брокер — без controlled shutdown.'),
        watch: T("For about 9 seconds (broker.session.timeout.ms) the cluster doesn't know the broker is dead: the partitions it led are unavailable — order-service p99 spikes. Then fencing and leader election.",
          'Около 9 секунд (broker.session.timeout.ms) кластер не знает, что брокер мёртв: партиции, где он был лидером, недоступны — p99 у order-service подскакивает. Потом фенсинг и выборы лидера.'),
        actions: [A(T('Kill another broker', 'Kill другого брокера'), async ctx => {
          ctx.b2 = pickBroker('orders', true);
          if (ctx.b2 === ctx.b1) ctx.b2 = (ctx.b1 % 3) + 1;
          toast(T(`Killing broker ${ctx.b2}`, `Убиваю broker ${ctx.b2}`));
          await actions.broker(ctx.b2, 'kill');
        })],
      },
      {
        text: T('Bring it back up.', 'Поднимем его.'),
        actions: [A('Start', ctx => actions.broker(ctx.b2 ?? 1, 'start'))],
      },
    ],
    summary: T(`<ul><li>A leader is elected only from the ISR — acknowledged data isn't lost.</li>
      <li>Graceful stop: leadership is handed off in advance, clients barely notice. Kill: we wait for the heartbeat timeout.</li>
      <li>URP (under-replicated partitions) is the main alert: the safety margin is reduced.</li>
      <li>A returning broker first catches up on the log (fetch), then rejoins the ISR, then gets leadership back.</li></ul>`,
      `<ul><li>Лидер выбирается только из ISR — подтверждённые данные не теряются.</li>
      <li>Graceful stop: лидерство передаётся заранее, клиенты почти не замечают. Kill: ждём таймаут heartbeat.</li>
      <li>URP (under-replicated partitions) — главный алерт: запас прочности снижен.</li>
      <li>Вернувшийся брокер сначала догоняет лог (fetch), потом возвращается в ISR, потом — лидерство.</li></ul>`),
  },
  {
    id: 'min-isr', level: 2, title: T('acks=all, min.insync.replicas and rejected writes', 'acks=all, min.insync.replicas и отказ в записи'),
    goal: T('Understand when Kafka rejects writes for the sake of durability, and how acks=1 bypasses that protection.',
      'Понять, когда Kafka отказывает в записи ради надёжности и как acks=1 обходит эту защиту.'),
    steps: [
      {
        text: T('Make the requirement stricter: <code>min.insync.replicas=3</code> for orders — an acks=all write needs ALL three replicas. order-service uses acks=all.',
          'Сделаем требование строже: для orders <code>min.insync.replicas=3</code> — запись с acks=all требует ВСЕ три реплики. order-service работает с acks=all.'),
        actions: [A('min.insync.replicas=3', () => topicConfig('orders', 'min.insync.replicas', '3')),
          A(T('acks=all, 50/s', 'acks=all, 50/с'), () => order({ acks: 'all', enableIdempotence: true, ratePerSec: 50 }))],
      },
      {
        text: T('Stop one broker.', 'Остановим один брокер.'),
        watch: T('ISR<min > 0 in the header, "ISR < min.insync.replicas" in the event log. The producer gets NOT_ENOUGH_REPLICAS and retries: acknowledgements stop, "awaiting ack" grows, after delivery.timeout (30 s) come Local_MsgTimedOut errors. Kafka honestly refuses instead of losing data.',
          'В шапке ISR<min > 0, в журнале «ISR < min.insync.replicas». Producer получает NOT_ENOUGH_REPLICAS и ретраит: подтверждения прекращаются, «в полёте» растёт, через delivery.timeout (30 с) — ошибки Local_MsgTimedOut. Kafka честно отказывает, а не теряет.'),
        actions: [A(T('Stop a broker', 'Stop брокера'), async ctx => {
          ctx.b = pickBroker('orders', true);
          toast(T(`Stopping broker ${ctx.b}`, `Останавливаю broker ${ctx.b}`));
          await actions.broker(ctx.b, 'stop');
        })],
      },
      {
        text: T('Switch the producer to acks=1.', 'Переключим producer на acks=1.'),
        watch: T("Writes resume instantly: the leader doesn't wait for replicas. Convenient, but such writes may be lost when the leader changes (see the \"Zombie leader\" scenario).",
          'Запись мгновенно возобновилась: лидеру не нужно ждать реплики. Удобно, но такие записи могут потеряться при смене лидера (см. сценарий «Зомби-лидер»).'),
        actions: [A('acks=1', () => order({ acks: '1', enableIdempotence: false }))],
      },
      {
        text: T('Put everything back.', 'Вернём всё как было.'),
        actions: [A(T('Restore', 'Восстановить'), async ctx => {
          await actions.broker(ctx.b ?? 1, 'start');
          await topicConfig('orders', 'min.insync.replicas', '2');
          await order({ acks: 'all', enableIdempotence: true, ratePerSec: 20 });
        })],
      },
    ],
    summary: T(`<ul><li>acks=all + min.insync.replicas=N: acknowledged only if the write is on at least N replicas.</li>
      <li>The typical choice: RF=3, min.insync.replicas=2 — survive the loss of one broker without stopping writes.</li>
      <li>min.insync.replicas=RF — losing any broker stops writes.</li>
      <li>acks=1 ignores min.insync.replicas: higher availability, weaker guarantees.</li></ul>`,
      `<ul><li>acks=all + min.insync.replicas=N: подтверждение только если запись есть минимум на N репликах.</li>
      <li>Типичный выбор: RF=3, min.insync.replicas=2 — переживаем потерю одного брокера без остановки записи.</li>
      <li>min.insync.replicas=RF — любая потеря брокера останавливает запись.</li>
      <li>acks=1 игнорирует min.insync.replicas: доступность выше, гарантий меньше.</li></ul>`),
  },
  {
    id: 'zombie', level: 3, title: T('Zombie leader: how acks=1 loses data', 'Зомби-лидер: как acks=1 теряет данные'),
    goal: T('Reproduce a real loss of acknowledged writes and see how acks=all prevents it.',
      'Воспроизвести настоящую потерю подтверждённых записей и увидеть, как acks=all её предотвращает.'),
    steps: [
      {
        text: T('Switch order-service to <code>acks=1</code> without idempotence, 100 orders/s. Watch the "🔍 Delivery audit" box — LOST is 0 right now.',
          'Переключим order-service на <code>acks=1</code> без идемпотентности, 100 заказов/с. Следи за блоком «🔍 Аудит доставки» — сейчас там ПОТЕРЯНО 0.'),
        actions: [A(T('acks=1, 100/s', 'acks=1, 100/с'), () => order({ acks: '1', enableIdempotence: false, ratePerSec: 100 }))],
      },
      {
        text: T(`Cut off from the cluster a broker that leads some orders partitions: it can't see the other brokers and the controller,
          but <b>clients can still reach it</b>.`,
          `Отрежем от кластера брокер, который лидирует в части партиций orders: он не видит других брокеров и контроллер,
          но <b>клиенты по-прежнему до него достают</b>.`),
        watch: T("The producer keeps getting acknowledgements from the \"zombie\". After ~9 s the controller fences the broker and elects new leaders from the ISR — they don't have the writes the zombie accepted. Wait 20–30 seconds.",
          'Producer продолжает получать подтверждения от «зомби». Через ~9 с контроллер зафенсит брокер и выберет новых лидеров из ISR — у них нет записей, принятых зомби. Подожди 20–30 секунд.'),
        actions: [A('🔌 Split brain', async ctx => {
          ctx.b = pickBroker('orders', true);
          toast(T(`Cutting broker ${ctx.b} off from the other brokers`, `Отрезаю broker ${ctx.b} от других брокеров`));
          await actions.network(ctx.b, { ...NET_OK, splitFromBrokers: true });
        })],
      },
      {
        text: T('Restore the network.', 'Вернём сеть.'),
        watch: T('The broker sees the new leader epoch and TRUNCATES its log to the divergence point. The delivery audit finds orders that were acknowledged but disappeared: "LOST N" and samples like "o-123 @ orders-2:456 → now holds o-789".',
          'Брокер видит новый leader epoch и ОБРЕЗАЕТ свой лог до точки расхождения. Аудит доставки найдёт заказы, которые были подтверждены, но исчезли: «ПОТЕРЯНО N» и примеры «o-123 @ orders-2:456 → там теперь o-789».'),
        actions: [A(T('Restore network', 'Вернуть сеть'), ctx => actions.network(ctx.b ?? 1, NET_OK))],
      },
      {
        text: T('Repeat with <code>acks=all</code> + idempotence.', 'Повторим с <code>acks=all</code> + идемпотентность.'),
        watch: T("Now the zombie can't gather an ISR and doesn't acknowledge writes: they hang \"in flight\", then go to the new leaders or time out. The LOST counter doesn't grow.",
          'Теперь зомби не может собрать ISR и не подтверждает записи: они висят «в полёте», затем уходят новым лидерам или падают по таймауту. Счётчик ПОТЕРЯНО не растёт.'),
        actions: [A('acks=all', () => order({ acks: 'all', enableIdempotence: true })),
          A('🔌 Split brain', async ctx => { ctx.b = pickBroker('orders', true); await actions.network(ctx.b, { ...NET_OK, splitFromBrokers: true }); }),
          A(T('Restore network', 'Вернуть сеть'), ctx => actions.network(ctx.b ?? 1, NET_OK))],
      },
      { text: T('Bring the load back.', 'Вернём нагрузку.'), actions: [A(T('20/s', '20/с'), () => order({ ratePerSec: 20 }))] },
    ],
    summary: T(`<ul><li>acks=1 acknowledges a write that only the leader has. The leader changes — the write may vanish.</li>
      <li>After a network partition the old leader truncates its log by leader epoch (KIP-101), and "acknowledged" data disappears.</li>
      <li>acks=all + min.insync.replicas≥2 + unclean.leader.election.enable=false = acknowledged data is never lost.</li>
      <li>This is not theory: data was lost exactly like this in real incidents during network partitions.</li></ul>`,
      `<ul><li>acks=1 подтверждает запись, которая есть только у лидера. Сменился лидер — запись может исчезнуть.</li>
      <li>После разделения сети старый лидер обрезает лог по leader epoch (KIP-101), «подтверждённые» данные пропадают.</li>
      <li>acks=all + min.insync.replicas≥2 + unclean.leader.election.enable=false = подтверждённое не теряется.</li>
      <li>Это не теория: так теряли данные в реальных инцидентах при сетевых разделениях.</li></ul>`),
  },
  {
    id: 'duplicates', level: 3, title: T('Network latency, timeouts and duplicates', 'Сетевая задержка, таймауты и дубли'),
    goal: T('Get duplicates in the log because of retries, and remove them with an idempotent producer.',
      'Получить дубликаты в логе из-за ретраев и убрать их идемпотентным producer-ом.'),
    steps: [
      {
        text: T('Configure an "aggressive" producer: acks=all, idempotence <b>off</b>, request.timeout = 1 s.',
          'Настроим «агрессивный» producer: acks=all, идемпотентность <b>выключена</b>, request.timeout = 1 с.'),
        actions: [A(T('No idempotence, timeout 1 s', 'Без идемпотентности, timeout 1 с'),
          () => order({ acks: 'all', enableIdempotence: false, requestTimeoutMs: 1000, deliveryTimeoutMs: 30000, ratePerSec: 50 }))],
      },
      {
        text: T('Add 400±300 ms of latency to a broker that leads some orders partitions: some responses make it within 1 s, some don\'t.',
          'Добавим брокеру-лидеру части партиций orders задержку 400±300 мс: часть ответов будет успевать за 1 с, часть — нет.'),
        watch: T('The broker manages to write the batch, but the response arrives after the timeout. The producer considers the request failed and sends the batch again → duplicates in the log ("duplicates in log" in the audit, "Duplicate in the log" events). Wait 20–30 seconds.',
          'Брокер успевает записать batch, но ответ приходит позже таймаута. Producer считает запрос неудачным и шлёт batch снова → в логе дубли («дубли в логе» в аудите, события «Дубликат в логе»). Подожди 20–30 секунд.'),
        actions: [A(T('🐢 Latency 400±300 ms', '🐢 Задержка 400±300 мс'), async ctx => {
          ctx.b = pickBroker('orders', true);
          toast(T(`Adding latency to broker ${ctx.b}`, `Задержка на broker ${ctx.b}`));
          await actions.network(ctx.b, { ...NET_OK, latencyMs: 400, jitterMs: 300 });
        })],
      },
      {
        text: T('Turn on idempotence.', 'Включим идемпотентность.'),
        watch: T("Retries remain, but the broker recognizes a repeated batch by (ProducerId, epoch, sequence) and doesn't write it twice. For the first ~10 s duplicates from the old producer are still being counted, then the growth almost stops. Almost — because under mass timeouts librdkafka bumps the producer epoch (KIP-360), and the broker can no longer recognize a rare retry with new numbers. Takeaway: the idempotent producer cuts duplicates dramatically, but idempotent processing on the consumer side is still required.",
          'Ретраи остаются, но брокер узнаёт повторный batch по (ProducerId, epoch, sequence) и не пишет его второй раз. Первые ~10 с ещё досчитываются дубли старого producer-а, потом рост почти останавливается. Почти — потому что при массовых таймаутах librdkafka «перезапускает» эпоху producer-а (KIP-360), и редкий повтор с новыми номерами брокер уже не узнаёт. Вывод: идемпотентный producer резко снижает дубли, но идемпотентная обработка у консюмера всё равно нужна.'),
        actions: [A(T('Idempotence ON', 'Идемпотентность ON'), () => order({ enableIdempotence: true }))],
      },
      {
        text: T('Restore the network and the timeouts.', 'Вернём сеть и таймауты.'),
        actions: [A(T('Restore', 'Восстановить'), async ctx => { await actions.network(ctx.b ?? 1, NET_OK); await order({ requestTimeoutMs: 10000, ratePerSec: 20 }); })],
      },
    ],
    summary: T(`<ul><li>A timeout ≠ a failed write: the request may have succeeded while the response got lost or arrived late.</li>
      <li>Retries without idempotence = duplicates (and even reordering with max.in.flight > 1).</li>
      <li>enable.idempotence=true solves this within one producer session (and epoch); after an epoch bump caused by timeouts, rare duplicates are possible.</li>
      <li>A timeout shorter than the real network latency = retries, duplicates and even fatal errors of the idempotent producer. Give timeouts headroom.</li>
      <li>For "exactly-once" between topics — transactions; for side effects — an idempotent consumer.</li></ul>`,
      `<ul><li>Таймаут ≠ ошибка записи: запрос мог выполниться, а ответ — потеряться или опоздать.</li>
      <li>Ретраи без идемпотентности = дубли (и даже нарушение порядка при max.in.flight > 1).</li>
      <li>enable.idempotence=true решает это в пределах одной сессии (и эпохи) producer-а; после бампа эпохи из-за таймаутов редкие дубли возможны.</li>
      <li>Таймаут меньше реальной задержки сети = ретраи, дубли и даже фатальные ошибки идемпотентного producer-а. Таймауты должны быть с запасом.</li>
      <li>Для «exactly-once» между топиками — транзакции, для побочных эффектов — идемпотентный консюмер.</li></ul>`),
  },
  {
    id: 'consumer-crash', level: 2, title: T('Consumer crash, duplicates and static membership', 'Крэш консюмера, дубли и static membership'),
    goal: T('Get to grips with session.timeout, max.poll.interval, at-least-once and static membership.',
      'Разобраться с session.timeout, max.poll.interval, at-least-once и static membership.'),
    steps: [
      {
        text: T('Raise the load to 200/s and "crash" the first consumer (💥 = Dispose without Close).',
          'Поднимем нагрузку до 200/с и «уроним» первый консюмер (💥 = Dispose без Close).'),
        watch: T('The member stays in the group as a "ghost" for ~10 s (session.timeout.ms) — nobody reads its partitions, their lag grows. Then a rebalance, and the new owner re-reads messages from the last commit → "duplicates" in the group card.',
          'Участник остаётся в группе «призраком» ~10 с (session.timeout.ms) — его партиции никто не читает, lag на них растёт. Затем ребаланс, а новый владелец перечитывает сообщения с последнего коммита → «дубли» в карточке группы.'),
        actions: [A(T('200/s', '200/с'), () => order({ ratePerSec: 200 })), A(T('💥 Crash a consumer', '💥 Крэш консюмера'), crashFirst),
          A(T('+1 consumer', '+1 консюмер'), addConsumer)],
      },
      {
        text: T('Shrink the duplicate window: auto.commit.interval = 100 ms — and crash a consumer again.',
          'Уменьшим окно дублей: auto.commit.interval = 100 мс — и снова уроним консюмер.'),
        watch: T("Noticeably fewer duplicates after the crash: offsets are committed more often. But duplicates won't vanish entirely — you need idempotent processing.",
          'Дублей после крэша заметно меньше: offset-ы коммитятся чаще. Но совсем дубли не исчезнут — нужна идемпотентная обработка.'),
        actions: [A(T('auto.commit 100 ms', 'auto.commit 100 мс'), () => proc({ autoCommitIntervalMs: 100 })), A(T('💥 Crash', '💥 Крэш'), crashFirst),
          A(T('+1 consumer', '+1 консюмер'), addConsumer)],
      },
      {
        text: T('Turn on static membership and do a quick restart (🔄: crash + start with the same id after 2 s).',
          'Включим static membership и сделаем быстрый рестарт (🔄: крэш + запуск с тем же id через 2 с).'),
        watch: T('The group stays Stable — the coordinator recognizes the member by group.instance.id and gives it the same partitions. Without static membership there would be two rebalances.',
          'Группа остаётся Stable — координатор узнаёт участника по group.instance.id и отдаёт ему те же партиции. Без static membership было бы два ребаланса.'),
        actions: [A('static membership', () => proc({ staticMembership: true })),
          A(T('🔄 Restart a consumer', '🔄 Рестарт консюмера'), () => { const i = processorInstances()[0]; return i && svc.post('order-processor', `instances/${i.id}/restart`); })],
      },
      {
        text: T('Now a "hang": the consumer is alive, heartbeats keep flowing, but Consume() is not called.',
          'Теперь «зависание»: консюмер жив, heartbeat-ы идут, но Consume() не вызывается.'),
        watch: T('After max.poll.interval.ms (20 s) the consumer leaves the group by itself: "partitions LOST". Click 🧊 again — it comes back.',
          'Через max.poll.interval.ms (20 с) консюмер сам покинет группу: «партиции ПОТЕРЯНЫ». Нажми 🧊 ещё раз — он вернётся.'),
        actions: [A(T('🧊 Hang', '🧊 Зависнуть'), () => { const i = processorInstances()[0]; return i && svc.post('order-processor', `instances/${i.id}/stuck?stuck=true`); }),
          A(T('Unstick', 'Отвиснуть'), () => { const i = processorInstances().find(x => x.state === 'stuck'); return i && svc.post('order-processor', `instances/${i.id}/stuck?stuck=false`); })],
      },
      {
        text: T('Restore the settings.', 'Вернём настройки.'),
        actions: [A(T('Reset', 'Сбросить'), async () => { await proc({ staticMembership: false, autoCommitIntervalMs: 5000 }); await order({ ratePerSec: 20 }); await setProcessorCount(2); })],
      },
    ],
    summary: T(`<ul><li>session.timeout — "the process/network is dead" (no heartbeat). max.poll.interval — "the application is stuck" (no poll).</li>
      <li>Committing after processing = at-least-once: after a crash some messages are processed again.</li>
      <li>Processing must be idempotent; commit frequency only shrinks the duplicate window.</li>
      <li>Static membership removes unnecessary rebalances on quick restarts.</li></ul>`,
      `<ul><li>session.timeout — «процесс/сеть мертвы» (нет heartbeat). max.poll.interval — «приложение зависло» (нет poll).</li>
      <li>Коммит после обработки = at-least-once: после крэша часть сообщений обработается повторно.</li>
      <li>Обработка должна быть идемпотентной; частота коммитов лишь уменьшает окно дублей.</li>
      <li>Static membership убирает лишние ребалансы при быстрых рестартах.</li></ul>`),
  },
  {
    id: 'kraft', level: 2, title: T('KRaft: losing the active controller', 'KRaft: потеря активного контроллера'),
    goal: T('See how the controller quorum survives losing its leader, and why a majority is required.',
      'Увидеть, как кворум контроллеров переживает потерю лидера и почему нужно большинство.'),
    steps: [
      {
        text: T("The header shows which node is the active controller right now (★ on the broker card). Fully isolate its network.",
          'В шапке видно, какой узел сейчас активный контроллер (★ в карточке брокера). Полностью изолируем его сеть.'),
        watch: T('Within a few seconds the remaining two controllers elect a new leader (event log: "KRaft: the active controller changed"). Then the new controller fences the isolated broker and re-elects leaders for its partitions.',
          'Через несколько секунд оставшиеся два контроллера выберут нового лидера (журнал: «KRaft: активный контроллер сменился»). Затем новый контроллер зафенсит изолированный брокер и перевыберет лидеров его партиций.'),
        actions: [A(T('✂ Isolate the controller', '✂ Изолировать контроллер'), async ctx => {
          ctx.c = controllerId() ?? 1;
          toast(T(`Isolating the controller — node ${ctx.c}`, `Изолирую контроллер — узел ${ctx.c}`));
          await actions.network(ctx.c, { ...NET_OK, isolated: true });
        })],
      },
      {
        text: T('Restore the network.', 'Вернём сеть.'),
        watch: T('The node comes back as a regular quorum follower, catches up on the metadata log and registers as a broker.',
          'Узел возвращается обычным follower-ом кворума, догоняет лог метаданных и регистрируется как брокер.'),
        actions: [A(T('Restore network', 'Вернуть сеть'), ctx => actions.network(ctx.c ?? 1, NET_OK))],
      },
      {
        text: T('⚠ Optional: stop two nodes out of three. That is a loss of quorum: no majority → no active controller.',
          '⚠ Опционально: остановим два узла из трёх. Это потеря кворума: нет большинства → нет активного контроллера.'),
        watch: T("The quorum doesn't answer, metadata is \"frozen\": you can't elect leaders, create a topic or fence a broker. The remaining broker serves only the partitions it already leads (and only acks≤1).",
          'Кворум не отвечает, метаданные «замерли»: нельзя выбрать лидеров, создать топик, зафенсить брокер. Оставшийся брокер обслуживает только те партиции, где он уже лидер (и только acks≤1).'),
        actions: [A(T('Stop two nodes', 'Stop двух узлов'), async ctx => {
          const ids = [1, 2, 3].filter(i => i !== (controllerId() ?? 1));
          ctx.two = ids;
          for (const i of ids) await actions.broker(i, 'stop');
        }),
        A(T('Start them again', 'Запустить обратно'), async ctx => { for (const i of ctx.two ?? [1, 2, 3]) await actions.broker(i, 'start'); })],
      },
    ],
    summary: T(`<ul><li>Cluster metadata is a log too (__cluster_metadata), replicated via Raft between the controllers.</li>
      <li>A majority is required: 3 controllers survive losing 1, 5 survive losing 2.</li>
      <li>In production controllers usually run on dedicated nodes (process.roles=controller).</li></ul>`,
      `<ul><li>Метаданные кластера — это тоже лог (__cluster_metadata), реплицируемый по Raft между контроллерами.</li>
      <li>Нужно большинство: 3 контроллера переживают потерю 1, 5 — потерю 2.</li>
      <li>В production контроллеры обычно выделяют на отдельные узлы (process.roles=controller).</li></ul>`),
  },
  {
    id: 'pause', level: 2, title: T('Broker pause ≈ a long GC pause', 'Пауза брокера ≈ долгая GC-пауза'),
    goal: T('Understand why short pauses are safe while long ones cause fencing and leader movement.',
      'Понять, почему короткие паузы безопасны, а длинные вызывают фенсинг и переезд лидеров.'),
    steps: [
      {
        text: T('Freeze a broker for 4 seconds — less than broker.session.timeout.ms (9 s).',
          'Заморозим брокер на 4 секунды — меньше, чем broker.session.timeout.ms (9 с).'),
        watch: T('Acknowledgement latency (p99) spikes, but there is no fencing and no leader election.',
          'Задержка подтверждений (p99) подскочит, но ни фенсинга, ни выборов лидера не будет.'),
        actions: [A(T('Pause 4 s', 'Пауза 4 с'), async ctx => {
          ctx.b = pickBroker('orders', true);
          toast(T(`Pausing broker ${ctx.b} for 4 s`, `Пауза broker ${ctx.b} на 4 с`));
          await actions.broker(ctx.b, 'pause'); await sleep(4000); await actions.broker(ctx.b, 'unpause');
        })],
      },
      {
        text: T('Now 20 seconds.', 'Теперь 20 секунд.'),
        watch: T('After ~9 s — "The controller fenced the broker", leader elections, the ISR shrinks. After unfreezing, the broker discovers it is no longer the leader and catches up.',
          'Через ~9 с — «Контроллер зафенсил брокер», выборы лидеров, ISR сжимается. После разморозки брокер обнаруживает, что больше не лидер, и догоняет.'),
        actions: [A(T('Pause 20 s', 'Пауза 20 с'), async ctx => {
          ctx.b = pickBroker('orders', true);
          toast(T(`Pausing broker ${ctx.b} for 20 s`, `Пауза broker ${ctx.b} на 20 с`));
          await actions.broker(ctx.b, 'pause'); await sleep(20000); await actions.broker(ctx.b, 'unpause');
          toast(T('Broker unfrozen', 'Брокер разморожен'), 'ok');
        })],
      },
    ],
    summary: T(`<ul><li>Timeouts are a trade-off: short ones detect failures faster but misfire on GC pauses.</li>
      <li>Long broker GC pauses = "flapping" leadership. Monitor GC and the heap size.</li></ul>`,
      `<ul><li>Таймауты — компромисс: короткие быстрее обнаруживают сбой, но ложно срабатывают на GC-паузах.</li>
      <li>Длинные GC-паузы у брокера = «мигание» лидерства. Мониторь GC и размер heap.</li></ul>`),
  },
  {
    id: 'dlq', level: 1, title: T('Poison pill and Dead Letter Queue', 'Poison pill и Dead Letter Queue'),
    goal: T('Understand what to do with a message that cannot be processed.', 'Понять, что делать с сообщением, которое невозможно обработать.'),
    steps: [
      {
        text: T('Send broken JSON (a "poison pill") to orders.', 'Отправим в orders битый JSON («poison pill»).'),
        watch: T("order-processor can't parse it and immediately sends it to orders.dlq (event log: \"poison pill → orders.dlq\"). Without a DLQ the consumer would either get stuck on this offset forever or silently lose the message.",
          'order-processor не сможет его разобрать и сразу отправит в orders.dlq (журнал: «poison pill → orders.dlq»). Без DLQ консюмер либо застрял бы на этом offset навсегда, либо молча потерял сообщение.'),
        actions: [A('☠ Poison pill', () => actions.produce({ topic: 'orders', key: 'customer-013', value: '{"orderId": oops, not json', note: 'dlq' }))],
      },
      {
        text: T('Look at the DLQ: open "Messages", topic orders.dlq — the headers carry the reason and the coordinates of the original.',
          'Посмотрим DLQ: открой «Сообщения», топик orders.dlq — в заголовках причина и координаты оригинала.'),
        actions: [A(T('Open "Messages"', 'Открыть «Сообщения»'), () => showSubtab('messages'))],
      },
      {
        text: T('Now 10% "processing errors": every error → retries (up to 2) → DLQ.', 'Теперь 10% «ошибок обработки»: каждая ошибка → ретраи (до 2) → DLQ.'),
        watch: T('The "retries" and "DLQ" counters grow, the orders.dlq topic is being written (a red particle stream).',
          'Растут счётчики «ретраи» и «DLQ», топик orders.dlq пишется (красный поток частиц).'),
        actions: [A(T('10% errors', '10% ошибок'), () => proc({ failureRatePercent: 10 })), A(T('Back to 0%', 'Вернуть 0%'), () => proc({ failureRatePercent: 0 }))],
      },
    ],
    summary: T(`<ul><li>You can't retry one message forever — the partition would stall.</li>
      <li>The pattern: N retries (sometimes via delayed retry topics) → DLQ with the reason in the headers → investigation and re-sending.</li>
      <li>Kafka has no built-in DLQ (unlike RabbitMQ) — the application or framework implements it.</li></ul>`,
      `<ul><li>Нельзя бесконечно ретраить одно сообщение — партиция встанет.</li>
      <li>Паттерн: N ретраев (иногда через retry-топики с задержкой) → DLQ с причиной в заголовках → разбор и повторная отправка.</li>
      <li>В Kafka нет встроенной DLQ (в отличие от RabbitMQ) — её реализует приложение или фреймворк.</li></ul>`),
  },
  {
    id: 'compaction', level: 2, title: T('Compaction: a topic as a table', 'Compaction: топик как таблица'),
    goal: T('See how a compacted topic keeps only the latest value for each key.', 'Увидеть, как compacted-топик хранит только последнее значение по каждому ключу.'),
    steps: [
      {
        text: T('With every order, order-service writes the customer profile to <code>customer-profiles</code> (cleanup.policy=compact). There are only 50 keys but thousands of messages. Open "Messages" → customer-profiles.',
          'order-service при каждом заказе пишет профиль клиента в <code>customer-profiles</code> (cleanup.policy=compact). Ключей всего 50, а сообщений — тысячи. Открой «Сообщения» → customer-profiles.'),
        actions: [A(T('Open "Messages"', 'Открыть «Сообщения»'), () => showSubtab('messages'))],
      },
      {
        text: T('Send a tombstone (value = null) for customer-007.', 'Отправим tombstone (value = null) для customer-007.'),
        watch: T('This "deletes" the key: after the next compaction all values of customer-007 disappear (the tombstone itself stays for delete.retention.ms).',
          'Это «удаление» ключа: после очередной компакции все значения customer-007 исчезнут (сам tombstone хранится ещё delete.retention.ms).'),
        actions: [A('🪦 Tombstone customer-007', () => actions.produce({ topic: 'customer-profiles', key: 'customer-007', value: null, note: 'compaction' }))],
      },
      {
        text: T('Wait 1–2 minutes (segment.ms=60 s: only closed segments are compacted) and read the partition again.',
          'Подожди 1–2 минуты (segment.ms=60 с: компактятся только закрытые сегменты) и прочитай партицию снова.'),
        watch: T('The log start moves, and the old part has "holes" in its offsets: one latest value per key. The active segment is never compacted.',
          'Начало лога сдвинется, в старой части останутся «дырки» в offset-ах: на каждый ключ — одно последнее значение. Активный сегмент не компактится никогда.'),
      },
    ],
    summary: T(`<ul><li>compact = "the latest value per key", delete = "everything within the retention period".</li>
      <li>A tombstone (null) deletes a key. This is how __consumer_offsets, Kafka Streams changelogs and KTables work.</li>
      <li>Offsets aren't renumbered — after compaction there are holes in them.</li></ul>`,
      `<ul><li>compact = «последнее значение по ключу», delete = «всё за период retention».</li>
      <li>Tombstone (null) удаляет ключ. Так устроены __consumer_offsets, changelog-и Kafka Streams, KTable.</li>
      <li>Offset-ы не перенумеровываются — после компакции в них дырки.</li></ul>`),
  },
  {
    id: 'load', level: 2, title: T('Load: batching, compression, backpressure', 'Нагрузка: batching, сжатие, backpressure'),
    goal: T('Feel how linger, compression and message size affect throughput.', 'Почувствовать, как linger, compression и размер сообщений влияют на пропускную способность.'),
    steps: [
      {
        text: T('Speed clickstream up to 20,000 messages/s without batching or compression.', 'Разгоним clickstream до 20 000 сообщений/с без batching и сжатия.'),
        watch: T('Watch "traffic" and p99 in the Go generator card, the write rate in the header and the analytics lag (Python may fall behind).',
          'Смотри «трафик» и p99 в карточке Go-генератора, скорость записи в шапке и lag analytics (Python может не успевать).'),
        actions: [A(T('20k/s, linger 0, none', '20k/с, linger 0, none'), () => click({ rate: 20000, lingerMs: 0, compression: 'none' }))],
      },
      {
        text: T('Turn on linger 50 ms and zstd compression.', 'Включим linger 50 мс и сжатие zstd.'),
        watch: T('Messages are packed into large batches: fewer requests, fewer bytes on the network and on disk. Acknowledgement latency grows by roughly linger.',
          'Сообщения собираются в крупные batch-и: меньше запросов, меньше байт по сети и на диске. Задержка подтверждения вырастет примерно на linger.'),
        actions: [A('linger 50, zstd', () => click({ lingerMs: 50, compression: 'zstd' }))],
      },
      {
        text: T("Large messages: 5 KB at 50,000/s. The cluster may not cope.", 'Крупные сообщения: 5 KB и 50 000/с. Возможно, кластер не справится.'),
        watch: T("If the brokers can't keep up, the producer buffer fills and backpressure kicks in. Analytics will most likely fall behind: scale it with <code>docker compose up -d --scale analytics=3</code>.",
          'Если брокеры не успевают — буфер producer-а заполняется и включается backpressure. Analytics, скорее всего, отстанет: масштабируй её командой <code>docker compose up -d --scale analytics=3</code>.'),
        actions: [A(T('5 KB × 50k/s', '5 KB × 50k/с'), () => click({ rate: 50000, sizeBytes: 5000 }))],
      },
      {
        text: T('Back to the normal load.', 'Вернём нормальную нагрузку.'),
        actions: [A(T('1k/s, 200 B, lz4', '1k/с, 200 B, lz4'), () => click({ rate: 1000, sizeBytes: 200, lingerMs: 10, compression: 'lz4' }))],
      },
    ],
    summary: T(`<ul><li>Kafka throughput = batching + sequential writes + page cache + zero-copy.</li>
      <li>linger.ms and batch.size trade latency for throughput. Compression works on a whole batch.</li>
      <li>The producer is asynchronous: under overload its buffer grows, then backpressure follows.</li></ul>`,
      `<ul><li>Пропускная способность Kafka = batching + последовательная запись + page cache + zero-copy.</li>
      <li>linger.ms и batch.size меняют задержку на пропускную способность. Сжатие работает на batch целиком.</li>
      <li>Producer асинхронный: при перегрузке растёт буфер, затем — backpressure.</li></ul>`),
  },
  {
    id: 'add-partitions', level: 2, title: T('Adding partitions breaks key routing', 'Добавление партиций ломает маршрутизацию ключей'),
    goal: T("See why you'd better not change the partition count of a keyed topic.", 'Увидеть, почему число партиций для топика с ключами лучше не менять.'),
    steps: [
      {
        text: T('Right now <code>customer-007</code> → partition by murmur2 % 6. ⚠ Adding a partition is irreversible (until <code>docker compose down -v</code>).',
          'Сейчас <code>customer-007</code> → партиция по формуле murmur2 % 6. ⚠ Добавить партицию — необратимо (до <code>docker compose down -v</code>).'),
        actions: [A(T('Where does customer-007 go?', 'Куда идёт customer-007?'), () => {
          const n = store.topic('orders')?.partitions.length ?? 6;
          toast(T(`customer-007: with ${n} partitions → P${partitionFor('customer-007', n)}, with ${n + 1} → P${partitionFor('customer-007', n + 1)}`,
            `customer-007: при ${n} партициях → P${partitionFor('customer-007', n)}, при ${n + 1} → P${partitionFor('customer-007', n + 1)}`), 'info', 9000);
        })],
      },
      {
        text: T('Add a partition to orders.', 'Добавим партицию в orders.'),
        watch: T('The groups rebalance (a new partition appeared). New orders of customer-007 may go to a different partition — while the old ones are not yet processed in the previous one. Per-customer ordering is broken during the transition.',
          'Группы получают ребаланс (появилась новая партиция). Новые заказы customer-007 могут уйти в другую партицию — а старые ещё не обработаны в прежней. Порядок по клиенту на переходном периоде нарушен.'),
        actions: [A(T('+1 orders partition', '+1 партиция orders'), async () => {
          if (demoGuard()) return;
          const n = store.topic('orders')?.partitions.length ?? 6;
          if (confirm(T(`Grow orders to ${n + 1} partitions? This is irreversible.`, `Увеличить orders до ${n + 1} партиций? Это необратимо.`))) await actions.addPartitions('orders', n + 1);
        }, 'btn-warn')],
      },
    ],
    summary: T(`<ul><li>The partition count can only grow.</li>
      <li>For keyed topics this changes routing → plan partitions with headroom from the start.</li>
      <li>If you really must — create a new topic and migrate the consumers.</li></ul>`,
      `<ul><li>Число партиций можно только увеличить.</li>
      <li>Для топиков с ключами это меняет маршрутизацию → закладывайте партиции с запасом сразу.</li>
      <li>Если всё же нужно — создают новый топик и мигрируют потребителей.</li></ul>`),
  },
];

// ------------------------------------------------------------------ UI

const LEVELS = ['', T('basics', 'база'), T('failures', 'сбои'), T('guarantees', 'гарантии')];

let done = new Set();
let current = null;
let stepIdx = 0;
let ctx = {};

function saveDone() { try { localStorage.setItem('scenarios-done', JSON.stringify([...done])); } catch { } }

export function initScenarios() {
  try { done = new Set(JSON.parse(localStorage.getItem('scenarios-done') ?? '[]')); } catch { done = new Set(); }
  $('#drawer-close').addEventListener('click', () => $('#drawer').classList.remove('open'));
}

export function openScenarios() {
  const d = $('#drawer');
  if (d.classList.contains('open') && !current) { d.classList.remove('open'); return; }
  d.classList.add('open');
  current ? renderScenario() : renderList();
}

function renderList() {
  current = null;
  $('#drawer-title').textContent = T('🎓 Scenarios', '🎓 Сценарии');
  const body = $('#drawer-body');
  body.replaceChildren(
    h('div', { class: 'small muted', style: 'margin-bottom:10px' },
      T('Each scenario is a small lab: you take a step with a button, watch the map and the event log, read the takeaways. Levels: ',
        'Каждый сценарий — маленькая лабораторная: делаешь шаг кнопкой, смотришь на схему и журнал событий, читаешь вывод. Уровни: '),
      h('span', { class: 'lvl l1' }, LEVELS[1]), ' ', h('span', { class: 'lvl l2' }, LEVELS[2]), ' ', h('span', { class: 'lvl l3' }, LEVELS[3])),
    ...SCENARIOS.map((s, i) => h('div', { class: 'sc-item' + (done.has(s.id) ? ' done' : ''), onclick: () => { current = s; stepIdx = 0; ctx = {}; renderScenario(); } },
      h('div', { class: 'sc-t' }, h('span', { class: `lvl l${s.level}` }, LEVELS[s.level]), `${i + 1}. ${s.title}`),
      h('div', { class: 'sc-d' }, s.goal))),
    h('div', { style: 'margin-top:14px;display:flex;gap:8px' },
      h('button', { class: 'btn btn-sm', onclick: resetLab, 'data-tip': T('Heal everything and restore the default service settings', 'Починить всё и вернуть настройки сервисов по умолчанию') }, T('↺ Reset the lab', '↺ Сбросить стенд')),
      h('button', { class: 'btn btn-sm', onclick: () => { done.clear(); saveDone(); renderList(); } }, T('Clear progress', 'Очистить прогресс'))));
}

function renderScenario() {
  const s = current;
  $('#drawer-title').textContent = s.title;
  const body = $('#drawer-body');
  const steps = s.steps.map((st, i) => {
    const el = h('div', { class: 'step' + (i === stepIdx ? ' current' : i < stepIdx ? ' past' : '') },
      h('div', { class: 'sn' }, T(`Step ${i + 1} of ${s.steps.length}`, `Шаг ${i + 1} из ${s.steps.length}`)),
      h('div', { html: st.text }),
      st.watch ? h('div', { class: 'watch', html: '👀 ' + st.watch }) : null,
      st.actions?.length ? h('div', { class: 'actions' }, st.actions.map(a => {
        const b = h('button', { class: `btn btn-sm ${a.kind || 'btn-primary'}` }, a.label);
        b.onclick = async () => {
          b.disabled = true;
          try { await a.run(ctx); } catch (e) { toast(e.message, 'err'); }
          b.disabled = false;
          if (stepIdx === i && i < s.steps.length - 1) { stepIdx = i + 1; renderScenario(); }
        };
        return b;
      })) : null);
    el.addEventListener('click', e => { if (!e.target.closest('button') && stepIdx !== i) { stepIdx = i; renderScenario(); } });
    return el;
  });
  const finished = stepIdx >= s.steps.length - 1;
  body.replaceChildren(...[
    h('div', {}, h('button', { class: 'btn btn-xs', onclick: renderList }, T('← all scenarios', '← все сценарии'))),
    h('div', { class: 'sc-goal', style: 'margin-top:8px', html: '🎯 ' + s.goal }),
    ...steps,
    h('div', { class: 'sc-nav' },
      h('button', { class: 'btn btn-sm', disabled: stepIdx === 0, onclick: () => { stepIdx--; renderScenario(); } }, T('← step', '← шаг')),
      finished
        ? h('button', { class: 'btn btn-sm btn-primary', onclick: () => { done.add(s.id); saveDone(); renderList(); toast(T('Scenario completed ✓', 'Сценарий пройден ✓'), 'ok'); } }, T('✓ Finish', '✓ Завершить'))
        : h('button', { class: 'btn btn-sm', onclick: () => { stepIdx++; renderScenario(); } }, T('step →', 'шаг →'))),
    finished ? h('div', { class: 'sc-explain' }, h('h4', {}, T('What we learned', 'Что мы узнали')), h('div', { html: s.summary })) : null,
  ].filter(Boolean));
  body.querySelector('.step.current')?.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
}
