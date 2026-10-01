// Пошаговые лабораторные работы. Каждая: цель → шаги с кнопками → «что наблюдать» → вывод.
import { h, $, esc, toast, partitionFor } from './util.js';
import { store } from './store.js';
import { svc, actions, demoGuard } from './api.js';
import { showSubtab } from './panels.js';

// ------------------------------------------------------------------ помощники

const sleep = ms => new Promise(r => setTimeout(r, ms));
const order = patch => svc.config('order-service', patch);
const proc = patch => svc.config('order-processor', patch);
const click = patch => svc.config('clickstream-generator', patch);
const analytics = patch => svc.config('analytics', patch);
const topicConfig = (topic, key, value) => actions.topicConfig(topic, key, value);

function controllerId() { return store.snap?.cluster?.quorum?.leaderId ?? null; }

function leadersOf(topic) {
  const counts = new Map();
  for (const p of store.topic(topic)?.partitions ?? []) if (p.leader >= 0) counts.set(p.leader, (counts.get(p.leader) ?? 0) + 1);
  return counts;
}

/** Брокер, у которого больше всего лидеров topic (по возможности — не активный контроллер). */
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
  toast('Возвращаю стенд к исходным настройкам…');
  await actions.healAll();
  await order({ ratePerSec: 20, acks: 'all', enableIdempotence: true, lingerMs: 5, compression: 'none', requestTimeoutMs: 10000, deliveryTimeoutMs: 30000, hotKeyPercent: 0, maxInFlight: 5 });
  await proc({ processingDelayMs: 5, failureRatePercent: 0, maxRetries: 2, assignmentStrategy: 'cooperative-sticky', sessionTimeoutMs: 10000, maxPollIntervalMs: 20000, autoCommitIntervalMs: 5000, staticMembership: false });
  await click({ rate: 1000, sizeBytes: 200, acks: '1', compression: 'lz4', lingerMs: 10, keyed: false });
  await analytics({ paused: false, processingDelayMs: 0 });
  await topicConfig('orders', 'min.insync.replicas', '2');
  for (const i of processorInstances().filter(i => i.state === 'stuck')) await svc.post('order-processor', `instances/${i.id}/stuck?stuck=false`);
  await setProcessorCount(2);
  toast('Стенд в исходном состоянии', 'ok');
}

// Действие внутри сценария: { label, run(ctx), kind }
const A = (label, run, kind = '') => ({ label, run, kind });

// ------------------------------------------------------------------ сценарии

const SCENARIOS = [
  {
    id: 'key-partition', level: 1, title: 'Ключ → партиция и порядок',
    goal: 'Понять, как producer выбирает партицию, почему порядок гарантирован только внутри партиции и чем опасен «горячий» ключ.',
    steps: [
      {
        text: `В карточке <b>order-service</b> есть поле ключа (по умолчанию <code>customer-007</code>). Рядом UI сам считает
          <code>murmur2(key) % 6</code> — тот же хеш, что использует Kafka. Отправь заказ с этим ключом три раза подряд.`,
        watch: 'Все три заказа попадут в одну и ту же партицию, а offset будет расти на 1. Предсказание UI совпадёт с ответом брокера.',
        actions: [A('Отправить 3 заказа customer-007', async () => {
          for (let i = 0; i < 3; i++) {
            const r = await svc.post('order-service', 'orders', { customerId: 'customer-007' });
            if (r?.ok) toast(`customer-007 → партиция ${r.partition}, offset ${r.offset}`, 'ok', 6000);
          }
        })],
      },
      {
        text: 'Теперь разные ключи. Каждый клиент «закреплён» за своей партицией — значит, его заказы обработаются строго по порядку.',
        watch: 'Разные ключи → разные (или совпадающие по модулю) партиции. Предсказание = факт.',
        actions: [A('Отправить customer-001…004', async () => {
          const n = store.topic('orders')?.partitions.length ?? 6;
          for (const k of ['customer-001', 'customer-002', 'customer-003', 'customer-004']) {
            const r = await svc.post('order-service', 'orders', { customerId: k });
            if (r?.ok) toast(`${k}: предсказано P${partitionFor(k, n)}, брокер ответил P${r.partition}`, 'ok', 7000);
          }
        })],
      },
      {
        text: `Включим «горячий ключ»: 80% заказов от <code>customer-007</code> и поднимем нагрузку до 200/с.`,
        watch: 'Чип лидера одной партиции orders начнёт светиться (горячая партиция). В таблице «Партиции» её скорость будет в разы выше остальных, а у консюмера, которому она досталась, вырастет lag — остальные при этом простаивают.',
        actions: [A('Горячий ключ 80%, 200/с', () => order({ hotKeyPercent: 80, ratePerSec: 200 })), A('Открыть таблицу партиций', () => showSubtab('partitions'))],
      },
      {
        text: 'Вернём нагрузку. Подумай: как бы ты разгрузил горячий ключ? (Подсказка: составной ключ customerId+orderId теряет порядок для клиента — это компромисс.)',
        actions: [A('Сбросить: 0%, 20/с', () => order({ hotKeyPercent: 0, ratePerSec: 20 }))],
      },
    ],
    summary: `<ul><li>Партиция = <code>murmur2(key) % N</code>. Один ключ — всегда одна партиция (пока N не меняется).</li>
      <li>Порядок гарантирован только <b>внутри партиции</b>. Нужен порядок по сущности — используй её id как ключ.</li>
      <li>Без ключа — sticky partitioner: batch целиком в одну партицию, потом следующая.</li>
      <li>Неравномерные ключи → горячие партиции → один консюмер перегружен, масштабирование не помогает.</li>
      <li>librdkafka по умолчанию использует CRC32 (<code>consistent_random</code>), а не murmur2 — для совместимости с Java ставят <code>partitioner=murmur2_random</code>.</li></ul>`,
  },
  {
    id: 'rebalance', level: 1, title: 'Consumer group: масштабирование и ребаланс',
    goal: 'Увидеть, как партиции делятся между консюмерами, что происходит при входе/выходе участника и чем eager отличается от cooperative.',
    steps: [
      {
        text: 'Посмотри на группу <b>order-processing</b> справа: у каждого консюмера свой набор партиций orders (o0…o5). Одна партиция — одному консюмеру группы.',
        watch: 'Каждая из 6 партиций принадлежит ровно одному участнику.',
      },
      {
        text: 'Добавим третий консюмер.',
        watch: 'В журнале: «вступили processor-3», состояние группы PreparingRebalance → Stable. При cooperative-sticky у старых консюмеров забирают только часть партиций — остальные читаются без остановки.',
        actions: [A('+1 консюмер', () => svc.post('order-processor', 'instances'))],
      },
      {
        text: 'Доведём число консюмеров до 7 — больше, чем партиций.',
        watch: 'Седьмой консюмер получит «нет партиций — простаивает». Параллелизм группы ограничен числом партиций!',
        actions: [A('Сделать 7 консюмеров', () => setProcessorCount(7))],
      },
      {
        text: 'Переключим стратегию на <b>range</b> (eager-протокол). Все консюмеры перезапустятся.',
        watch: 'При eager-ребалансе каждый участник сначала отдаёт ВСЕ свои партиции («отозваны …»), и только потом получает новые — stop-the-world. Сравни с cooperative-sticky.',
        actions: [A('Стратегия range', () => proc({ assignmentStrategy: 'range' }))],
      },
      {
        text: 'Уберём 4 консюмера корректно (Close → LeaveGroup).',
        watch: 'Ребаланс происходит сразу, без ожидания таймаутов — потому что консюмеры попрощались.',
        actions: [A('Оставить 3', () => setProcessorCount(3))],
      },
      {
        text: 'Попробуй новый протокол <b>KIP-848</b> (group.protocol=consumer): назначение партиций считает брокер-координатор, клиенты получают изменения инкрементально.',
        watch: 'В карточке группы тип станет Consumer, assignor — uniform/range (серверный). Добавь/убери консюмер и посмотри, как быстро стабилизируется группа.',
        actions: [A('KIP-848', () => proc({ assignmentStrategy: 'consumer' })), A('Вернуть cooperative-sticky', () => proc({ assignmentStrategy: 'cooperative-sticky' })), A('Оставить 2 консюмера', () => setProcessorCount(2))],
      },
    ],
    summary: `<ul><li>Внутри группы партиция читается одним консюмером → консюмеров больше партиций держать бессмысленно.</li>
      <li>Ребаланс запускается при входе/выходе/смерти участника, изменении подписки или числа партиций.</li>
      <li>Eager (range/roundrobin) — все отдают всё; cooperative-sticky — двигается минимум; KIP-848 — ребаланс на стороне брокера.</li>
      <li>Корректный Close() ускоряет ребаланс: не нужно ждать session.timeout.</li></ul>`,
  },
  {
    id: 'two-groups', level: 1, title: 'Две группы читают один топик',
    goal: 'Понять модель pub/sub в Kafka: каждая consumer group получает все сообщения и хранит свои offset-ы. Перечитать историю.',
    steps: [
      {
        text: 'Топик orders читают две группы: <b>order-processing</b> (C#) и <b>analytics</b> (Python). Открой таблицу партиций: у каждой партиции два столбца lag — по одному на группу.',
        actions: [A('Таблица партиций', () => showSubtab('partitions'))],
      },
      {
        text: 'Поставим analytics на паузу — <code>consumer.pause()</code>.',
        watch: 'Lag analytics растёт, а order-processing продолжает работать как ни в чём не бывало. Участники analytics остаются в группе — партиции за ними.',
        actions: [A('pause() analytics', () => analytics({ paused: true }))],
      },
      {
        text: 'Снимем паузу.',
        watch: 'Analytics догоняет поток: скорость чтения резко выше скорости записи, пока lag не уйдёт в 0.',
        actions: [A('resume()', () => analytics({ paused: false }))],
      },
      {
        text: 'Kafka не удаляет прочитанное. Перечитаем всю историю: остановить analytics → <code>kafka-consumer-groups.sh --reset-offsets --to-earliest --execute</code> → запустить.',
        watch: 'Lag analytics подскочит до размера всех топиков, затем рассосётся. order-processing это не затронет.',
        actions: [A('⏪ Перечитать историю', () => $('.card.group[data-group="analytics"] .btn-warn')?.click())],
      },
    ],
    summary: `<ul><li>Топик ≠ очередь: сообщения не удаляются после чтения, а живут до retention.</li>
      <li>Разные группы = независимые подписчики со своими offset-ами в __consumer_offsets.</li>
      <li>Offset-ы можно перемотать (reset-offsets) — только у неактивной группы.</li>
      <li>pause()/resume() — способ притормозить чтение, не выходя из группы.</li></ul>`,
  },
  {
    id: 'lag', level: 1, title: 'Медленный консюмер и lag',
    goal: 'Увидеть, как растёт lag, и почему масштабирование консюмеров упирается в число партиций.',
    steps: [
      {
        text: 'Поднимем запись до 500 заказов/с. Два консюмера с обработкой 5 мс тянут ~400/с.',
        watch: 'Lag order-processing начинает расти (график в карточке группы).',
        actions: [A('500 заказов/с', () => order({ ratePerSec: 500 }))],
      },
      {
        text: 'Масштабируем группу до 6 консюмеров.',
        watch: 'После ребаланса lag начнёт уменьшаться: 6 × 200/с > 500/с.',
        actions: [A('6 консюмеров', () => setProcessorCount(6))],
      },
      {
        text: 'Теперь обработка стала медленнее — 20 мс на заказ: 6 × 50 = 300/с < 500/с.',
        watch: 'Lag снова растёт. Добавить седьмой консюмер не поможет — у orders всего 6 партиций.',
        actions: [A('Обработка 20 мс', () => proc({ processingDelayMs: 20 })), A('+1 консюмер (бесполезно)', () => svc.post('order-processor', 'instances'))],
      },
      {
        text: 'Вернём всё назад.',
        actions: [A('Сбросить', async () => { await order({ ratePerSec: 20 }); await proc({ processingDelayMs: 5 }); await setProcessorCount(2); })],
      },
    ],
    summary: `<ul><li>Lag = high watermark − committed offset. Главная метрика консюмеров.</li>
      <li>Пропускная способность группы ≈ min(консюмеры, партиции) × скорость одного консюмера.</li>
      <li>Партиции — единица параллелизма. Их число закладывают с запасом.</li>
      <li>Другие рычаги: батчевая обработка, асинхронная обработка внутри консюмера, оптимизация downstream.</li></ul>`,
  },
  {
    id: 'broker-failure', level: 2, title: 'Падение брокера: graceful vs kill',
    goal: 'Увидеть выборы лидера, сжатие ISR, under-replicated partitions и разницу между корректной остановкой и крэшем.',
    steps: [
      {
        text: 'Остановим корректно (SIGTERM) брокер с наибольшим числом лидеров orders.',
        watch: 'Controlled shutdown: лидерство переезжает почти мгновенно, затем брокер исчезает. ISR партиций сжимается, в шапке растёт URP. Producer продолжает писать: при RF=3 и min.insync.replicas=2 двух реплик достаточно.',
        actions: [A('Stop самого нагруженного', async ctx => { ctx.b1 = pickBroker('orders', false); toast(`Останавливаю broker ${ctx.b1}`); await actions.broker(ctx.b1, 'stop'); })],
      },
      {
        text: 'Запустим его обратно.',
        watch: 'Брокер стартует, догоняет лидеров и возвращается в ISR (журнал: «вернулись в ISR»). Лидерство вернётся к нему в течение ~30 с (auto.leader.rebalance) или сразу по кнопке ⚖.',
        actions: [A('Start', ctx => actions.broker(ctx.b1 ?? pickBroker(), 'start')), A('⚖ Предпочтительные лидеры', () => actions.preferred())],
      },
      {
        text: 'Теперь «убьём» (SIGKILL) другой брокер — без controlled shutdown.',
        watch: 'Около 9 секунд (broker.session.timeout.ms) кластер не знает, что брокер мёртв: партиции, где он был лидером, недоступны — p99 у order-service подскакивает. Потом фенсинг и выборы лидера.',
        actions: [A('Kill другого брокера', async ctx => { ctx.b2 = pickBroker('orders', true); if (ctx.b2 === ctx.b1) ctx.b2 = (ctx.b1 % 3) + 1; toast(`Убиваю broker ${ctx.b2}`); await actions.broker(ctx.b2, 'kill'); })],
      },
      {
        text: 'Поднимем его.',
        actions: [A('Start', ctx => actions.broker(ctx.b2 ?? 1, 'start'))],
      },
    ],
    summary: `<ul><li>Лидер выбирается только из ISR — подтверждённые данные не теряются.</li>
      <li>Graceful stop: лидерство передаётся заранее, клиенты почти не замечают. Kill: ждём таймаут heartbeat.</li>
      <li>URP (under-replicated partitions) — главный алерт: запас прочности снижен.</li>
      <li>Вернувшийся брокер сначала догоняет лог (fetch), потом возвращается в ISR, потом — лидерство.</li></ul>`,
  },
  {
    id: 'min-isr', level: 2, title: 'acks=all, min.insync.replicas и отказ в записи',
    goal: 'Понять, когда Kafka отказывает в записи ради надёжности и как acks=1 обходит эту защиту.',
    steps: [
      {
        text: 'Сделаем требование строже: для orders <code>min.insync.replicas=3</code> — запись с acks=all требует ВСЕ три реплики. order-service работает с acks=all.',
        actions: [A('min.insync.replicas=3', () => topicConfig('orders', 'min.insync.replicas', '3')), A('acks=all, 50/с', () => order({ acks: 'all', enableIdempotence: true, ratePerSec: 50 }))],
      },
      {
        text: 'Остановим один брокер.',
        watch: 'В шапке ISR<min > 0, в журнале «ISR < min.insync.replicas». Producer получает NOT_ENOUGH_REPLICAS и ретраит: подтверждения прекращаются, «в полёте» растёт, через delivery.timeout (30 с) — ошибки Local_MsgTimedOut. Kafka честно отказывает, а не теряет.',
        actions: [A('Stop брокера', async ctx => { ctx.b = pickBroker('orders', true); toast(`Останавливаю broker ${ctx.b}`); await actions.broker(ctx.b, 'stop'); })],
      },
      {
        text: 'Переключим producer на acks=1.',
        watch: 'Запись мгновенно возобновилась: лидеру не нужно ждать реплики. Удобно, но такие записи могут потеряться при смене лидера (см. сценарий «Зомби-лидер»).',
        actions: [A('acks=1', () => order({ acks: '1', enableIdempotence: false }))],
      },
      {
        text: 'Вернём всё как было.',
        actions: [A('Восстановить', async ctx => { await actions.broker(ctx.b ?? 1, 'start'); await topicConfig('orders', 'min.insync.replicas', '2'); await order({ acks: 'all', enableIdempotence: true, ratePerSec: 20 }); })],
      },
    ],
    summary: `<ul><li>acks=all + min.insync.replicas=N: подтверждение только если запись есть минимум на N репликах.</li>
      <li>Типичный выбор: RF=3, min.insync.replicas=2 — переживаем потерю одного брокера без остановки записи.</li>
      <li>min.insync.replicas=RF — любая потеря брокера останавливает запись.</li>
      <li>acks=1 игнорирует min.insync.replicas: доступность выше, гарантий меньше.</li></ul>`,
  },
  {
    id: 'zombie', level: 3, title: 'Зомби-лидер: как acks=1 теряет данные',
    goal: 'Воспроизвести настоящую потерю подтверждённых записей и увидеть, как acks=all её предотвращает.',
    steps: [
      {
        text: 'Переключим order-service на <code>acks=1</code> без идемпотентности, 100 заказов/с. Следи за блоком «🔍 Аудит доставки» — сейчас там ПОТЕРЯНО 0.',
        actions: [A('acks=1, 100/с', () => order({ acks: '1', enableIdempotence: false, ratePerSec: 100 }))],
      },
      {
        text: `Отрежем от кластера брокер, который лидирует в части партиций orders: он не видит других брокеров и контроллер,
          но <b>клиенты по-прежнему до него достают</b>.`,
        watch: 'Producer продолжает получать подтверждения от «зомби». Через ~9 с контроллер зафенсит брокер и выберет новых лидеров из ISR — у них нет записей, принятых зомби. Подожди 20–30 секунд.',
        actions: [A('🔌 Split brain', async ctx => { ctx.b = pickBroker('orders', true); toast(`Отрезаю broker ${ctx.b} от других брокеров`); await actions.network(ctx.b, { latencyMs: 0, jitterMs: 0, lossPct: 0, isolated: false, splitFromBrokers: true }); })],
      },
      {
        text: 'Вернём сеть.',
        watch: 'Брокер видит новый leader epoch и ОБРЕЗАЕТ свой лог до точки расхождения. Аудит доставки найдёт заказы, которые были подтверждены, но исчезли: «ПОТЕРЯНО N» и примеры «o-123 @ orders-2:456 → там теперь o-789».',
        actions: [A('Вернуть сеть', ctx => actions.network(ctx.b ?? 1, { latencyMs: 0, jitterMs: 0, lossPct: 0, isolated: false, splitFromBrokers: false }))],
      },
      {
        text: 'Повторим с <code>acks=all</code> + идемпотентность.',
        watch: 'Теперь зомби не может собрать ISR и не подтверждает записи: они висят «в полёте», затем уходят новым лидерам или падают по таймауту. Счётчик ПОТЕРЯНО не растёт.',
        actions: [A('acks=all', () => order({ acks: 'all', enableIdempotence: true })),
          A('🔌 Split brain', async ctx => { ctx.b = pickBroker('orders', true); await actions.network(ctx.b, { latencyMs: 0, jitterMs: 0, lossPct: 0, isolated: false, splitFromBrokers: true }); }),
          A('Вернуть сеть', ctx => actions.network(ctx.b ?? 1, { latencyMs: 0, jitterMs: 0, lossPct: 0, isolated: false, splitFromBrokers: false }))],
      },
      { text: 'Вернём нагрузку.', actions: [A('20/с', () => order({ ratePerSec: 20 }))] },
    ],
    summary: `<ul><li>acks=1 подтверждает запись, которая есть только у лидера. Сменился лидер — запись может исчезнуть.</li>
      <li>После разделения сети старый лидер обрезает лог по leader epoch (KIP-101), «подтверждённые» данные пропадают.</li>
      <li>acks=all + min.insync.replicas≥2 + unclean.leader.election.enable=false = подтверждённое не теряется.</li>
      <li>Это не теория: так теряли данные в реальных инцидентах при сетевых разделениях.</li></ul>`,
  },
  {
    id: 'duplicates', level: 3, title: 'Сетевая задержка, таймауты и дубли',
    goal: 'Получить дубликаты в логе из-за ретраев и убрать их идемпотентным producer-ом.',
    steps: [
      {
        text: 'Настроим «агрессивный» producer: acks=all, идемпотентность <b>выключена</b>, request.timeout = 1 с.',
        actions: [A('Без идемпотентности, timeout 1 с', () => order({ acks: 'all', enableIdempotence: false, requestTimeoutMs: 1000, deliveryTimeoutMs: 30000, ratePerSec: 50 }))],
      },
      {
        text: 'Добавим брокеру-лидеру части партиций orders задержку 400±300 мс: часть ответов будет успевать за 1 с, часть — нет.',
        watch: 'Брокер успевает записать batch, но ответ приходит позже таймаута. Producer считает запрос неудачным и шлёт batch снова → в логе дубли («дубли в логе» в аудите, события «Дубликат в логе»). Подожди 20–30 секунд.',
        actions: [A('🐢 Задержка 400±300 мс', async ctx => { ctx.b = pickBroker('orders', true); toast(`Задержка на broker ${ctx.b}`); await actions.network(ctx.b, { latencyMs: 400, jitterMs: 300, lossPct: 0, isolated: false, splitFromBrokers: false }); })],
      },
      {
        text: 'Включим идемпотентность.',
        watch: 'Ретраи остаются, но брокер узнаёт повторный batch по (ProducerId, epoch, sequence) и не пишет его второй раз. Первые ~10 с ещё досчитываются дубли старого producer-а, потом рост почти останавливается. Почти — потому что при массовых таймаутах librdkafka «перезапускает» эпоху producer-а (KIP-360), и редкий повтор с новыми номерами брокер уже не узнаёт. Вывод: идемпотентный producer резко снижает дубли, но идемпотентная обработка у консюмера всё равно нужна.',
        actions: [A('Идемпотентность ON', () => order({ enableIdempotence: true }))],
      },
      {
        text: 'Вернём сеть и таймауты.',
        actions: [A('Восстановить', async ctx => { await actions.network(ctx.b ?? 1, { latencyMs: 0, jitterMs: 0, lossPct: 0, isolated: false, splitFromBrokers: false }); await order({ requestTimeoutMs: 10000, ratePerSec: 20 }); })],
      },
    ],
    summary: `<ul><li>Таймаут ≠ ошибка записи: запрос мог выполниться, а ответ — потеряться или опоздать.</li>
      <li>Ретраи без идемпотентности = дубли (и даже нарушение порядка при max.in.flight > 1).</li>
      <li>enable.idempotence=true решает это в пределах одной сессии (и эпохи) producer-а; после бампа эпохи из-за таймаутов редкие дубли возможны.</li>
      <li>Таймаут меньше реальной задержки сети = ретраи, дубли и даже фатальные ошибки идемпотентного producer-а. Таймауты должны быть с запасом.</li>
      <li>Для «exactly-once» между топиками — транзакции, для побочных эффектов — идемпотентный консюмер.</li></ul>`,
  },
  {
    id: 'consumer-crash', level: 2, title: 'Крэш консюмера, дубли и static membership',
    goal: 'Разобраться с session.timeout, max.poll.interval, at-least-once и static membership.',
    steps: [
      {
        text: 'Поднимем нагрузку до 200/с и «уроним» первый консюмер (💥 = Dispose без Close).',
        watch: 'Участник остаётся в группе «призраком» ~10 с (session.timeout.ms) — его партиции никто не читает, lag на них растёт. Затем ребаланс, а новый владелец перечитывает сообщения с последнего коммита → «дубли» в карточке группы.',
        actions: [A('200/с', () => order({ ratePerSec: 200 })), A('💥 Крэш консюмера', () => { const i = processorInstances()[0]; return i && svc.post('order-processor', `instances/${i.id}/crash`); }), A('+1 консюмер', () => svc.post('order-processor', 'instances'))],
      },
      {
        text: 'Уменьшим окно дублей: auto.commit.interval = 100 мс — и снова уроним консюмер.',
        watch: 'Дублей после крэша заметно меньше: offset-ы коммитятся чаще. Но совсем дубли не исчезнут — нужна идемпотентная обработка.',
        actions: [A('auto.commit 100 мс', () => proc({ autoCommitIntervalMs: 100 })), A('💥 Крэш', () => { const i = processorInstances()[0]; return i && svc.post('order-processor', `instances/${i.id}/crash`); }), A('+1 консюмер', () => svc.post('order-processor', 'instances'))],
      },
      {
        text: 'Включим static membership и сделаем быстрый рестарт (🔄: крэш + запуск с тем же id через 2 с).',
        watch: 'Группа остаётся Stable — координатор узнаёт участника по group.instance.id и отдаёт ему те же партиции. Без static membership было бы два ребаланса.',
        actions: [A('static membership', () => proc({ staticMembership: true })), A('🔄 Рестарт консюмера', () => { const i = processorInstances()[0]; return i && svc.post('order-processor', `instances/${i.id}/restart`); })],
      },
      {
        text: 'Теперь «зависание»: консюмер жив, heartbeat-ы идут, но Consume() не вызывается.',
        watch: 'Через max.poll.interval.ms (20 с) консюмер сам покинет группу: «партиции ПОТЕРЯНЫ». Нажми 🧊 ещё раз — он вернётся.',
        actions: [A('🧊 Зависнуть', () => { const i = processorInstances()[0]; return i && svc.post('order-processor', `instances/${i.id}/stuck?stuck=true`); }),
          A('Отвиснуть', () => { const i = processorInstances().find(x => x.state === 'stuck'); return i && svc.post('order-processor', `instances/${i.id}/stuck?stuck=false`); })],
      },
      { text: 'Вернём настройки.', actions: [A('Сбросить', async () => { await proc({ staticMembership: false, autoCommitIntervalMs: 5000 }); await order({ ratePerSec: 20 }); await setProcessorCount(2); })] },
    ],
    summary: `<ul><li>session.timeout — «процесс/сеть мертвы» (нет heartbeat). max.poll.interval — «приложение зависло» (нет poll).</li>
      <li>Коммит после обработки = at-least-once: после крэша часть сообщений обработается повторно.</li>
      <li>Обработка должна быть идемпотентной; частота коммитов лишь уменьшает окно дублей.</li>
      <li>Static membership убирает лишние ребалансы при быстрых рестартах.</li></ul>`,
  },
  {
    id: 'kraft', level: 2, title: 'KRaft: потеря активного контроллера',
    goal: 'Увидеть, как кворум контроллеров переживает потерю лидера и почему нужно большинство.',
    steps: [
      {
        text: 'В шапке видно, какой узел сейчас активный контроллер (★ в карточке брокера). Полностью изолируем его сеть.',
        watch: 'Через несколько секунд оставшиеся два контроллера выберут нового лидера (журнал: «KRaft: активный контроллер сменился»). Затем новый контроллер зафенсит изолированный брокер и перевыберет лидеров его партиций.',
        actions: [A('✂ Изолировать контроллер', async ctx => { ctx.c = controllerId() ?? 1; toast(`Изолирую контроллер — узел ${ctx.c}`); await actions.network(ctx.c, { latencyMs: 0, jitterMs: 0, lossPct: 0, isolated: true, splitFromBrokers: false }); })],
      },
      {
        text: 'Вернём сеть.',
        watch: 'Узел возвращается обычным follower-ом кворума, догоняет лог метаданных и регистрируется как брокер.',
        actions: [A('Вернуть сеть', ctx => actions.network(ctx.c ?? 1, { latencyMs: 0, jitterMs: 0, lossPct: 0, isolated: false, splitFromBrokers: false }))],
      },
      {
        text: '⚠ Опционально: остановим два узла из трёх. Это потеря кворума: нет большинства → нет активного контроллера.',
        watch: 'Кворум не отвечает, метаданные «замерли»: нельзя выбрать лидеров, создать топик, зафенсить брокер. Оставшийся брокер обслуживает только те партиции, где он уже лидер (и только acks≤1).',
        actions: [A('Stop двух узлов', async ctx => { const ids = [1, 2, 3].filter(i => i !== (controllerId() ?? 1)); ctx.two = ids; for (const i of ids) await actions.broker(i, 'stop'); }),
          A('Запустить обратно', async ctx => { for (const i of ctx.two ?? [1, 2, 3]) await actions.broker(i, 'start'); })],
      },
    ],
    summary: `<ul><li>Метаданные кластера — это тоже лог (__cluster_metadata), реплицируемый по Raft между контроллерами.</li>
      <li>Нужно большинство: 3 контроллера переживают потерю 1, 5 — потерю 2.</li>
      <li>В production контроллеры обычно выделяют на отдельные узлы (process.roles=controller).</li></ul>`,
  },
  {
    id: 'pause', level: 2, title: 'Пауза брокера ≈ долгая GC-пауза',
    goal: 'Понять, почему короткие паузы безопасны, а длинные вызывают фенсинг и переезд лидеров.',
    steps: [
      {
        text: 'Заморозим брокер на 4 секунды — меньше, чем broker.session.timeout.ms (9 с).',
        watch: 'Задержка подтверждений (p99) подскочит, но ни фенсинга, ни выборов лидера не будет.',
        actions: [A('Пауза 4 с', async ctx => { ctx.b = pickBroker('orders', true); toast(`Пауза broker ${ctx.b} на 4 с`); await actions.broker(ctx.b, 'pause'); await sleep(4000); await actions.broker(ctx.b, 'unpause'); })],
      },
      {
        text: 'Теперь 20 секунд.',
        watch: 'Через ~9 с — «Контроллер зафенсил брокер», выборы лидеров, ISR сжимается. После разморозки брокер обнаруживает, что больше не лидер, и догоняет.',
        actions: [A('Пауза 20 с', async ctx => { ctx.b = pickBroker('orders', true); toast(`Пауза broker ${ctx.b} на 20 с`); await actions.broker(ctx.b, 'pause'); await sleep(20000); await actions.broker(ctx.b, 'unpause'); toast('Брокер разморожен', 'ok'); })],
      },
    ],
    summary: `<ul><li>Таймауты — компромисс: короткие быстрее обнаруживают сбой, но ложно срабатывают на GC-паузах.</li>
      <li>Длинные GC-паузы у брокера = «мигание» лидерства. Мониторь GC и размер heap.</li></ul>`,
  },
  {
    id: 'dlq', level: 1, title: 'Poison pill и Dead Letter Queue',
    goal: 'Понять, что делать с сообщением, которое невозможно обработать.',
    steps: [
      {
        text: 'Отправим в orders битый JSON («poison pill»).',
        watch: 'order-processor не сможет его разобрать и сразу отправит в orders.dlq (журнал: «poison pill → orders.dlq»). Без DLQ консюмер либо застрял бы на этом offset навсегда, либо молча потерял сообщение.',
        actions: [A('☠ Poison pill', () => actions.produce({ topic: 'orders', key: 'customer-013', value: '{"orderId": oops, not json', note: 'dlq' }))],
      },
      {
        text: 'Посмотрим DLQ: открой «Сообщения», топик orders.dlq — в заголовках причина и координаты оригинала.',
        actions: [A('Открыть «Сообщения»', () => showSubtab('messages'))],
      },
      {
        text: 'Теперь 10% «ошибок обработки»: каждая ошибка → ретраи (до 2) → DLQ.',
        watch: 'Растут счётчики «ретраи» и «DLQ», топик orders.dlq пишется (красный поток частиц).',
        actions: [A('10% ошибок', () => proc({ failureRatePercent: 10 })), A('Вернуть 0%', () => proc({ failureRatePercent: 0 }))],
      },
    ],
    summary: `<ul><li>Нельзя бесконечно ретраить одно сообщение — партиция встанет.</li>
      <li>Паттерн: N ретраев (иногда через retry-топики с задержкой) → DLQ с причиной в заголовках → разбор и повторная отправка.</li>
      <li>В Kafka нет встроенной DLQ (в отличие от RabbitMQ) — её реализует приложение или фреймворк.</li></ul>`,
  },
  {
    id: 'compaction', level: 2, title: 'Compaction: топик как таблица',
    goal: 'Увидеть, как compacted-топик хранит только последнее значение по каждому ключу.',
    steps: [
      {
        text: 'order-service при каждом заказе пишет профиль клиента в <code>customer-profiles</code> (cleanup.policy=compact). Ключей всего 50, а сообщений — тысячи. Открой «Сообщения» → customer-profiles.',
        actions: [A('Открыть «Сообщения»', () => showSubtab('messages'))],
      },
      {
        text: 'Отправим tombstone (value = null) для customer-007.',
        watch: 'Это «удаление» ключа: после очередной компакции все значения customer-007 исчезнут (сам tombstone хранится ещё delete.retention.ms).',
        actions: [A('🪦 Tombstone customer-007', () => actions.produce({ topic: 'customer-profiles', key: 'customer-007', value: null, note: 'compaction' }))],
      },
      {
        text: 'Подожди 1–2 минуты (segment.ms=60 с: компактятся только закрытые сегменты) и прочитай партицию снова.',
        watch: 'Начало лога сдвинется, в старой части останутся «дырки» в offset-ах: на каждый ключ — одно последнее значение. Активный сегмент не компактится никогда.',
      },
    ],
    summary: `<ul><li>compact = «последнее значение по ключу», delete = «всё за период retention».</li>
      <li>Tombstone (null) удаляет ключ. Так устроены __consumer_offsets, changelog-и Kafka Streams, KTable.</li>
      <li>Offset-ы не перенумеровываются — после компакции в них дырки.</li></ul>`,
  },
  {
    id: 'load', level: 2, title: 'Нагрузка: batching, сжатие, backpressure',
    goal: 'Почувствовать, как linger, compression и размер сообщений влияют на пропускную способность.',
    steps: [
      {
        text: 'Разгоним clickstream до 20 000 сообщений/с без batching и сжатия.',
        watch: 'Смотри «трафик» и p99 в карточке Go-генератора, скорость записи в шапке и lag analytics (Python может не успевать).',
        actions: [A('20k/с, linger 0, none', () => click({ rate: 20000, lingerMs: 0, compression: 'none' }))],
      },
      {
        text: 'Включим linger 50 мс и сжатие zstd.',
        watch: 'Сообщения собираются в крупные batch-и: меньше запросов, меньше байт по сети и на диске. Задержка подтверждения вырастет примерно на linger.',
        actions: [A('linger 50, zstd', () => click({ lingerMs: 50, compression: 'zstd' }))],
      },
      {
        text: 'Крупные сообщения: 5 KB и 50 000/с. Возможно, кластер не справится.',
        watch: 'Если брокеры не успевают — буфер producer-а заполняется и включается backpressure. Analytics, скорее всего, отстанет: масштабируй её командой <code>docker compose up -d --scale analytics=3</code>.',
        actions: [A('5 KB × 50k/с', () => click({ rate: 50000, sizeBytes: 5000 }))],
      },
      { text: 'Вернём нормальную нагрузку.', actions: [A('1k/с, 200 B, lz4', () => click({ rate: 1000, sizeBytes: 200, lingerMs: 10, compression: 'lz4' }))] },
    ],
    summary: `<ul><li>Пропускная способность Kafka = batching + последовательная запись + page cache + zero-copy.</li>
      <li>linger.ms и batch.size меняют задержку на пропускную способность. Сжатие работает на batch целиком.</li>
      <li>Producer асинхронный: при перегрузке растёт буфер, затем — backpressure.</li></ul>`,
  },
  {
    id: 'add-partitions', level: 2, title: 'Добавление партиций ломает маршрутизацию ключей',
    goal: 'Увидеть, почему число партиций для топика с ключами лучше не менять.',
    steps: [
      {
        text: 'Сейчас <code>customer-007</code> → партиция по формуле murmur2 % 6. ⚠ Добавить партицию — необратимо (до <code>docker compose down -v</code>).',
        actions: [A('Куда идёт customer-007?', () => {
          const n = store.topic('orders')?.partitions.length ?? 6;
          toast(`customer-007: при ${n} партициях → P${partitionFor('customer-007', n)}, при ${n + 1} → P${partitionFor('customer-007', n + 1)}`, 'info', 9000);
        })],
      },
      {
        text: 'Добавим партицию в orders.',
        watch: 'Группы получают ребаланс (появилась новая партиция). Новые заказы customer-007 могут уйти в другую партицию — а старые ещё не обработаны в прежней. Порядок по клиенту на переходном периоде нарушен.',
        actions: [A('+1 партиция orders', async () => {
          if (demoGuard()) return;
          const n = store.topic('orders')?.partitions.length ?? 6;
          if (confirm(`Увеличить orders до ${n + 1} партиций? Это необратимо.`)) await actions.addPartitions('orders', n + 1);
        }, 'btn-warn')],
      },
    ],
    summary: `<ul><li>Число партиций можно только увеличить.</li>
      <li>Для топиков с ключами это меняет маршрутизацию → закладывайте партиции с запасом сразу.</li>
      <li>Если всё же нужно — создают новый топик и мигрируют потребителей.</li></ul>`,
  },
];

// ------------------------------------------------------------------ UI

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
  $('#drawer-title').textContent = '🎓 Сценарии';
  const body = $('#drawer-body');
  body.replaceChildren(
    h('div', { class: 'small muted', style: 'margin-bottom:10px' },
      'Каждый сценарий — маленькая лабораторная: делаешь шаг кнопкой, смотришь на схему и журнал событий, читаешь вывод. Уровни: ',
      h('span', { class: 'lvl l1' }, 'база'), ' ', h('span', { class: 'lvl l2' }, 'сбои'), ' ', h('span', { class: 'lvl l3' }, 'гарантии')),
    ...SCENARIOS.map((s, i) => h('div', { class: 'sc-item' + (done.has(s.id) ? ' done' : ''), onclick: () => { current = s; stepIdx = 0; ctx = {}; renderScenario(); } },
      h('div', { class: 'sc-t' }, h('span', { class: `lvl l${s.level}` }, ['', 'база', 'сбои', 'гарантии'][s.level]), `${i + 1}. ${s.title}`),
      h('div', { class: 'sc-d' }, s.goal))),
    h('div', { style: 'margin-top:14px;display:flex;gap:8px' },
      h('button', { class: 'btn btn-sm', onclick: resetLab, 'data-tip': 'Починить всё и вернуть настройки сервисов по умолчанию' }, '↺ Сбросить стенд'),
      h('button', { class: 'btn btn-sm', onclick: () => { done.clear(); saveDone(); renderList(); } }, 'Очистить прогресс')));
}

function renderScenario() {
  const s = current;
  $('#drawer-title').textContent = s.title;
  const body = $('#drawer-body');
  const steps = s.steps.map((st, i) => {
    const el = h('div', { class: 'step' + (i === stepIdx ? ' current' : i < stepIdx ? ' past' : '') },
      h('div', { class: 'sn' }, `Шаг ${i + 1} из ${s.steps.length}`),
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
    h('div', {}, h('button', { class: 'btn btn-xs', onclick: renderList }, '← все сценарии')),
    h('div', { class: 'sc-goal', style: 'margin-top:8px', html: '🎯 ' + s.goal }),
    ...steps,
    h('div', { class: 'sc-nav' },
      h('button', { class: 'btn btn-sm', disabled: stepIdx === 0, onclick: () => { stepIdx--; renderScenario(); } }, '← шаг'),
      finished
        ? h('button', { class: 'btn btn-sm btn-primary', onclick: () => { done.add(s.id); saveDone(); renderList(); toast('Сценарий пройден ✓', 'ok'); } }, '✓ Завершить')
        : h('button', { class: 'btn btn-sm', onclick: () => { stepIdx++; renderScenario(); } }, 'шаг →')),
    finished ? h('div', { class: 'sc-explain' }, h('h4', {}, 'Что мы узнали'), h('div', { html: s.summary })) : null,
  ].filter(Boolean));
  body.querySelector('.step.current')?.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
}
