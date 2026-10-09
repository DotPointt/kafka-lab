// The "Trace" panel: follow ONE business event (an order) through every service by its correlation id.
// Each service remembers what it did with the event; control-center stitches the steps together and checks
// that the business state changed exactly once in every service — not lost, not duplicated.
import { h, $, fmtTime, toast } from './util.js';
import { api, svc, demoGuard, DEMO } from './api.js';
import { T } from './i18n.js';
import { showSubtab } from './panels.js';

const STAGES = {
  produce: { icon: '📤', title: T('Publish', 'Публикация'), what: T('order → orders', 'заказ → orders') },
  log: { icon: '🗄', title: T('Kafka log', 'Лог Kafka'), what: T('stored in a partition', 'лежит в партиции') },
  process: { icon: '⚙', title: T('Processing', 'Обработка'), what: T('order received', 'заказ получен') },
  payment: { icon: '💳', title: T('Payment', 'Платёж'), what: T('payment → payments', 'платёж → payments') },
  'analytics-order': { icon: '📊', title: T('Report: order', 'Отчёт: заказ'), what: T('order counted', 'заказ учтён') },
  'analytics-payment': { icon: '📊', title: T('Report: payment', 'Отчёт: платёж'), what: T('payment counted', 'платёж учтён') },
};

const STATUS = {
  ok: ['✓', T('exactly once', 'ровно один раз')],
  duplicate: ['⚠', T('DUPLICATE', 'ДУБЛЬ')],
  lost: ['✗', T('lost', 'потеряно')],
  failed: ['✗', T('failed', 'ошибка')],
  pending: ['…', T('waiting', 'ждём')],
  missing: ['⌛', T('not happening (30 s+)', 'не наступает (30 с+)')],
  skipped: ['–', T('not needed', 'не нужно')],
  unverified: ['?', T('not audited', 'аудитор не видел')],
};

const VERDICT = {
  ok: ['ok', '✓', T('Exactly once', 'Ровно один раз'),
    T('Every service changed the business state exactly once.', 'Каждый сервис сменил бизнес-состояние ровно один раз.')],
  'in-progress': ['info', '⏳', T('In progress', 'В процессе'),
    T('The event is still moving through the pipeline.', 'Событие ещё идёт по цепочке.')],
  duplicate: ['warn', '⚠', T('Duplicated', 'Задвоилось'),
    T('A service changed the state more than once. Without idempotent processing that is a double charge or a doubled report.',
      'Какой-то сервис сменил состояние больше одного раза. Без идемпотентной обработки это двойное списание или задвоенный отчёт.')],
  lost: ['err', '✗', T('Lost', 'Потерялось'),
    T('The broker acknowledged the order, then the log was truncated — nobody downstream ever saw it.',
      'Брокер подтвердил заказ, а потом лог обрезали — дальше по цепочке его никто не увидел.')],
  dlq: ['err', '☠', T('Dead-lettered', 'Ушло в DLQ'),
    T('Processing failed after retries; the order is parked in orders.dlq.', 'Обработка не удалась после ретраев; заказ лежит в orders.dlq.')],
  failed: ['err', '✗', T('Not delivered', 'Не доставлено'),
    T('The producer could not publish the order — the application got an error and must decide what to do.',
      'Producer не смог опубликовать заказ — приложение получило ошибку и должно решить, что делать.')],
  stuck: ['warn', '⌛', T('Stuck', 'Застряло'),
    T('No progress for 30+ s at some stage: lag, a paused or dead consumer — or a loss.',
      'Больше 30 с нет движения на каком-то этапе: lag, пауза или смерть консюмера — или потеря.')],
  unknown: ['muted', '?', T('Unknown id', 'Неизвестный id'),
    T('No service has seen this correlation id (or it was already evicted from memory).',
      'Ни один сервис не видел этот correlation id (или он уже вытеснен из памяти).')],
};

const FLOW = ['CREATED', 'PUBLISHED', 'PROCESSING', 'PAID', 'REPORTED'];
const BUSINESS = {
  CREATED: T('order created in order-service', 'заказ создан в order-service'),
  PUBLISHED: T('the broker acknowledged the order', 'брокер подтвердил заказ'),
  PROCESSING: T('order-processor took it', 'order-processor взял его в работу'),
  PAID: T('the payment is written to payments', 'платёж записан в payments'),
  REPORTED: T('analytics counted the order and the payment', 'analytics учла заказ и платёж'),
  LOST: T('acknowledged, then erased from the log', 'подтверждён, но стёрт из лога'),
  DEAD_LETTERED: T('parked in orders.dlq', 'отправлен в orders.dlq'),
  FAILED: T('the producer failed to publish it', 'producer не смог его опубликовать'),
};

const KIND = {
  'duplicate-processing': T('processed again', 'обработан повторно'),
  'duplicate-payment': T('paid twice', 'оплачен дважды'),
  'duplicate-count': T('counted twice', 'учтён дважды'),
  'duplicate-in-log': T('twice in the log', 'дважды в логе'),
  lost: T('lost', 'потерян'),
  dlq: 'DLQ',
  'publish-failed': T('not published', 'не опубликован'),
  'possibly-persisted': T('possibly persisted', 'возможно записан'),
  'payment-failed': T('payment failed', 'платёж не записан'),
};

/** Steps that should happen once per event: a repeat is highlighted in the timeline. */
const ONCE = new Set(['created', 'published', 'in-log', 'received', 'paid', 'counted-order', 'counted-payment']);

let current = null;
let ui = null;
let tickN = 0;
let lists = null;

const isActive = () => $('#sub-trace')?.classList.contains('active');

export function initTrace() {
  const root = $('#sub-trace');
  const input = h('input', {
    type: 'text', class: 'mono trace-input', placeholder: 'c-1a2b3c4d', spellcheck: 'false',
    'data-tip': T('Correlation ID: travels in the "correlation-id" Kafka header through every service',
      'Correlation ID: едет в Kafka-заголовке «correlation-id» через все сервисы'),
  });
  const go = h('button', { class: 'btn btn-sm' }, T('Trace', 'Найти'));
  const send = h('button', {
    class: 'btn btn-sm btn-primary',
    'data-tip': T('Send one order through order-service and follow it live', 'Отправить один заказ через order-service и следить за ним вживую'),
  }, T('📨 Send a traced order', '📨 Отправить отслеживаемый заказ'));
  const latest = h('button', { class: 'btn btn-sm', 'data-tip': T('Pick the newest generated order', 'Взять самый свежий сгенерированный заказ') },
    T('Latest order', 'Последний заказ'));
  const live = h('span', { class: 'small muted trace-live' });
  const side = h('div', { class: 'trace-side' });
  const view = h('div', { class: 'trace-view' }, h('div', { class: 'empty-state small' },
    T('Send a traced order or click any correlation id (c-…) in the event log, the messages or the lists on the left.',
      'Отправь отслеживаемый заказ или кликни любой correlation id (c-…) в журнале событий, сообщениях или списках слева.')));

  go.onclick = () => input.value.trim() && openTrace(input.value.trim());
  input.addEventListener('keydown', e => { if (e.key === 'Enter' && input.value.trim()) openTrace(input.value.trim()); });
  send.onclick = () => sendTraced(send);
  latest.onclick = async () => {
    await loadLists();
    const cid = lists?.recent?.find(r => !r.manual)?.cid;
    if (cid) openTrace(cid);
  };

  root.append(
    h('div', { class: 'small muted', style: 'margin-bottom:8px' },
      T('Follow one business event across services: did each service change its business state exactly once — or was it lost or duplicated along the way?',
        'Проследи одно бизнес-событие через все сервисы: сменил ли каждый сервис своё бизнес-состояние ровно один раз — или событие потерялось либо задвоилось по пути?'),
      DEMO ? ' ' + T('In the demo the traces are a snapshot taken at the end of the recording.', 'В демо трассы — снимок, сделанный в конце записи.') : ''),
    h('div', { class: 'msg-toolbar' }, input, go, send, latest, live),
    h('div', { class: 'trace-layout' }, side, view));
  ui = { input, live, side, view };

  // Any element with data-cid (event log, messages, order card, lists) opens its trace
  document.addEventListener('click', e => {
    const a = e.target.closest('[data-cid]');
    if (!a || !a.dataset.cid) return;
    e.preventDefault();
    openTrace(a.dataset.cid);
  });
  document.querySelector('.subtab[data-sub="trace"]')?.addEventListener('click', () => { loadLists(); if (current) loadTrace(current); });
  setInterval(tick, 1500);
}

async function tick() {
  if (!isActive() || document.hidden) return;
  tickN++;
  if (current) await loadTrace(current);
  if (tickN % 2 === 0 || !lists) await loadLists();
}

export function openTrace(cid) {
  if ($('#tab-live') && !$('#tab-live').classList.contains('active')) document.querySelector('.tab-btn[data-tab="live"]')?.click();
  showSubtab('trace');
  current = cid;
  ui.input.value = cid;
  loadTrace(cid);
  loadLists();
  $('#sub-trace').closest('.panel')?.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
}

async function sendTraced(btn) {
  if (demoGuard()) return;
  btn.disabled = true;
  const r = await svc.post('order-service', 'orders', {});
  btn.disabled = false;
  if (r?.correlationId) {
    toast(T(`Order ${r.orderId} sent with correlation id ${r.correlationId}`, `Заказ ${r.orderId} отправлен с correlation id ${r.correlationId}`), 'ok', 4000);
    openTrace(r.correlationId);
  }
}

async function loadTrace(cid) {
  const t = await api('GET', `api/trace/${encodeURIComponent(cid)}`, undefined, { quiet: true });
  if (!t || t.ok === false || cid !== current) return;
  renderTrace(t);
}

async function loadLists() {
  const r = await api('GET', 'api/traces/recent', undefined, { quiet: true });
  if (!r || r.ok === false) return;
  lists = r;
  renderLists(r);
}

// ------------------------------------------------------------------ rendering

function verdictChip(verdict) {
  const [cls, icon, title] = VERDICT[verdict] ?? VERDICT.unknown;
  return h('span', { class: `verdict v-${cls}` }, `${icon} ${title}`);
}

function cidRow(item, extra) {
  return h('a', { class: 'trace-row' + (item.cid === current ? ' active' : ''), href: '#', 'data-cid': item.cid },
    h('span', { class: 'mono' }, item.cid),
    h('span', { class: `vdot v-${(VERDICT[item.verdict] ?? VERDICT.unknown)[0]}`, 'data-tip': (VERDICT[item.verdict] ?? VERDICT.unknown)[2] },
      (VERDICT[item.verdict] ?? VERDICT.unknown)[1]),
    h('span', { class: 'bstate' }, item.businessState),
    extra ? h('span', { class: 'small muted trace-row-extra' }, extra) : null);
}

function renderLists(r) {
  const manual = r.recent.filter(x => x.manual);
  const generated = r.recent.filter(x => !x.manual);
  const section = (title, tip, rows, empty) => h('div', { class: 'trace-list' },
    h('div', { class: 'trace-list-title', 'data-tip': tip }, title),
    rows.length ? rows : h('div', { class: 'small muted' }, empty));
  ui.side.replaceChildren(...[
    section(T('⚠ Problems', '⚠ Проблемы'),
      T('Events where a service saw a duplicate, a loss, a DLQ or a failed publish', 'События, где сервис заметил дубль, потерю, DLQ или неудачную публикацию'),
      r.problems.map(p => cidRow(p, p.kinds.map(k => KIND[k] ?? k).join(', '))),
      T('none so far — try the scenarios', 'пока нет — попробуй сценарии')),
    section(T('📨 Traced by you', '📨 Отправлены тобой'), null,
      manual.map(x => cidRow(x, x.customer)), T('nothing yet', 'пока нет')),
    section(T('🕒 Latest generated', '🕒 Последние сгенерированные'), null,
      generated.map(x => cidRow(x, x.customer)), '—'),
    r.unreachable?.length ? h('div', { class: 'small warn-text' },
      T('Not reachable: ', 'Недоступны: ') + r.unreachable.join(', ')) : null,
  ].filter(Boolean));
}

function renderTrace(t) {
  const [cls, , , text] = VERDICT[t.verdict] ?? VERDICT.unknown;
  const created = t.steps.find(s => s.step === 'created');
  const waiting = t.stages.filter(s => s.status === 'pending' || s.status === 'missing').map(s => STAGES[s.id]?.title ?? s.id);
  const dups = t.stages.filter(s => s.status === 'duplicate').map(s => STAGES[s.id]?.title ?? s.id);
  const final = ['ok', 'lost', 'dlq', 'failed', 'unknown'].includes(t.verdict);
  ui.live.textContent = final ? '' : T('● live', '● обновляется');

  const head = h('div', { class: 'trace-head' },
    h('span', { class: 'mono trace-cid' }, t.cid),
    created ? h('span', { class: 'small muted' }, created.detail) : null,
    h('span', { style: 'flex:1' }),
    verdictChip(t.verdict));
  const explain = h('div', { class: `trace-explain v-${cls}` }, text,
    dups.length ? ' ' + T(`Duplicated at: ${dups.join(', ')}.`, `Задвоилось на этапах: ${dups.join(', ')}.`) : '',
    waiting.length && !['ok', 'lost', 'dlq', 'failed'].includes(t.verdict) ? ' ' + T(`Waiting for: ${waiting.join(', ')}.`, `Ждём: ${waiting.join(', ')}.`) : '');

  ui.view.replaceChildren(...[head, explain, renderFlow(t), renderStages(t),
    t.unreachable?.length ? h('div', { class: 'small warn-text', style: 'margin:6px 0' },
      T(`⚠ Not reachable: ${t.unreachable.join(', ')} — their steps are unknown, so the verdict may be incomplete (each service keeps its trace in memory).`,
        `⚠ Недоступны: ${t.unreachable.join(', ')} — их шаги неизвестны, вердикт может быть неполным (каждый сервис хранит трассу в памяти).`)) : null,
    renderTimeline(t)].filter(Boolean));
}

function renderFlow(t) {
  const st = Object.fromEntries(t.stages.map(s => [s.id, s]));
  const reached = {
    CREATED: t.found,
    PUBLISHED: st.produce?.count > 0,
    PROCESSING: st.process?.count > 0,
    PAID: st.payment?.count > 0,
    REPORTED: st['analytics-payment']?.count > 0 || (st['analytics-order']?.count > 0 && st['analytics-payment']?.status === 'skipped'),
  };
  const chips = [];
  FLOW.forEach((s, i) => {
    if (i) chips.push(h('span', { class: 'flow-arrow' }, '→'));
    chips.push(h('span', {
      class: 'bstate flow' + (reached[s] ? ' reached' : '') + (t.businessState === s ? ' current' : ''),
      'data-tip': BUSINESS[s],
    }, s));
  });
  if (['LOST', 'DEAD_LETTERED', 'FAILED'].includes(t.businessState)) {
    chips.push(h('span', { class: 'flow-arrow' }, '⇒'), h('span', { class: 'bstate flow terminal', 'data-tip': BUSINESS[t.businessState] }, t.businessState));
  }
  return h('div', { class: 'trace-flow' },
    h('span', { class: 'small muted', 'data-tip': T('An abstract business state of the order, assembled from what every service did with it',
      'Абстрактное бизнес-состояние заказа, собранное из того, что сделал с ним каждый сервис') }, T('Business state:', 'Бизнес-состояние:')),
    ...chips);
}

function renderStages(t) {
  const cards = [];
  t.stages.forEach((s, i) => {
    const def = STAGES[s.id] ?? { icon: '•', title: s.id, what: '' };
    const [sIcon, sText] = STATUS[s.status] ?? ['?', s.status];
    cards.push(h('div', { class: `tstage st-${s.status}` },
      h('div', { class: 'tstage-top' }, h('span', { class: 'tstage-n' }, `${i + 1}`), h('span', {}, def.icon), h('b', {}, def.title)),
      h('div', { class: 'small muted' }, s.service === 'kafka' ? `Kafka · ${s.topic}` : `${s.service}`),
      h('div', { class: 'tstage-count', 'data-tip': T('How many times this happened (expected: exactly 1)', 'Сколько раз это произошло (ожидается ровно 1)') },
        `×${s.count}`),
      h('div', { class: 'small muted' }, def.what),
      s.state ? h('span', { class: 'bstate' }, s.state) : null,
      h('div', { class: 'tstage-status' }, `${sIcon} ${sText}`),
      s.instances?.length ? h('div', { class: 'small muted tstage-inst' }, s.instances.join(', ')) : null));
  });
  return h('div', { class: 'tstages' }, cards);
}

function renderTimeline(t) {
  if (!t.steps.length) return h('div', { class: 'empty-state small' }, (VERDICT.unknown)[3]);
  const seen = {};
  const rows = t.steps.map(s => {
    seen[s.step] = (seen[s.step] ?? 0) + 1;
    const repeat = ONCE.has(s.step) && seen[s.step] > 1;
    const bad = ['LOST', 'PUBLISH_FAILED', 'DEAD_LETTERED', 'PAYMENT_FAILED'].includes(s.state);
    const where = s.topic ? `${s.topic}${s.partition !== null && s.partition !== undefined ? `-${s.partition}` : ''}${s.offset !== null && s.offset !== undefined ? `@${s.offset}` : ''}` : '';
    return h('tr', { class: repeat ? 'dup' : bad ? 'bad' : '' },
      h('td', { class: 'num mono' }, `+${fmtDelta(s.ts - t.firstTs)}`),
      h('td', {}, h('b', {}, s.service), s.instance && s.instance !== s.service ? h('span', { class: 'muted' }, ` · ${s.instance}`) : null),
      h('td', { class: 'mono' }, s.step, repeat ? h('span', { class: 'badge warn', style: 'margin-left:6px' }, `#${seen[s.step]}`) : null),
      h('td', {}, h('span', { class: 'bstate' }, s.state)),
      h('td', { class: 'mono small' }, where),
      h('td', { class: 'small muted' }, s.detail ?? ''));
  });
  return h('div', { style: 'overflow-x:auto;margin-top:8px' },
    h('div', { class: 'small muted', style: 'margin-bottom:3px' },
      T(`Timeline (started ${fmtTime(t.firstTs)})`, `Хронология (начало ${fmtTime(t.firstTs)})`)),
    h('table', { class: 'grid trace-table' },
      h('thead', {}, h('tr', {},
        h('th', {}, T('time', 'время')), h('th', {}, T('service', 'сервис')), h('th', {}, T('step', 'шаг')),
        h('th', {}, T('business state', 'бизнес-состояние')), h('th', {}, T('where', 'где')), h('th', {}, T('details', 'детали')))),
      h('tbody', {}, rows)));
}

function fmtDelta(ms) {
  if (ms < 1000) return `${ms} ${T('ms', 'мс')}`;
  if (ms < 120000) return `${(ms / 1000).toFixed(1)} ${T('s', 'с')}`;
  return `${Math.round(ms / 60000)} ${T('min', 'мин')}`;
}
