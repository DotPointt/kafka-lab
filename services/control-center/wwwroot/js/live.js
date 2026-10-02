// Live map: producers → brokers (partitions) → consumer groups. Cards are built once and updated every second.
import {
  h, $, setText, setClass, fmtNum, fmtRate, fmtMs, fmtBytes, topicColor, LANG_CLASS,
  stepSlider, segmented, selectBox, toggle, syncList, partitionFor, openPopover, closePopover, toast, esc,
} from './util.js';
import { store } from './store.js';
import { svc, actions, demoGuard } from './api.js';
import { T, isRu } from './i18n.js';

const TOPIC_ORDER = ['orders', 'payments', 'orders.dlq', 'customer-profiles', 'clickstream'];
const GROUP_SERVICE = { 'order-processing': 'order-processor', 'analytics': 'analytics' };
const PER_SEC = T('/s', '/с');
const MS = T('ms', 'мс');
const SEC = T('s', 'с');

export function sortTopics(topics) {
  return [...topics].filter(t => !t.internal).sort((a, b) => {
    const ia = TOPIC_ORDER.indexOf(a.name), ib = TOPIC_ORDER.indexOf(b.name);
    return (ia < 0 ? 99 : ia) - (ib < 0 ? 99 : ib) || a.name.localeCompare(b.name);
  });
}

// ================================================================== shared bits

function metric(label, tip) {
  const v = h('div', { class: 'v' }, '—');
  const el = h('div', { class: 'metric', 'data-tip': tip }, v, h('div', { class: 'l' }, label));
  return { el, set(text, cls = '') { setText(v, text); setClass(el, 'metric ' + cls); } };
}

function containerOf(serviceName, index = 0) {
  return store.service(serviceName)?.instances?.[index] ?? null;
}

/** Service container buttons: pause / stop / kill / start. */
function containerButtons(getContainer, opts = {}) {
  const pause = h('button', { class: 'icon-btn', 'data-tip': T('docker pause — freeze the process', 'docker pause — заморозить процесс') }, '⏸');
  const unpause = h('button', { class: 'icon-btn', 'data-tip': T('docker unpause — unfreeze', 'docker unpause — разморозить') }, '▶');
  const stop = h('button', { class: 'icon-btn', 'data-tip': opts.stopTip ?? T('docker stop — graceful shutdown (SIGTERM)', 'docker stop — корректная остановка (SIGTERM)') }, '⏹');
  const kill = h('button', { class: 'icon-btn danger', 'data-tip': opts.killTip ?? T('docker kill — instant death (SIGKILL)', 'docker kill — мгновенная смерть (SIGKILL)') }, '💥');
  const start = h('button', { class: 'icon-btn', 'data-tip': 'docker start' }, '▶');
  const run = action => async () => {
    const c = getContainer();
    if (!c) return;
    const r = await actions.container(c.container, action);
    if (r.ok !== false) toast(`${c.container}: ${action}`, 'ok', 2500);
  };
  pause.onclick = run('pause'); unpause.onclick = run('unpause'); stop.onclick = run('stop'); kill.onclick = run('kill'); start.onclick = run('start');
  const el = h('div', { class: 'card-actions' }, pause, unpause, stop, kill, start);
  el.sync = state => {
    pause.classList.toggle('hidden', state !== 'running');
    unpause.classList.toggle('hidden', state !== 'paused');
    stop.classList.toggle('hidden', state !== 'running');
    kill.classList.toggle('hidden', !(state === 'running' || state === 'paused'));
    start.classList.toggle('hidden', !(state === 'exited' || state === 'created' || state === 'dead'));
  };
  return el;
}

function stateDot(state, ok) {
  if (state === 'paused') return 'dot pause';
  if (state !== 'running') return 'dot err';
  return ok ? 'dot ok' : 'dot warn';
}

function downNote(state) {
  return {
    paused: T('Container is frozen (docker pause)', 'Контейнер заморожен (docker pause)'),
    exited: T('Container is stopped', 'Контейнер остановлен'),
    dead: T('Container is dead', 'Контейнер мёртв'),
    created: T('Container is not started', 'Контейнер не запущен'),
  }[state] ?? null;
}

// ================================================================== order-service (C# producer)

function orderServiceCard() {
  const name = 'order-service';
  const m = {
    sent: metric(T('sending', 'отправка'), T('Orders per second put into the producer buffer (Produce)', 'Сколько заказов в секунду кладём в буфер producer-а (Produce)')),
    acked: metric(T('acked', 'подтверждено'), T('Acknowledged by the broker per second (delivery report without an error)', 'Сколько в секунду брокер подтвердил (delivery report без ошибки)')),
    failed: metric(T('errors', 'ошибки'), T('Messages per second that could not be delivered within delivery.timeout.ms', 'Сколько в секунду не удалось доставить за delivery.timeout.ms')),
    p99: metric('p99 ack', T('Time from Produce() to the broker acknowledgement (99th percentile over 5 s)', 'Время от Produce() до подтверждения от брокера (99-й перцентиль за 5 с)')),
  };
  const dot = h('span', { class: 'dot' });
  const ctl = containerButtons(() => containerOf(name), {
    killTip: T('docker kill — the producer dies, unacknowledged messages in its buffer are lost', 'docker kill — producer умирает, неподтверждённые сообщения из буфера теряются'),
  });
  const kv = h('div', { class: 'kv' });
  const note = h('div', { class: 'small muted' });

  const set = patch => svc.config(name, patch);
  const rate = stepSlider([0, 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000], 20, v => set({ ratePerSec: v }), v => v + PER_SEC);
  const acks = segmented([
    ['0', '0', T("acks=0 — don't wait for the broker. Fast, but losses are invisible", 'acks=0 — не ждать ответа брокера. Быстро, но потери не видны')],
    ['1', '1', T('acks=1 — wait for the leader only. Data may be lost when the leader changes', 'acks=1 — ждать записи только у лидера. Потеря при смене лидера возможна')],
    ['all', 'all', T('acks=all — wait for every ISR replica (and check min.insync.replicas)', 'acks=all — ждать все реплики из ISR (и проверять min.insync.replicas)')],
  ], 'all', v => set({ acks: v }));
  const idem = toggle(T('idempotence', 'идемпотентность'), true, v => set({ enableIdempotence: v }),
    T('enable.idempotence: the broker drops retried duplicates by (ProducerId, sequence). Requires acks=all', 'enable.idempotence: брокер отбрасывает дубли ретраев по (ProducerId, sequence). Требует acks=all'));
  const linger = stepSlider([0, 1, 5, 10, 20, 50, 100, 500], 5, v => set({ lingerMs: v }), v => v + MS);
  const compression = selectBox([['none', 'none'], ['gzip', 'gzip'], ['snappy', 'snappy'], ['lz4', 'lz4'], ['zstd', 'zstd']], 'none', v => set({ compression: v }));
  const reqTimeout = stepSlider([300, 500, 1000, 2000, 5000, 10000, 30000], 10000, v => set({ requestTimeoutMs: v }), fmtMs);
  const delTimeout = stepSlider([3000, 5000, 10000, 30000, 60000, 120000, 300000], 30000, v => set({ deliveryTimeoutMs: v }), fmtMs);
  const hot = stepSlider([0, 10, 25, 50, 80, 100], 0, v => set({ hotKeyPercent: v }), v => v + '%');
  const inflight = stepSlider([1, 2, 5, 10, 20], 5, v => set({ maxInFlight: v }), v => String(v));

  const keyInput = h('input', { type: 'text', value: 'customer-007', 'data-tip': T('Message key. Partition = murmur2(key) % partition count', 'Ключ сообщения. Партиция = murmur2(key) % число партиций') });
  const predicted = h('span', { class: 'mono small' });
  const result = h('div', { class: 'result' });
  const predict = () => {
    const n = store.topic('orders')?.partitions?.length ?? 6;
    const key = keyInput.value.trim();
    predicted.textContent = key ? `murmur2 → P${partitionFor(key, n)}` : T('no key → any', 'без ключа → любая');
  };
  keyInput.addEventListener('input', predict);
  const sendBtn = h('button', { class: 'btn btn-sm btn-primary' }, T('Send order', 'Отправить заказ'));
  sendBtn.onclick = async () => {
    sendBtn.disabled = true;
    const r = await svc.post(name, 'orders', { customerId: keyInput.value.trim() || null });
    sendBtn.disabled = false;
    if (r?.ok) {
      result.innerHTML = T(
        `✓ ${esc(r.orderId)}: key "${esc(r.key)}" → <b>partition ${r.partition}</b>, offset ${r.offset}, ${fmtMs(r.latencyMs)} (acks=${r.acks})`,
        `✓ ${esc(r.orderId)}: ключ «${esc(r.key)}» → <b>партиция ${r.partition}</b>, offset ${r.offset}, ${fmtMs(r.latencyMs)} (acks=${r.acks})`);
    } else if (r && r.error !== 'demo') {
      result.innerHTML = `<span style="color:var(--err)">✗ ${esc(r.error)}: ${esc(r.reason ?? '')} ${r.status ? '(' + esc(r.status) + ')' : ''}</span>`;
    }
  };
  const burstBtn = h('button', { class: 'btn btn-sm', 'data-tip': T('Send 10,000 orders as fast as possible — watch batching and lag growth', 'Отправить 10 000 заказов максимально быстро — увидеть batching и рост lag') }, 'Burst 10k');
  burstBtn.onclick = () => svc.post(name, 'burst', { count: 10000 });

  const verifier = h('div', { class: 'box' });
  const client = h('div', { class: 'client-view' });
  const errors = h('div', { class: 'errors-list' });

  const el = h('div', { class: 'card producer', 'data-node': 'svc:order-service' },
    h('div', { class: 'card-head' },
      h('span', { class: 'lang cs' }, 'C#'),
      h('div', { style: 'min-width:0' }, h('div', { class: 'card-title' }, 'order-service'),
        h('div', { class: 'card-sub' }, T('producer → orders (key = customerId), customer-profiles', 'producer → orders (ключ = customerId), customer-profiles'))),
      dot, ctl),
    h('div', { class: 'metrics' }, m.sent.el, m.acked.el, m.failed.el, m.p99.el),
    kv, note,
    h('div', { class: 'controls' },
      h('label', { 'data-tip': T('Order generation rate', 'Скорость генерации заказов') }, T('Load', 'Нагрузка')), rate,
      h('label', { 'data-tip': T('How many acknowledgements to wait for', 'Сколько подтверждений ждать от брокеров') }, 'acks'), h('div', { class: 'ctl-row' }, acks, idem)),
    h('details', { class: 'more' }, h('summary', {}, T('⚙ more producer settings', '⚙ ещё настройки producer')),
      h('div', { class: 'controls' },
        h('label', { 'data-tip': T('linger.ms: how long to wait while filling a batch', 'linger.ms: сколько ждать, наполняя batch') }, 'linger.ms'), linger,
        h('label', { 'data-tip': T('Batch compression', 'Сжатие batch-ей') }, 'compression'), compression,
        h('label', { 'data-tip': T('How long to wait for the broker to answer a request. Shorter than network latency → retries → duplicates without idempotence', 'Таймаут ответа брокера на запрос. Меньше задержки сети → ретраи → дубли без идемпотентности') }, 'request.timeout'), reqTimeout,
        h('label', { 'data-tip': T('Total time to deliver a message, retries included', 'Общее время на доставку сообщения, включая ретраи') }, 'delivery.timeout'), delTimeout,
        h('label', { 'data-tip': T('Share of orders from customer-007: one key → one "hot" partition', 'Доля заказов от customer-007: один ключ → одна «горячая» партиция') }, T('hot key', 'горячий ключ')), hot,
        h('label', { 'data-tip': 'max.in.flight.requests.per.connection' }, 'max.in.flight'), inflight)),
    h('div', { class: 'send-row' }, keyInput, predicted, sendBtn, burstBtn),
    result,
    verifier, client, errors);

  predict();

  return {
    el,
    update() {
      const inst = containerOf(name);
      const s = inst?.stats;
      ctl.sync(inst?.containerState);
      setClass(dot, stateDot(inst?.containerState, inst?.ok));
      el.classList.toggle('dim', !inst?.ok);
      setText(note, inst?.ok ? '' : (downNote(inst?.containerState) ??
        (inst?.error ? `${T('no response', 'нет ответа')}: ${inst.error}` : T('no data', 'нет данных'))));
      if (!s) return;
      const p = s.producer;
      m.sent.set(fmtRate(p.rates.sent));
      m.acked.set(fmtRate(p.rates.acked), p.rates.acked > 0 ? 'good' : '');
      m.failed.set(fmtRate(p.rates.failed), p.rates.failed > 0 ? 'bad' : '');
      m.p99.set(fmtMs(p.latencyMs?.p99), (p.latencyMs?.p99 ?? 0) > 1000 ? 'warnv' : '');
      kv.innerHTML =
        `<span data-tip="${T('Messages buffered in the producer or in flight — not acknowledged yet', 'Сообщения в буфере producer-а и в полёте — ещё без подтверждения брокера')}">${T('awaiting ack', 'ждут ack')} <b>${fmtNum(p.inFlight)}</b></span>` +
        `<span>${T('sent', 'отправлено')} <b>${fmtNum(p.sent)}</b></span><span>${T('acked', 'подтверждено')} <b>${fmtNum(p.acked)}</b></span>` +
        `<span>${T('errors', 'ошибок')} <b class="${p.failed ? 'bad' : ''}">${fmtNum(p.failed)}</b></span>` +
        (p.possiblyPersisted ? `<span data-tip="${T('Failed, but the write may have reached the broker', 'Ошибка, но запись могла дойти до брокера')}">${T('possibly written', 'возможно записаны')} <b class="bad">${fmtNum(p.possiblyPersisted)}</b></span>` : '') +
        (p.queueFull ? `<span>${T('buffer full', 'буфер полон')} ×<b class="bad">${fmtNum(p.queueFull)}</b></span>` : '');

      const c = s.config;
      rate.sync(c.ratePerSec); acks.sync(c.acks); idem.sync(c.enableIdempotence); linger.sync(c.lingerMs);
      compression.sync(c.compression); reqTimeout.sync(c.requestTimeoutMs); delTimeout.sync(c.deliveryTimeoutMs);
      hot.sync(c.hotKeyPercent); inflight.sync(c.maxInFlight);
      if (document.activeElement !== keyInput) predict();

      const v = s.verifier;
      const lost = v.lost + v.unconfirmedTimeout;
      verifier.className = 'box' + (lost > 0 || v.duplicatesInLog > 0 ? ' alarm' : '');
      verifier.innerHTML =
        `<div class="box-title" data-tip="${T('A separate consumer reads orders and checks that everything the broker acknowledged is in the log at its offset', 'Отдельный consumer читает orders и сверяет: всё ли, что брокер подтвердил, лежит в логе по своему offset')}">${T('🔍 Delivery audit (ack ↔ log)', '🔍 Аудит доставки (ack ↔ лог)')}</div>` +
        `<div class="kv"><span>${T('acked and found', 'подтверждено и найдено')} <b class="good">${fmtNum(v.confirmed)}</b></span>` +
        `<span>${T('pending check', 'ждут сверки')} <b>${fmtNum(v.pending)}</b></span>` +
        `<span data-tip="${T('Acknowledged by the broker, then gone from the log', 'Подтверждены брокером, но исчезли из лога')}">${T('LOST', 'ПОТЕРЯНО')} <b class="${lost ? 'bad' : ''}">${fmtNum(lost)}</b></span>` +
        `<span data-tip="${T('The same orderId written to the log twice', 'Один orderId записан в лог дважды')}">${T('duplicates in log', 'дубли в логе')} <b class="${v.duplicatesInLog ? 'bad' : ''}">${fmtNum(v.duplicatesInLog)}</b></span>` +
        (v.unverifiable ? `<span data-tip="${T("acks=0: the broker doesn't report an offset — nothing to check", 'acks=0: брокер не сообщает offset — проверить нельзя')}">${T('unverifiable', 'не проверить')} <b>${fmtNum(v.unverifiable)}</b></span>` : '') +
        `</div>` +
        (v.lostSamples?.length ? `<div class="small" style="margin-top:4px;color:#fca5a5">${v.lostSamples.slice(0, 3).map(x => `${esc(x.orderId)} @ orders-${x.partition}:${x.offset}` + (x.replacedBy ? ` → ${T('now holds', 'там теперь')} ${esc(x.replacedBy)}` : '')).join('<br>')}</div>` : '');

      const brokers = s.client?.brokers ?? [];
      syncList(client, brokers, b => b.id, () => h('span', { class: 'cv' }), (elx, b) => {
        const up = b.state === 'UP';
        setClass(elx, 'cv ' + (up ? 'up' : 'down'));
        setText(elx, `b${b.id} ${b.state}${up ? ' · ' + fmtMs(b.rttAvgMs) : ''}`);
        elx.dataset.tip = `<div class="tt-title">${T(`How order-service sees broker ${b.id}`, `Как order-service видит брокер ${b.id}`)}</div>` +
          `<div class="tt-row">${T('connection state', 'состояние соединения')}: ${b.state}</div><div class="tt-row">RTT avg/p99: ${fmtMs(b.rttAvgMs)} / ${fmtMs(b.rttP99Ms)}</div>` +
          `<div class="tt-row">${T('waiting to send', 'ждут отправки')}: ${b.outbufMsgs}, ${T('awaiting response', 'ждут ответа')}: ${b.waitRespMsgs}</div>` +
          `<div class="tt-row">${T('request timeouts', 'таймаутов запросов')}: ${b.reqTimeouts}</div>`;
      });

      const lastErr = p.recentErrors?.length ? p.recentErrors[p.recentErrors.length - 1] : null;
      const errs = Object.entries(p.errorCounts ?? {});
      errors.innerHTML = lastErr && Date.now() - lastErr.ts < 60000
        ? `${T('last error', 'последняя ошибка')}: ${esc(lastErr.code)} — ${esc(lastErr.reason)}<br><span class="muted">${T('total', 'всего')}: ${errs.map(([k, n]) => `${esc(k)} ×${fmtNum(n)}`).join(' · ')}</span>`
        : '';
    },
  };
}

// ================================================================== clickstream-generator (Go producer)

function clickstreamCard() {
  const name = 'clickstream-generator';
  const m = {
    sent: metric(T('sending', 'отправка'), T('Messages per second', 'Сообщений в секунду')),
    acked: metric(T('acked', 'подтверждено'), T('Acknowledged by the broker per second', 'Подтверждено брокером в секунду')),
    bytes: metric(T('traffic', 'трафик'), T('Acknowledged data per second (before compression)', 'Объём подтверждённых данных в секунду (до сжатия)')),
    p99: metric('p99 ack', T('Acknowledgement latency, 99th percentile', 'Задержка подтверждения, 99-й перцентиль')),
  };
  const dot = h('span', { class: 'dot' });
  const ctl = containerButtons(() => containerOf(name));
  const kv = h('div', { class: 'kv' });
  const note = h('div', { class: 'small muted' });
  const set = patch => svc.config(name, patch);
  const rate = stepSlider([0, 100, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100000], 1000, v => set({ rate: v }), v => fmtNum(v) + PER_SEC);
  const size = stepSlider([100, 200, 500, 1000, 2000, 5000, 10000], 200, v => set({ sizeBytes: v }), v => fmtBytes(v));
  const acks = segmented([['0', '0', 'acks=0'], ['1', '1', 'acks=1'], ['all', 'all', T('acks=all (+ idempotence in franz-go)', 'acks=all (+ идемпотентность в franz-go)')]], '1', v => set({ acks: v }));
  const compression = selectBox([['none', 'none'], ['gzip', 'gzip'], ['snappy', 'snappy'], ['lz4', 'lz4'], ['zstd', 'zstd']], 'lz4', v => set({ compression: v }));
  const linger = stepSlider([0, 1, 5, 10, 20, 50, 100, 200], 10, v => set({ lingerMs: v }), v => v + MS);
  const keyed = toggle(T('key = sessionId', 'ключ = sessionId'), false, v => set({ keyed: v }),
    T('With a key — partition by hash(key); without one — the sticky partitioner (a whole batch goes to one partition)', 'С ключом — партиция по hash(key); без ключа — sticky partitioner (батч целиком в одну партицию)'));
  const errors = h('div', { class: 'errors-list' });

  const el = h('div', { class: 'card producer', 'data-node': 'svc:clickstream-generator' },
    h('div', { class: 'card-head' },
      h('span', { class: 'lang go' }, 'Go'),
      h('div', { style: 'min-width:0' }, h('div', { class: 'card-title' }, 'clickstream-generator'), h('div', { class: 'card-sub' }, 'producer → clickstream (RF=2), franz-go')),
      dot, ctl),
    h('div', { class: 'metrics' }, m.sent.el, m.acked.el, m.bytes.el, m.p99.el),
    kv, note,
    h('div', { class: 'controls' },
      h('label', {}, T('Load', 'Нагрузка')), rate,
      h('label', {}, 'acks'), h('div', { class: 'ctl-row' }, acks, keyed)),
    h('details', { class: 'more' }, h('summary', {}, T('⚙ batching and compression', '⚙ batching и сжатие')),
      h('div', { class: 'controls' },
        h('label', {}, T('size', 'размер')), size,
        h('label', { 'data-tip': T('How long to wait for a batch to fill', 'Сколько ждать наполнения batch') }, 'linger'), linger,
        h('label', {}, 'compression'), compression)),
    errors);

  return {
    el,
    update() {
      const inst = containerOf(name);
      const s = inst?.stats;
      ctl.sync(inst?.containerState);
      setClass(dot, stateDot(inst?.containerState, inst?.ok));
      el.classList.toggle('dim', !inst?.ok);
      setText(note, inst?.ok ? '' : (downNote(inst?.containerState) ?? T('no data', 'нет данных')));
      if (!s) return;
      const p = s.producer;
      m.sent.set(fmtRate(p.rates.sent));
      m.acked.set(fmtRate(p.rates.acked), p.rates.acked > 0 ? 'good' : '');
      m.bytes.set(fmtBytes(p.rates.bytes) + PER_SEC);
      m.p99.set(fmtMs(p.latencyMs?.p99), (p.latencyMs?.p99 ?? 0) > 1000 ? 'warnv' : '');
      kv.innerHTML =
        `<span data-tip="${T('Records in the client buffer, not acknowledged by the broker yet', 'Записи в буфере клиента, ещё не подтверждённые брокером')}">${T('buffered', 'в буфере')} <b>${fmtNum(p.buffered)}</b></span>` +
        `<span>${T('sent', 'отправлено')} <b>${fmtNum(p.sent)}</b></span><span>${T('errors', 'ошибок')} <b class="${p.failed ? 'bad' : ''}">${fmtNum(p.failed)}</b></span>` +
        (p.bufferFull ? `<span data-tip="${T('The buffer was full — the generator had to slow down', 'Буфер был полон — генератор притормаживал')}">backpressure ×<b class="bad">${fmtNum(p.bufferFull)}</b></span>` : '');
      const c = s.config;
      rate.sync(c.rate); size.sync(c.sizeBytes); acks.sync(c.acks); compression.sync(c.compression); linger.sync(c.lingerMs); keyed.sync(c.keyed);
      const errs = Object.entries(p.errorCounts ?? {});
      errors.innerHTML = p.lastErrorTs && Date.now() - p.lastErrorTs < 60000
        ? T('errors so far: ', 'ошибки за всё время: ') + errs.slice(0, 3).map(([k, n]) => `${esc(k)} ×${fmtNum(n)}`).join('<br>')
        : '';
    },
  };
}

// ================================================================== brokers

const STATUS_TEXT = {
  online: [T('● in cluster', '● в кластере'), 'badge ok'],
  fenced: [T('⛔ fenced', '⛔ зафенсен'), 'badge fenced'],
  paused: [T('⏸ frozen', '⏸ заморожен'), 'badge pause'],
  stopped: [T('■ stopped', '■ остановлен'), 'badge err'],
  starting: [T('… starting', '… запускается'), 'badge warn'],
  unknown: ['?', 'badge'],
};

function networkPopover(broker) {
  const c = broker.chaos ?? {};
  const state = { latencyMs: c.latencyMs ?? 0, jitterMs: c.jitterMs ?? 0, lossPct: c.lossPct ?? 0, isolated: !!c.isolated, splitFromBrokers: !!c.splitFromBrokers };
  const LAT = [0, 20, 50, 100, 200, 300, 500, 1000, 1500, 2000, 3000, 5000];
  const JIT = [0, 5, 10, 25, 50, 100, 200, 500];
  const LOSS = [0, 1, 2, 5, 10, 20, 30, 50, 80, 100];
  const lat = stepSlider(LAT, state.latencyMs, v => { state.latencyMs = v; }, fmtMs);
  const jit = stepSlider(JIT, state.jitterMs, v => { state.jitterMs = v; }, fmtMs);
  const loss = stepSlider(LOSS, state.lossPct, v => { state.lossPct = v; }, v => v + '%');
  const iso = toggle(T('full isolation (100% loss)', 'полная изоляция (100% потерь)'), state.isolated, v => { state.isolated = v; },
    T("The broker sees nobody and nobody sees it", 'Брокер не видит никого и никто не видит его'));
  const split = toggle(T('cut off from the other brokers only', 'отрезать только от других брокеров'), state.splitFromBrokers, v => { state.splitFromBrokers = v; },
    T("The broker can't see the cluster or the controller, but clients still reach it → a \"zombie leader\". Try it with acks=1 and acks=all!",
      'Брокер не видит кластер и контроллер, но клиенты его видят → «зомби-лидер». Попробуй с acks=1 и acks=all!'));
  const apply = async chaos => {
    const r = await actions.network(broker.id, chaos);
    if (r.ok !== false) { toast(T(`Broker ${broker.id} network updated`, `Сеть брокера ${broker.id} обновлена`), 'ok', 2500); closePopover(); }
  };
  const none = { latencyMs: 0, jitterMs: 0, lossPct: 0, isolated: false, splitFromBrokers: false };
  const preset = (label, tip, chaos) => h('button', { class: 'btn btn-xs', 'data-tip': tip, onclick: () => apply({ ...none, ...chaos }) }, label);
  return h('div', {},
    h('h4', {}, T(`Broker ${broker.id} network`, `Сеть брокера ${broker.id}`)),
    h('div', { class: 'hint' }, T("tc netem rules on the broker's outgoing traffic. They are reset when the container restarts.", 'Правила tc netem на исходящий трафик брокера. Сбрасываются при рестарте контейнера.')),
    h('div', { class: 'btns', style: 'justify-content:flex-start;margin:0 0 6px' },
      preset(T('🐢 300 ms', '🐢 300 мс'), T('Latency 300±50 ms', 'Задержка 300±50 мс'), { latencyMs: 300, jitterMs: 50 }),
      preset(T('🐌 2 s', '🐌 2 с'), T('2 s latency — longer than request.timeout with "aggressive" settings', 'Задержка 2 с — длиннее request.timeout при «агрессивных» настройках'), { latencyMs: 2000, jitterMs: 100 }),
      preset('📉 20%', T('20% packet loss', 'Потеря 20% пакетов'), { lossPct: 20 }),
      preset(T('✂ isolate', '✂ изоляция'), T('Full isolation', 'Полная изоляция'), { isolated: true }),
      preset('🔌 split', T('Cut off from the other brokers', 'Отрезать от других брокеров'), { splitFromBrokers: true })),
    h('div', { class: 'row' }, h('span', {}, T('latency', 'задержка')), lat.firstChild, lat.lastChild),
    h('div', { class: 'row' }, h('span', {}, T('jitter', 'джиттер')), jit.firstChild, jit.lastChild),
    h('div', { class: 'row' }, h('span', {}, T('loss', 'потери')), loss.firstChild, loss.lastChild),
    iso, h('br'), split,
    h('div', { class: 'btns' },
      h('button', { class: 'btn btn-sm', onclick: () => apply(none) }, T('✚ Restore network', '✚ Вернуть сеть')),
      h('button', {
        class: 'btn btn-sm btn-primary',
        onclick: () => {
          // sliders update state only on "change" — read their current position
          state.latencyMs = LAT[+lat.firstChild.value];
          state.jitterMs = JIT[+jit.firstChild.value];
          state.lossPct = LOSS[+loss.firstChild.value];
          apply(state);
        },
      }, T('Apply', 'Применить'))));
}

function brokerCard(id) {
  const status = h('span', { class: 'badge' });
  const kraft = h('span', { class: 'badge' });
  const btn = (label, tip, cls, fn) => h('button', { class: 'icon-btn ' + (cls ?? ''), 'data-tip': tip, onclick: fn }, label);
  let current = null;
  const b = {
    stop: btn('⏹', T('<b>Stop</b> (docker stop → SIGTERM): controlled shutdown — the broker hands off leadership and exits', '<b>Stop</b> (docker stop → SIGTERM): controlled shutdown — брокер сам передаёт лидерство и выходит'), '', () => actions.broker(id, 'stop')),
    kill: btn('💥', T('<b>Kill</b> (SIGKILL): instant death without controlled shutdown — the cluster finds out via the heartbeat timeout', '<b>Kill</b> (SIGKILL): мгновенная смерть без controlled shutdown — кластер узнает по таймауту heartbeat'), 'danger', () => actions.broker(id, 'kill')),
    pause: btn('⏸', T('<b>Pause</b>: freeze the process (like a long GC pause). TCP connections stay open', '<b>Pause</b>: заморозить процесс (как долгая GC-пауза). TCP-соединения остаются открытыми'), '', () => actions.broker(id, 'pause')),
    unpause: btn('▶', T('Unfreeze', 'Разморозить'), '', () => actions.broker(id, 'unpause')),
    net: btn('🌐', T('<b>Network</b>: latency, loss, isolation, split brain', '<b>Сеть</b>: задержка, потери, изоляция, split brain'), '', e => current && openPopover(e.currentTarget, networkPopover(current))),
    start: btn('▶', T('Start the broker', 'Запустить брокер'), '', () => actions.broker(id, 'start')),
  };
  const meta = h('div', { class: 'broker-meta' });
  const badges = h('div', { class: 'chaos-badges' });
  const rows = h('div', {});
  const overlayBig = h('div', { class: 'big' });
  const overlaySmall = h('div', { class: 'small' });
  const overlayBtn = h('button', { class: 'btn btn-primary' });
  const overlay = h('div', { class: 'broker-overlay hidden' }, overlayBig, overlaySmall, overlayBtn);
  const el = h('div', { class: 'card broker', 'data-broker': id },
    h('div', { class: 'card-head' },
      h('span', { class: 'card-title' }, `Broker ${id}`), status, kraft,
      h('div', { class: 'chaos-bar' }, b.stop, b.kill, b.pause, b.unpause, b.net, b.start)),
    meta, badges, rows, overlay);

  return {
    el,
    update(snap) {
      const br = snap.brokers.find(x => x.id === id);
      if (!br) return;
      current = br;
      const [stText, stCls] = STATUS_TEXT[br.status] ?? STATUS_TEXT.unknown;
      setText(status, stText); setClass(status, stCls);
      setClass(el, `card broker st-${br.status}`);

      const voter = br.voter;
      if (br.isQuorumLeader) {
        setText(kraft, T('★ active controller', '★ активный контроллер')); setClass(kraft, 'badge star');
        kraft.dataset.tip = T('Leader of the KRaft Raft quorum: keeps the cluster metadata log, elects partition leaders, fences brokers',
          'Лидер Raft-кворума KRaft: ведёт лог метаданных кластера, выбирает лидеров партиций, фенсит брокеры');
      } else if (voter) {
        const stale = voter.lastFetchAgoMs > 4000;
        setText(kraft, stale
          ? T(`controller · no contact ${Math.round(voter.lastFetchAgoMs / 1000)}s`, `контроллер · нет связи ${Math.round(voter.lastFetchAgoMs / 1000)}с`)
          : T(`controller · lag ${voter.lag}`, `контроллер · lag ${voter.lag}`));
        setClass(kraft, stale ? 'badge warn' : 'badge');
        kraft.dataset.tip = T('A follower in the KRaft quorum: replicates the metadata log (__cluster_metadata) from the active controller',
          'Follower в кворуме KRaft: реплицирует лог метаданных (__cluster_metadata) от активного контроллера');
      } else {
        setText(kraft, T('controller', 'контроллер')); setClass(kraft, 'badge');
      }

      const running = br.containerState === 'running';
      b.stop.classList.toggle('hidden', !running);
      b.kill.classList.toggle('hidden', !(running || br.containerState === 'paused'));
      b.pause.classList.toggle('hidden', !running);
      b.unpause.classList.toggle('hidden', br.containerState !== 'paused');
      b.net.classList.toggle('hidden', !running);
      b.start.classList.toggle('hidden', running || br.containerState === 'paused');

      const cv = store.stats('order-service')?.client?.brokers?.find(x => x.id === id);
      meta.innerHTML = `<span>${esc(br.container)}${br.ip ? ' · ' + esc(br.ip) : ''}</span>` +
        `<span data-tip="${T('How many partitions (of user topics) it leads', 'Сколько партиций (пользовательских топиков) ведёт как лидер')}">${T('leader', 'лидер')}: <b>${br.leaders}</b></span>` +
        `<span data-tip="${T('How many replicas it stores', 'Сколько реплик хранит')}">${T('replicas', 'реплик')}: <b>${br.replicas}</b></span>` +
        `<span data-tip="${T('Total write rate of the partitions this broker leads', 'Сумма скоростей записи в партиции, где этот брокер — лидер')}">${T('in', 'вход')}: <b>${fmtRate(br.inRate)}</b></span>` +
        (cv ? `<span data-tip="${T('How the order-service client (librdkafka) sees this broker', 'Как этого брокера видит клиент order-service (librdkafka)')}">${T('client', 'клиент')}: <b>${cv.state}</b>${cv.state === 'UP' ? ' ' + fmtMs(cv.rttAvgMs) : ''}</span>` : '');

      const ch = br.chaos ?? {};
      const bad = [];
      if (ch.isolated) bad.push([T('✂ full isolation', '✂ полная изоляция'), 'badge err']);
      if (ch.splitFromBrokers) bad.push([T('🔌 cut off from brokers', '🔌 отрезан от брокеров'), 'badge err']);
      if (ch.latencyMs > 0) bad.push([`🐢 +${fmtMs(ch.latencyMs)}${ch.jitterMs ? ' ±' + fmtMs(ch.jitterMs) : ''}`, 'badge warn']);
      if (ch.lossPct > 0) bad.push([T(`📉 ${ch.lossPct}% loss`, `📉 потери ${ch.lossPct}%`), 'badge warn']);
      if (br.status === 'fenced') bad.push([T('the controller removed the broker from the cluster', 'контроллер исключил брокер из кластера'), 'badge fenced']);
      badges.innerHTML = bad.map(([t, c]) => `<span class="${c}">${esc(t)}</span>`).join('');

      renderPartitionRows(rows, snap, id);

      const showOverlay = br.status === 'stopped' || br.status === 'paused';
      overlay.classList.toggle('hidden', !showOverlay);
      if (br.status === 'stopped') {
        setText(overlayBig, T('BROKER STOPPED', 'БРОКЕР ОСТАНОВЛЕН'));
        setText(overlaySmall, `${br.containerStatus ?? ''}. ` + T('Its replicas dropped out of the ISR; leadership moved to other brokers (where possible).',
          'Его реплики выпали из ISR, лидерство переехало на другие брокеры (если было куда).'));
        setText(overlayBtn, T('▶ Start', '▶ Запустить'));
        overlayBtn.onclick = () => actions.broker(id, 'start');
      } else if (br.status === 'paused') {
        setText(overlayBig, T('FROZEN', 'ЗАМОРОЖЕН'));
        setText(overlaySmall, T("docker pause: the process doesn't run. After ~9 s without heartbeats the controller fences the broker.",
          'docker pause: процесс не выполняется. Через ~9 с без heartbeat контроллер зафенсит брокер.'));
        setText(overlayBtn, T('▶ Unfreeze', '▶ Разморозить'));
        overlayBtn.onclick = () => actions.broker(id, 'unpause');
      }
    },
  };
}

function renderPartitionRows(container, snap, brokerId) {
  const topics = sortTopics(snap.topics);
  syncList(container, topics, t => t.name,
    t => {
      const chips = h('div', { class: 'chips' });
      const row = h('div', { class: 'prow' },
        h('div', { class: 'tname', 'data-tip': `${T('Topic', 'Топик')} <b>${esc(t.name)}</b>` }, h('i', { style: `background:${topicColor(t.name)}` }), t.name), chips);
      row.chips = chips;
      return row;
    },
    (row, t) => {
      const minIsr = +(t.config?.['min.insync.replicas'] ?? 1);
      const avg = t.partitions.length ? t.rate / t.partitions.length : 0;
      syncList(row.chips, t.partitions, p => p.id,
        p => h('span', { class: 'pchip', style: `--c:${topicColor(t.name)}` }, String(p.id)),
        (chip, p) => {
          const hosted = p.replicas.includes(brokerId);
          const isLeader = p.leader === brokerId;
          const inIsr = p.isr.includes(brokerId);
          let cls = 'pchip';
          if (!hosted) cls += ' empty';
          else if (p.leader < 0) cls += ' offline';
          else if (isLeader) cls += ' leader' + (p.isr.length < minIsr ? ' underMin' : '') + (p.rate > 20 && p.rate > avg * 2.5 && t.partitions.length > 1 ? ' hot' : '');
          else if (!inIsr) cls += ' out';
          setClass(chip, cls);
          chip.dataset.chip = hosted ? `${brokerId}|${t.name}|${p.id}` : '';
          if (hosted) {
            const role = p.leader < 0 ? T('OFFLINE — no leader', 'OFFLINE — нет лидера')
              : isLeader ? T('LEADER', 'ЛИДЕР')
              : inIsr ? T('follower (in ISR)', 'follower (в ISR)')
              : T('follower OUT OF ISR — lagging or unreachable', 'follower ВНЕ ISR — отстал или недоступен');
            chip.dataset.tip = `<div class="tt-title">${esc(t.name)}-${p.id} ${T('on broker', 'на брокере')} ${brokerId}: ${role}</div>` +
              `<div class="tt-row">${T('leader', 'лидер')}: ${p.leader < 0 ? T('none', 'нет') : p.leader} · ${T('replicas', 'реплики')}: [${p.replicas.join(', ')}] · ISR: [${p.isr.join(', ')}]</div>` +
              `<div class="tt-row">${T('log', 'лог')}: ${p.start ?? '—'} … ${p.end ?? '—'} (${fmtNum((p.end ?? 0) - (p.start ?? 0))} ${T('msgs', 'сообщ.')}) · ${fmtRate(p.rate)}</div>` +
              (p.isr.length < minIsr && p.leader >= 0
                ? `<div class="tt-row" style="color:#fca5a5">ISR ${p.isr.length} &lt; min.insync.replicas ${minIsr}: ${T('acks=all will fail', 'acks=all не пройдёт')}</div>` : '');
          } else {
            delete chip.dataset.tip;
          }
        });
    });
}

// ================================================================== consumer groups

/** Lag measured in time: how many seconds the group needs to drain the tail at the current rate. */
function lagSeconds(lag, rate) {
  if (!lag) return 0;
  return rate > 1 ? lag / rate : Infinity;
}
function lagClass(lag, rate) {
  const secs = lagSeconds(lag, rate);
  if (lag > 1000 && secs > 60) return 'lag-big terrible';
  if (lag > 200 && secs > 10) return 'lag-big bad';
  return 'lag-big';
}

function sparkline(canvas, values, color = '#f59e0b') {
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth, hgt = canvas.clientHeight;
  if (!w) return;
  if (canvas.width !== w * dpr) { canvas.width = w * dpr; canvas.height = hgt * dpr; }
  const ctx = canvas.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, w, hgt);
  const vals = values.slice(-120);
  const max = Math.max(10, ...vals.map(v => v ?? 0));
  ctx.beginPath();
  vals.forEach((v, i) => {
    const x = (i / Math.max(1, vals.length - 1)) * w;
    const y = hgt - 2 - ((v ?? 0) / max) * (hgt - 6);
    i ? ctx.lineTo(x, y) : ctx.moveTo(x, y);
  });
  ctx.strokeStyle = color; ctx.lineWidth = 1.5; ctx.stroke();
  ctx.lineTo(w, hgt); ctx.lineTo(0, hgt); ctx.closePath();
  ctx.fillStyle = color + '22'; ctx.fill();
  ctx.fillStyle = '#7f8aa0'; ctx.font = '10px ui-monospace, monospace';
  ctx.fillText(T(`lag over 2 min, max ${fmtNum(max)}`, `lag за 2 мин, max ${fmtNum(max)}`), 4, 10);
}

function assignChips(assignment) {
  if (!assignment?.length) return [h('span', { class: 'idle-note' }, T('no partitions', 'нет партиций'))];
  return assignment.map(a => h('span', { class: 'achip', style: `--c:${topicColor(a.topic)}`, 'data-tip': `${esc(a.topic)}-${a.partition}` }, `${shortTopic(a.topic)}${a.partition}`));
}
function shortTopic(t) {
  return { orders: 'o', payments: 'p', clickstream: 'c', 'orders.dlq': 'dlq', 'customer-profiles': 'cp' }[t] ?? t.slice(0, 3);
}

const GROUP_STATE_TIP = {
  Stable: T('Stable: partitions are assigned, everyone is consuming', 'Stable: партиции распределены, все читают'),
  PreparingRebalance: T('PreparingRebalance: the coordinator waits for every member to rejoin (JoinGroup)', 'PreparingRebalance: координатор ждёт, пока все участники заново вступят (JoinGroup)'),
  CompletingRebalance: T('CompletingRebalance: the group leader computes the assignment and distributes it (SyncGroup)', 'CompletingRebalance: лидер группы считает назначение и рассылает его (SyncGroup)'),
  Empty: T('Empty: no members, offsets are kept — they can be reset (reset-offsets)', 'Empty: нет участников, offset-ы сохранены — можно сбрасывать (reset-offsets)'),
};

function groupCard(groupId) {
  const serviceName = GROUP_SERVICE[groupId];
  const langBadge = h('span', { class: 'lang' });
  const state = h('span', { class: 'badge' });
  const assignor = h('span', { class: 'badge' });
  const dot = h('span', { class: 'dot' });
  const lag = h('span', { class: 'lag-big' }, '—');
  const info = h('div', { class: 'kv' });
  const spark = h('canvas', { class: 'spark' });
  const members = h('div', { class: 'members' });
  const extra = h('div', {});
  const ctl = serviceName ? containerButtons(() => containerOf(serviceName), {
    stopTip: T('docker stop — every consumer of the process leaves the group gracefully', 'docker stop — все консюмеры процесса корректно выходят из группы'),
    killTip: T('docker kill — every consumer of the process dies at once', 'docker kill — все консюмеры процесса умирают разом'),
  }) : h('span');
  const el = h('div', { class: 'card group', 'data-group': groupId },
    h('div', { class: 'card-head' }, langBadge,
      h('div', { style: 'min-width:0' }, h('div', { class: 'card-title' }, groupId),
        h('div', { class: 'card-sub' }, serviceName ? T(`service ${serviceName}`, `сервис ${serviceName}`) : 'consumer group')),
      dot, ctl),
    h('div', { class: 'ctl-row', style: 'margin-top:6px;gap:8px;flex-wrap:wrap' }, state, assignor),
    h('div', { style: 'display:flex;align-items:baseline;gap:10px;margin-top:6px' },
      h('span', { class: 'muted small', 'data-tip': T('Lag = log end − committed offset (sum over partitions)', 'Lag = конец лога − закоммиченный offset (сумма по партициям)') }, 'lag'), lag, info),
    spark, members, extra);

  const controls = groupId === 'order-processing' ? processorControls() : groupId === 'analytics' ? analyticsControls() : null;
  if (controls) extra.append(controls.el);

  return {
    el,
    update(snap) {
      const g = snap.groups.find(x => x.id === groupId);
      const svcView = serviceName ? store.service(serviceName) : null;
      const inst0 = svcView?.instances?.[0];
      if (serviceName) {
        ctl.sync?.(inst0?.containerState);
        setClass(dot, stateDot(inst0?.containerState, inst0?.ok));
        const l = svcView?.lang;
        setText(langBadge, l ?? '?'); setClass(langBadge, 'lang ' + (LANG_CLASS[l] ?? ''));
      } else {
        setText(langBadge, 'ext'); setClass(langBadge, 'lang');
        dot.className = 'dot';
      }
      if (!g) {
        setText(state, T('no group yet', 'группы ещё нет')); setClass(state, 'badge');
        setText(lag, '—');
        members.innerHTML = '';
        controls?.update(snap, null);
        return;
      }
      const stCls = g.state === 'Stable' ? 'badge ok' : g.state === 'Empty' ? 'badge' : g.state.includes('Rebalance') ? 'badge warn' : 'badge';
      setText(state, g.state); setClass(state, stCls);
      state.dataset.tip = GROUP_STATE_TIP[g.state] ?? g.state;
      setText(assignor, `${g.assignor ?? '—'}${g.type && g.type !== 'Classic' ? ' · ' + g.type : ''} · ${T('coord.', 'коорд.')} b${g.coordinator ?? '?'}`);
      assignor.dataset.tip = T("Partition assignment strategy · group protocol · coordinator broker (leader of this group's __consumer_offsets partition)",
        'Стратегия назначения партиций · протокол группы · брокер-координатор (лидер партиции __consumer_offsets этой группы)');
      setText(lag, fmtNum(g.totalLag)); setClass(lag, lagClass(g.totalLag, g.rate));
      const secs = lagSeconds(g.totalLag, g.rate);
      info.innerHTML =
        `<span data-tip="${T('Seconds needed to drain the tail at the current rate. Lag is measured by committed offsets, so with auto.commit every 5 s it saw-tooths', 'Сколько секунд нужно, чтобы дочитать хвост при текущей скорости. Lag считается по закоммиченным offset-ам, поэтому при auto.commit раз в 5 с он колеблется «пилой»')}">≈ <b>${secs === Infinity ? '∞' : secs < 1 ? '<1' : Math.round(secs)}</b> ${SEC}</span>` +
        `<span>${T('reads', 'читает')} <b>${fmtRate(g.rate)}</b></span><span>${T('members', 'участников')} <b>${g.members.length}</b></span>`;
      sparkline(spark, store.series(`lag:${groupId}`), g.totalLag > 1000 ? '#ef4444' : '#f59e0b');

      renderMembers(members, g, snap);
      controls?.update(snap, g);
    },
  };
}

function renderMembers(container, g, snap) {
  const serviceName = GROUP_SERVICE[g.id];
  const rows = [];
  if (serviceName === 'order-processor') {
    const inst = store.stats('order-processor')?.instances ?? [];
    const byClient = new Map(inst.map(i => [i.clientId, i]));
    for (const m of g.members) {
      const i = byClient.get(m.clientId);
      const alive = i && ['running', 'stuck', 'starting'].includes(i.state);
      rows.push({ key: 'm:' + m.memberId, member: m, inst: alive ? i : null, ghost: !alive });
    }
    const inGroup = new Set(g.members.map(m => m.clientId));
    for (const i of inst) if (!inGroup.has(i.clientId)) rows.push({ key: 'i:' + i.clientId, member: null, inst: i, ghost: false });
  } else if (serviceName === 'analytics') {
    const instances = store.service('analytics')?.instances ?? [];
    const byClient = new Map(instances.filter(i => i.stats).map(i => [i.stats.clientId, i]));
    for (const m of g.members) rows.push({ key: 'm:' + m.memberId, member: m, container: byClient.get(m.clientId), ghost: !byClient.get(m.clientId)?.ok });
    const inGroup = new Set(g.members.map(m => m.clientId));
    for (const i of instances) if (!i.stats || !inGroup.has(i.stats.clientId)) rows.push({ key: 'c:' + i.container, member: null, container: i, ghost: false });
  } else {
    for (const m of g.members) rows.push({ key: 'm:' + m.memberId, member: m });
  }

  container.querySelectorAll('.empty-state').forEach(e => e.remove());
  syncList(container, rows, r => r.key, r => memberRow(g.id, r), (el, r) => el.update(r, g));
  if (!rows.length) container.append(h('div', { class: 'empty-state small' }, T("no members — the group's offsets are kept in __consumer_offsets", 'нет участников — offset-ы группы хранятся в __consumer_offsets')));
}

function memberRow(groupId, initial) {
  const name = h('div', { class: 'mname' });
  const act = h('div', { class: 'mact' });
  const assign = h('div', { class: 'massign' });
  const stats = h('div', { class: 'mstats' });
  const el = h('div', { class: 'member' }, name, act, assign, stats);

  if (groupId === 'order-processing') {
    const id = () => el._row?.inst?.id;
    const b = (label, tip, cls, fn) => h('button', { class: 'icon-btn ' + cls, 'data-tip': tip, onclick: fn }, label);
    act.append(
      b('⏏', T('<b>Close()</b>: commit offsets and leave the group (LeaveGroup) → immediate rebalance', '<b>Close()</b>: закоммитить offset-ы и выйти из группы (LeaveGroup) → ребаланс сразу'), '',
        () => id() && svc.del('order-processor', `instances/${id()}`)),
      b('💥', T('<b>Crash</b>: no commit, no LeaveGroup. The group notices after session.timeout.ms, some messages get processed twice', '<b>Крэш</b>: без коммита и без LeaveGroup. Группа заметит через session.timeout.ms, часть сообщений обработается повторно'), 'danger',
        () => id() && svc.post('order-processor', `instances/${id()}/crash`)),
      b('🧊', T('<b>Hang</b>: stop calling Consume(). Heartbeats keep going, but after max.poll.interval.ms the consumer is kicked out. Click again to unstick it', '<b>Зависание</b>: перестать вызывать Consume(). Heartbeat-ы идут, но через max.poll.interval.ms консюмер вылетит. Нажми ещё раз, чтобы «отвиснуть»'), '',
        () => id() && svc.post('order-processor', `instances/${id()}/stuck`)),
      b('🔄', T('<b>Restart</b>: crash + start with the same client.id after 2 s. With static membership there is no rebalance', '<b>Рестарт</b>: крэш + запуск с тем же client.id через 2 с. Со static membership ребаланса не будет'), '',
        () => id() && svc.post('order-processor', `instances/${id()}/restart`)));
  } else if (groupId === 'analytics') {
    const cont = () => el._row?.container?.container;
    const b = (label, tip, cls, action) => h('button', { class: 'icon-btn ' + cls, 'data-tip': tip, onclick: () => cont() && actions.container(cont(), action) }, label);
    el._buttons = {
      pause: b('⏸', T('docker pause this instance', 'docker pause этого экземпляра'), '', 'pause'),
      unpause: b('▶', 'docker unpause', '', 'unpause'),
      stop: b('⏹', 'docker stop → close() → LeaveGroup', '', 'stop'),
      kill: b('💥', 'docker kill → session.timeout', 'danger', 'kill'),
      start: b('▶', 'docker start', '', 'start'),
    };
    act.append(...Object.values(el._buttons));
  }

  el.update = (r, g) => {
    el._row = r;
    const m = r.member;
    let cls = 'member';
    const label = m?.clientId ?? r.inst?.clientId ?? r.container?.stats?.clientId ?? r.container?.container ?? '?';
    let sub = '';
    if (r.ghost && m) { cls += ' ghost'; sub = T('ghost: the process is gone, waiting for session.timeout', 'призрак: процесса нет, ждём session.timeout'); }
    else if (!m) {
      cls += ' outside';
      sub = T('outside the group', 'вне группы') + ` (${r.inst ? r.inst.state : (r.container?.containerState ?? '?')})`;
    }
    if (r.inst?.state === 'stuck') {
      cls += ' stuck';
      const s = Math.round(r.inst.stuckForMs / 1000);
      sub = T(`stuck for ${s} s — will be kicked by max.poll.interval`, `завис ${s} с — вылетит по max.poll.interval`);
    } else if (r.inst?.state && r.inst.state !== 'running' && m) sub = r.inst.state;
    setClass(el, cls);
    el.dataset.member = m ? `${groupId}|${m.clientId}` : '';
    name.innerHTML = `${esc(label)}${m?.instanceId ? ' <span class="badge" data-tip="static membership: group.instance.id">static</span>' : ''} <span class="small">${esc(sub || m?.host || '')}</span>`;
    const assignment = m?.assignment ?? [];
    assign.replaceChildren(...assignChips(assignment));
    if (m && !assignment.length && g.state === 'Stable') {
      assign.append(h('span', { class: 'idle-note' }, T(' — idle: more consumers than partitions', ' — простаивает: консюмеров больше, чем партиций')));
    }

    if (r.inst) {
      stats.innerHTML = `${T('processed', 'обработано')} ${fmtNum(r.inst.processed)} · ${fmtRate(r.inst.rate)}` +
        (r.inst.duplicates ? ` · <span style="color:var(--warn)">${T('duplicates', 'дублей')} ${fmtNum(r.inst.duplicates)}</span>` : '') +
        (r.inst.dlq ? ` · DLQ ${fmtNum(r.inst.dlq)}` : '');
    } else if (r.container?.stats) {
      const s = r.container.stats;
      stats.innerHTML = Object.entries(s.rates ?? {}).map(([t, v]) => `${esc(t)} ${fmtRate(v)}`).join(' · ') +
        (s.config?.paused ? ` · <b style="color:var(--pause)">${T('paused', 'на паузе')}</b>` : '');
    } else {
      stats.textContent = '';
    }
    if (el._buttons) {
      const st = r.container?.containerState;
      el._buttons.pause.classList.toggle('hidden', st !== 'running');
      el._buttons.unpause.classList.toggle('hidden', st !== 'paused');
      el._buttons.stop.classList.toggle('hidden', st !== 'running');
      el._buttons.kill.classList.toggle('hidden', !(st === 'running' || st === 'paused'));
      el._buttons.start.classList.toggle('hidden', !(st === 'exited' || st === 'created'));
    }
    act.classList.toggle('hidden', groupId === 'order-processing' && !r.inst);
  };
  el.update(initial, { state: '' });
  return el;
}

function processorControls() {
  const name = 'order-processor';
  const set = patch => svc.config(name, patch);
  const add = h('button', { class: 'btn btn-sm btn-primary', 'data-tip': T('Add a consumer to the group → rebalance. Beyond 6 the extra ones sit idle (orders has 6 partitions)', 'Добавить консюмер в группу → ребаланс. Больше 6 — лишние будут простаивать (у orders 6 партиций)') }, T('+ consumer', '+ консюмер'));
  add.onclick = () => svc.post(name, 'instances');
  const delay = stepSlider([0, 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000], 5, v => set({ processingDelayMs: v }), v => v + MS);
  const fail = stepSlider([0, 1, 5, 10, 25, 50, 100], 0, v => set({ failureRatePercent: v }), v => v + '%');
  const strategy = selectBox([
    ['cooperative-sticky', 'cooperative-sticky'], ['range', 'range (eager)'], ['roundrobin', 'roundrobin (eager)'], ['consumer', 'KIP-848: group.protocol=consumer'],
  ], 'cooperative-sticky', v => set({ assignmentStrategy: v }));
  const stat = toggle('static membership', false, v => set({ staticMembership: v }),
    T('group.instance.id = client.id: a restart within session.timeout causes no rebalance', 'group.instance.id = client.id: рестарт в пределах session.timeout без ребаланса'));
  const s6 = T('6 s', '6 с'), s10 = T('10 s', '10 с'), s20 = T('20 s', '20 с'), s30 = T('30 s', '30 с'), s45 = T('45 s', '45 с'), s60 = T('60 s', '60 с');
  const session = selectBox([['6000', s6], ['10000', s10], ['30000', s30], ['45000', s45]], '10000', v => set({ sessionTimeoutMs: +v }));
  const maxPoll = selectBox([['10000', s10], ['20000', s20], ['60000', s60], ['300000', T('5 min', '5 мин')]], '20000', v => set({ maxPollIntervalMs: +v }));
  const commit = selectBox([['100', T('100 ms', '100 мс')], ['1000', T('1 s', '1 с')], ['5000', T('5 s', '5 с')], ['10000', s10]], '5000', v => set({ autoCommitIntervalMs: +v }));
  const totals = h('div', { class: 'kv', style: 'margin-top:6px' });
  const el = h('div', {},
    h('div', { class: 'controls' },
      h('label', {}, ''), h('div', { class: 'ctl-row' }, add, h('span', { class: 'small muted' }, T('consume → process → payments', 'consume → обработка → payments'))),
      h('label', { 'data-tip': T('Time to "process" one order. Throughput ≈ 1000/delay per consumer', 'Время «обработки» одного заказа. Пропускная способность ≈ 1000/задержка на консюмер') }, T('processing', 'обработка')), delay,
      h('label', { 'data-tip': T('Probability of a processing error → retries → DLQ', 'Вероятность ошибки обработки → ретраи → DLQ') }, T('errors', 'ошибки')), fail,
      h('label', { 'data-tip': 'partition.assignment.strategy / group.protocol' }, T('strategy', 'стратегия')), strategy),
    h('details', { class: 'more' }, h('summary', {}, T('⚙ timeouts and commits', '⚙ таймауты и коммиты')),
      h('div', { class: 'controls' },
        h('label', {}, ''), stat,
        h('label', { 'data-tip': T('session.timeout.ms — no heartbeat for longer → the member is dead', 'session.timeout.ms — без heartbeat дольше → участник мёртв') }, 'session.timeout'), session,
        h('label', { 'data-tip': T("max.poll.interval.ms — didn't call Consume() for longer → leaves the group by itself", 'max.poll.interval.ms — не вызывал Consume() дольше → сам выходит из группы') }, 'max.poll.interval'), maxPoll,
        h('label', { 'data-tip': T('auto.commit.interval.ms — the window of possible duplicates after a crash', 'auto.commit.interval.ms — окно возможных дублей после крэша') }, 'auto.commit'), commit)),
    totals);
  return {
    el,
    update() {
      const s = store.stats(name);
      if (!s) return;
      const c = s.config;
      delay.sync(c.processingDelayMs); fail.sync(c.failureRatePercent); strategy.sync(c.assignmentStrategy);
      stat.sync(c.staticMembership); session.sync(c.sessionTimeoutMs); maxPoll.sync(c.maxPollIntervalMs); commit.sync(c.autoCommitIntervalMs);
      const t = s.totals;
      totals.innerHTML = `<span>${T('processed', 'обработано')} <b>${fmtNum(t.processed)}</b></span><span>→ payments <b>${fmtRate(t.payments.rate)}</b></span>` +
        `<span data-tip="${T('Orders processed more than once (at-least-once)', 'Повторно обработанные заказы (at-least-once)')}">${T('duplicates', 'дубли')} <b class="${t.duplicates ? 'bad' : ''}">${fmtNum(t.duplicates)}</b></span>` +
        `<span>${T('retries', 'ретраи')} <b>${fmtNum(t.retries)}</b></span><span>DLQ <b class="${t.dlq ? 'bad' : ''}">${fmtNum(t.dlq)}</b></span>`;
    },
  };
}

function analyticsControls() {
  const name = 'analytics';
  const set = patch => svc.config(name, patch);
  const pause = toggle('pause()', false, v => set({ paused: v }),
    T('consumer.pause(): stop fetching data while staying in the group', 'consumer.pause(): перестать забирать данные, оставаясь в группе'));
  const delay = stepSlider([0, 0.05, 0.1, 0.2, 0.5, 1, 2, 5], 0, v => set({ processingDelayMs: v }), v => v + MS);
  const replay = h('button', { class: 'btn btn-sm btn-warn', 'data-tip': T("Stop analytics → reset the group's offsets to the beginning → start it. The group re-reads the whole history", 'Остановить analytics → сбросить offset-ы группы на начало → запустить. Группа перечитает всю историю') }, T('⏪ Replay history', '⏪ Перечитать историю'));
  replay.onclick = () => replayAnalytics(replay);
  const metrics = h('div', { class: 'box' });
  const el = h('div', {},
    h('div', { class: 'controls' },
      h('label', {}, T('reading', 'чтение')), h('div', { class: 'ctl-row' }, pause, replay),
      h('label', { 'data-tip': T('Processing delay per message (Python)', 'Задержка обработки каждого сообщения (Python)') }, T('processing', 'обработка')), delay),
    h('div', { class: 'small muted', style: 'margin-top:4px' }, T('Scaling: ', 'Масштабирование: '), h('code', {}, 'docker compose up -d --scale analytics=3')),
    metrics);
  return {
    el,
    update() {
      const s = store.stats(name);
      if (!s) return;
      pause.sync(s.config.paused); delay.sync(s.config.processingDelayMs);
      const mtr = s.metrics;
      const all = store.service(name)?.instances?.filter(i => i.ok).map(i => i.stats) ?? [s];
      const sum = k => all.reduce((acc, x) => acc + (x.metrics?.[k] ?? 0), 0);
      const e2e = s.e2eLatencyMs ?? {};
      metrics.innerHTML = `<div class="box-title">${T('📊 Analytics (Python)', '📊 Аналитика (Python)')}</div>` +
        `<div class="kv"><span>${T('orders', 'заказов')} <b>${fmtNum(sum('orders'))}</b></span><span>${T('payments', 'платежей')} <b>${fmtNum(sum('payments'))}</b></span>` +
        Object.entries(mtr.revenue ?? {}).map(([cur, v]) => `<span>${esc(cur)} <b>${fmtNum(v)}</b></span>`).join('') + `</div>` +
        `<div class="kv" style="margin-top:3px">` + Object.entries(e2e).map(([t, v]) =>
          `<span data-tip="${T('End-to-end: from the producer write to being read here (p99 over 5 s)', 'End-to-end: от записи producer-ом до чтения здесь (p99 за 5 с)')}">${esc(t)} e2e <b>${fmtMs(v.p99)}</b></span>`).join('') + `</div>` +
        (mtr.topCustomers?.length ? `<div class="top-list" style="margin-top:3px">${T('top', 'топ')}: ${mtr.topCustomers.slice(0, 3).map(([k, v]) => `${esc(k)} ${fmtNum(v)}`).join(' · ')}</div>` : '');
    },
  };
}

async function replayAnalytics(btn) {
  if (demoGuard()) return;
  const instances = store.service('analytics')?.instances ?? [];
  if (!confirm(T('Stop analytics, reset the "analytics" group offsets to the beginning and start it again?', 'Остановить analytics, сбросить offset-ы группы «analytics» на начало и запустить снова?'))) return;
  btn.disabled = true;
  try {
    toast(T('1/3: stopping the analytics consumers (docker stop)…', '1/3: останавливаем консюмеров analytics (docker stop)…'));
    for (const i of instances) if (i.containerState === 'running' || i.containerState === 'paused') await actions.container(i.container, 'stop');
    const deadline = Date.now() + 30000;
    while (Date.now() < deadline) {
      const g = store.group('analytics');
      if (!g || g.state === 'Empty' || g.members.length === 0) break;
      await new Promise(r => setTimeout(r, 1000));
    }
    toast('2/3: kafka-consumer-groups.sh --reset-offsets --to-earliest --execute');
    const r = await actions.resetGroup('analytics', 'earliest');
    if (r?.ok) toast(T('Offsets reset to the beginning', 'Offset-ы сброшены на начало'), 'ok');
    toast(T('3/3: starting analytics — watch the lag jump and drain', '3/3: запускаем analytics — смотри, как вырастет и рассосётся lag'));
    for (const i of instances) await actions.container(i.container, 'start');
  } finally {
    btn.disabled = false;
  }
}

// ================================================================== init

const cards = { producers: [], brokers: new Map(), groups: new Map() };

export function initLive() {
  const producers = $('#producers');
  cards.producers = [orderServiceCard(), clickstreamCard()];
  for (const c of cards.producers) producers.append(c.el);

  $('#legend').innerHTML =
    `<span class="lg"><span class="pchip leader" style="--c:var(--t-orders);width:18px;height:15px"></span>${T('leader', 'лидер')}</span>` +
    `<span class="lg"><span class="pchip" style="--c:var(--t-orders);width:18px;height:15px"></span>${T('follower in ISR', 'follower в ISR')}</span>` +
    `<span class="lg"><span class="pchip out" style="width:18px;height:15px"></span>${T('out of ISR', 'вне ISR')}</span>` +
    `<span class="lg"><span class="pchip offline" style="width:18px;height:15px"></span>offline</span>` +
    `<span class="lg" data-tip="${T('A partition written noticeably faster than the rest — a hot key', 'Партиция пишется заметно быстрее остальных — горячий ключ')}"><span class="pchip leader hot" style="--c:var(--t-orders);width:18px;height:15px"></span>${T('hot', 'горячая')}</span>`;

  $('#btn-preferred').onclick = () => actions.preferred();
  store.onSnapshot(render);
}

function render(snap) {
  for (const c of cards.producers) c.update(snap);

  const brokersEl = $('#brokers');
  syncList(brokersEl, snap.brokers, b => b.id, b => {
    const card = brokerCard(b.id);
    cards.brokers.set(b.id, card);
    return card.el;
  }, (el, b) => cards.brokers.get(b.id).update(snap));

  const known = ['order-processing', 'analytics'];
  const groups = [...known.map(id => snap.groups.find(g => g.id === id) ?? { id, placeholder: true }),
    ...snap.groups.filter(g => !known.includes(g.id))];
  syncList($('#groups'), groups, g => g.id, g => {
    const card = groupCard(g.id);
    cards.groups.set(g.id, card);
    return card.el;
  }, (el, g) => cards.groups.get(g.id).update(snap));

  const q = snap.cluster.quorum;
  const qErr = isRu ? q?.error : (q?.errorEn ?? q?.error);
  setText($('#cluster-hint'), q?.leaderId ? T(`active controller: node ${q.leaderId}`, `активный контроллер: узел ${q.leaderId}`) : (qErr ?? ''));
}
