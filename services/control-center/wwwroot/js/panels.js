// Bottom panels of the live map: partitions and offsets table, messages, event log, header summary.
import { h, $, $$, esc, fmtNum, fmtRate, fmtTime, topicColor, syncList, toast, setText } from './util.js';
import { store } from './store.js';
import { actions, demoGuard } from './api.js';
import { LEARN } from './learn.js';
import { sortTopics } from './live.js';
import { renderAll as renderCharts } from './charts.js';
import { T, isRu, evText } from './i18n.js';

// ================================================================== header

export function renderHealth(snap) {
  const c = snap.cluster;
  const q = c.quorum;
  const totalLag = snap.groups.reduce((a, g) => a + g.totalLag, 0);
  const pill = (label, value, cls, tip) => `<span class="pill ${cls}" data-tip="${esc(tip)}">${label} <b>${value}</b></span>`;
  $('#health').innerHTML = [
    pill(T('brokers', 'брокеры'), `${c.brokersAlive}/${c.brokersTotal}`, c.brokersAlive === c.brokersTotal ? 'ok' : 'err',
      T('Brokers registered in the cluster (not fenced)', 'Брокеров, зарегистрированных в кластере (не зафенсенных)')),
    pill(T('controller', 'контроллер'), q?.leaderId ? `#${q.leaderId}` : q ? '—' : '…', q?.leaderId || !q ? '' : 'err',
      (isRu ? q?.error : (q?.errorEn ?? q?.error)) ?? T('Active KRaft controller (leader of the metadata Raft quorum)', 'Активный контроллер KRaft (лидер Raft-кворума метаданных)')),
    pill('URP', c.underReplicated, c.underReplicated ? 'warn' : 'ok',
      T('Under-replicated partitions: the ISR is smaller than the replica set. The #1 Kafka operations alert', 'Under-replicated partitions: ISR меньше, чем реплик. Главный алерт эксплуатации Kafka')),
    pill('ISR&lt;min', c.underMinIsr, c.underMinIsr ? 'err' : 'ok',
      T('Partitions where ISR < min.insync.replicas: acks=all writes are impossible', 'Партиции, где ISR < min.insync.replicas: запись с acks=all невозможна')),
    pill('offline', c.offline, c.offline ? 'err' : 'ok',
      T('Partitions without a leader: unavailable for both writes and reads', 'Партиции без лидера: недоступны ни для записи, ни для чтения')),
    pill(T('writes', 'запись'), fmtRate(c.produceRate), '', T('Total log growth rate of all user topics', 'Суммарная скорость роста логов всех пользовательских топиков')),
    pill('lag Σ', fmtNum(totalLag), totalLag > 10000 ? 'err' : totalLag > 1000 ? 'warn' : '', T('Total lag of all consumer groups', 'Суммарный lag всех consumer groups')),
    !c.metadataOk ? pill('metadata', T('down', 'нет'), 'err', c.metadataError ?? '') : '',
    !c.dockerOk ? pill('docker', T('down', 'нет'), 'err', c.dockerError ?? '') : '',
  ].join('');
}

// ================================================================== partitions

export function initPartitions() {
  const root = $('#sub-partitions');
  root.append(h('div', { class: 'small muted', style: 'margin-bottom:6px' },
    T('Each row is a partition: where the leader is, which replicas are in the ISR, where the log starts and ends, and how far behind each consumer group is.',
      'Каждая строка — партиция: где лидер, какие реплики в ISR, где начало и конец лога, и насколько отстаёт каждая consumer group.')));
  const list = h('div', {});
  root.append(list);
  store.onSnapshot(snap => {
    if (!$('#sub-partitions').classList.contains('active')) return;
    renderPartitions(list, snap);
  });
}

function renderPartitions(list, snap) {
  const topics = sortTopics(snap.topics);
  const maxLag = Math.max(100, ...snap.groups.flatMap(g => g.offsets.map(o => o.lag ?? 0)));
  syncList(list, topics, t => t.name, t => {
    const add = h('button', { class: 'btn btn-xs', 'data-tip': T('Increase the partition count (kafka-topics.sh --alter). You can never decrease it!', 'Увеличить число партиций (kafka-topics.sh --alter). Уменьшить нельзя!') }, T('+ partition', '+ партиция'));
    add.onclick = async () => {
      if (demoGuard()) return;
      const cur = store.topic(t.name)?.partitions.length ?? 0;
      if (!confirm(T(`Grow "${t.name}" from ${cur} to ${cur + 1} partitions?\n\nhash(key) % N changes — some keys will start landing in other partitions.`,
        `Увеличить «${t.name}» с ${cur} до ${cur + 1} партиций?\n\nhash(key) % N изменится — часть ключей начнёт попадать в другие партиции.`))) return;
      await actions.addPartitions(t.name, cur + 1);
    };
    const cfg = h('span', { class: 'cfg' });
    const head = h('div', { class: 'topic-head' }, h('span', { class: 'tn' }, h('i', { style: `background:${topicColor(t.name)}` }), t.name), cfg, add);
    const table = h('table', { class: 'grid parts' });
    const wrap = h('div', {}, head, table);
    wrap.cfg = cfg; wrap.table = table;
    return wrap;
  }, (wrap, t) => {
    const c = t.config ?? {};
    setText(wrap.cfg, `RF=${t.replicationFactor} · min.isr=${c['min.insync.replicas'] ?? '?'} · ${c['cleanup.policy'] ?? ''}` +
      (c['retention.ms'] ? ` · retention=${Math.round(+c['retention.ms'] / 60000)} ${T('min', 'мин')}` : '') + ` · ${fmtRate(t.rate)}`);
    const groups = snap.groups.filter(g => g.offsets.some(o => o.topic === t.name));
    const minIsr = +(c['min.insync.replicas'] ?? 1);
    let html = `<colgroup><col style="width:34px"><col style="width:88px"><col style="width:96px"><col style="width:80px"><col style="width:80px"><col style="width:76px">` +
      groups.map(() => '<col>').join('') + (groups.length ? '' : '<col>') + `</colgroup>` +
      `<thead><tr><th>P</th><th>${T('leader', 'лидер')}</th><th>${T('replicas (ISR)', 'реплики (ISR)')}</th><th class="num">${T('start', 'начало')}</th>` +
      `<th class="num">${T('end', 'конец')}</th><th class="num">${T('rate', 'скорость')}</th>` +
      groups.map(g => `<th data-tip="${T(`Lag of group ${esc(g.id)} and the partition owner`, `Lag группы ${esc(g.id)} и владелец партиции`)}">${esc(g.id)}</th>`).join('') +
      (groups.length ? '' : '<th></th>') + `</tr></thead><tbody>`;
    for (const p of t.partitions) {
      const reps = p.replicas.map(r => {
        const cls = r === p.leader ? 'rchip leader' : p.isr.includes(r) ? 'rchip isr' : 'rchip out';
        const role = r === p.leader ? T('leader', 'лидер') : p.isr.includes(r) ? T('in ISR', 'в ISR') : T('OUT OF ISR', 'ВНЕ ISR');
        return `<span class="${cls}" data-tip="${T('broker', 'брокер')} ${r}: ${role}">${r}</span>`;
      }).join('');
      const leaderTxt = p.leader < 0 ? '<span style="color:var(--err)">OFFLINE</span>' : `b${p.leader}`;
      const warn = p.leader >= 0 && p.isr.length < minIsr ? ' <span class="badge err" data-tip="ISR &lt; min.insync.replicas">ISR&lt;min</span>' : '';
      html += `<tr><td class="num">${p.id}</td><td>${leaderTxt}${warn}</td><td>${reps}</td>` +
        `<td class="num">${fmtNum(p.start)}</td><td class="num">${fmtNum(p.end)}</td><td class="num">${fmtRate(p.rate)}</td>`;
      for (const g of groups) {
        const o = g.offsets.find(x => x.topic === t.name && x.partition === p.id);
        const owner = g.members.find(m => m.assignment.some(a => a.topic === t.name && a.partition === p.id));
        const lag = o?.lag;
        const pct = lag ? Math.max(3, (Math.log10(1 + lag) / Math.log10(1 + maxLag)) * 100) : 0;
        const cls = lag > 10000 ? 'high' : lag > 1000 ? 'mid' : '';
        html += `<td><div class="lagcell" data-tip="${T('committed', 'закоммичено')}: ${o?.committed ?? T('none', 'нет')} · ${T('end', 'конец')}: ${p.end ?? '—'} · ${T('reading', 'читают')} ${fmtRate(o?.rate)}">` +
          `<div class="lagbar"><i class="${cls}" style="width:${pct}%"></i></div><span class="lagnum">${lag === undefined || lag === null ? '—' : fmtNum(lag)}</span>` +
          `<span class="owner">${owner ? esc(owner.clientId.replace(/^analytics-/, 'py-')) : '—'}</span></div></td>`;
      }
      html += (groups.length ? '' : '<td></td>') + '</tr>';
    }
    wrap.table.innerHTML = html + '</tbody>';
  });
}

// ================================================================== messages

export function initMessages() {
  const root = $('#sub-messages');
  const topic = h('select', {});
  const partition = h('select', {});
  const limit = h('select', {}, [20, 50, 100].map(n => h('option', { value: n }, T(`last ${n}`, `${n} последних`))));
  const refresh = h('button', { class: 'btn btn-sm btn-primary' }, T('Read', 'Прочитать'));
  const auto = h('label', { class: 'chk' }, h('input', { type: 'checkbox' }), T('auto (3 s)', 'авто (3 с)'));
  const info = h('div', { class: 'small muted' });
  const table = h('table', { class: 'grid msg-table' });

  const fillTopics = () => {
    const names = sortTopics(store.snap?.topics ?? []).map(t => t.name);
    if (topic.options.length === names.length) return;
    const cur = topic.value || 'orders';
    topic.replaceChildren(...names.map(n => h('option', { value: n }, n)));
    topic.value = names.includes(cur) ? cur : names[0];
    fillPartitions();
  };
  const fillPartitions = () => {
    const parts = store.topic(topic.value)?.partitions ?? [];
    const cur = partition.value;
    partition.replaceChildren(h('option', { value: '' }, T('all partitions', 'все партиции')), ...parts.map(p => h('option', { value: p.id }, `${T('partition', 'партиция')} ${p.id}`)));
    partition.value = [...partition.options].some(o => o.value === cur) ? cur : '';
  };
  topic.addEventListener('change', () => { fillPartitions(); load(); });
  partition.addEventListener('change', load);
  limit.addEventListener('change', load);
  refresh.onclick = load;

  let loading = false;
  async function load() {
    if (loading || !topic.value) return;
    loading = true;
    refresh.disabled = true;
    const r = await actions.peek(topic.value, partition.value, limit.value);
    refresh.disabled = false;
    loading = false;
    if (!r || r.ok === false) return;
    info.innerHTML = (r.watermarks ?? []).map(w => `P${w.partition}: [${w.low} … ${w.high})`).join(' · ') +
      (r.errors?.length ? ` · <span style="color:var(--err)">${esc(r.errors.join('; '))}</span>` : '');
    table.innerHTML = `<thead><tr><th>P</th><th class="num">offset</th><th>${T('time', 'время')}</th><th>key</th><th>value</th><th>headers</th></tr></thead><tbody>` +
      r.messages.map(m => `<tr><td class="num">${m.partition}</td><td class="num">${m.offset}</td><td class="mono small">${fmtTime(m.timestamp)}</td>` +
        `<td class="mono">${m.key === null ? '<span class="muted">null</span>' : esc(m.key)}</td>` +
        `<td class="val">${m.value === null ? '<span class="muted">null (tombstone)</span>' : esc(m.value)}</td>` +
        `<td class="hdrs">${Object.entries(m.headers).map(([k, v]) => `${esc(k)}=${esc(v)}`).join('<br>')}</td></tr>`).join('') + '</tbody>';
    if (!r.messages.length) table.innerHTML += `<tr><td colspan="6" class="muted">${T('empty', 'пусто')}</td></tr>`;
  }
  setInterval(() => {
    if (auto.firstChild.checked && $('#sub-messages').classList.contains('active')) load();
  }, 3000);

  // --- produce form ---
  const pTopic = h('select', {});
  const pKey = h('input', { type: 'text', placeholder: T('key (empty = null)', 'key (пусто = null)') });
  const pValue = h('textarea', { placeholder: 'value' });
  const pTomb = h('label', { class: 'chk', 'data-tip': T('value = null: deletes the key in a compacted topic', 'value = null: в compacted-топике удаляет ключ') }, h('input', { type: 'checkbox' }), 'tombstone (null)');
  const pSend = h('button', { class: 'btn btn-sm btn-primary' }, T('Send', 'Отправить'));
  const pResult = h('span', { class: 'result' });
  const fillProduceTopics = () => {
    const names = sortTopics(store.snap?.topics ?? []).map(t => t.name);
    if (pTopic.options.length === names.length) return;
    pTopic.replaceChildren(...names.map(n => h('option', { value: n }, n)));
  };
  const send = async (msg) => {
    const r = await actions.produce(msg);
    if (r?.ok) pResult.textContent = `✓ ${msg.topic}-${r.partition} @ ${r.offset}`;
  };
  pSend.onclick = () => send({
    topic: pTopic.value, key: pKey.value || null,
    value: pTomb.firstChild.checked ? null : pValue.value, note: pTomb.firstChild.checked ? 'compaction' : null,
  });
  const preset = (label, tip, msg) => h('button', { class: 'btn btn-xs', 'data-tip': tip, onclick: () => send(msg) }, label);

  root.append(
    h('div', { class: 'msg-toolbar' }, topic, partition, limit, refresh, auto),
    info, table,
    h('div', { class: 'produce-form' },
      h('label', {}, T('topic', 'топик')), pTopic, h('label', {}, 'key'), pKey,
      h('label', {}, 'value'), pValue,
      h('span', {}), h('div', { class: 'ctl-row', style: 'grid-column: 2 / -1; flex-wrap: wrap' }, pTomb, pSend, pResult)),
    h('div', { class: 'send-row' }, h('span', { class: 'small muted' }, T('Quick:', 'Быстро:')),
      preset('☠ poison pill → orders', T("Broken JSON in orders: order-processor can't parse it and sends it to orders.dlq", 'Битый JSON в orders: order-processor не сможет разобрать и отправит в orders.dlq'),
        { topic: 'orders', key: 'customer-013', value: '{"orderId": oops, not json', note: 'dlq' }),
      preset(T('🧾 profile → customer-profiles', '🧾 профиль → customer-profiles'), T('A new value for key customer-007 in the compacted topic', 'Новое значение для ключа customer-007 в compacted-топике'),
        { topic: 'customer-profiles', key: 'customer-007', value: JSON.stringify({ customerId: 'customer-007', tier: 'vip', note: T('manual edit', 'ручная правка') }), note: 'compaction' }),
      preset('🪦 tombstone customer-007', T('value = null → after compaction the key disappears from the topic', 'value = null → после компакции ключ исчезнет из топика'),
        { topic: 'customer-profiles', key: 'customer-007', value: null, note: 'compaction' }),
    ));

  store.onSnapshot(() => { fillTopics(); fillProduceTopics(); });
  return { load };
}

// ================================================================== event log

const CATEGORY_GROUPS = {
  cluster: ['broker', 'partition', 'controller', 'topic', 'system'],
  group: ['group'],
  service: ['service'],
  action: ['action', 'chaos'],
};

let filter = 'all';
let search = '';

function passes(e) {
  if (filter === 'problems' && !(e.level === 'warn' || e.level === 'error')) return false;
  if (CATEGORY_GROUPS[filter] && !CATEGORY_GROUPS[filter].includes(e.category)) return false;
  if (search && !(`${evText(e)} ${e.source ?? ''}`).toLowerCase().includes(search)) return false;
  return true;
}

function eventEl(e, fresh) {
  const learn = e.learn && LEARN[e.learn];
  const el = h('div', { class: `ev ${e.level}${fresh ? ' fresh' : ''}` },
    h('span', { class: 't' }, fmtTime(e.ts)),
    h('div', {},
      e.source ? h('span', { class: 'src' }, e.source) : h('span', { class: 'src' }, e.category),
      evText(e),
      learn ? h('button', { class: 'why' }, T('why?', 'почему?')) : null));
  if (learn) {
    el.querySelector('.why').onclick = () => {
      const ex = el.querySelector('.explain');
      if (ex) { ex.remove(); return; }
      el.append(h('div', { class: 'explain', html: `<b>${esc(learn.title)}.</b> ${learn.html}` }));
    };
  }
  return el;
}

export function initEvents() {
  const box = $('#events');
  const rerender = () => {
    box.replaceChildren(...store.events.filter(passes).slice(-400).reverse().map(e => eventEl(e, false)));
  };
  $$('#events-filters .chip-btn').forEach(b => b.addEventListener('click', () => {
    $$('#events-filters .chip-btn').forEach(x => x.classList.toggle('active', x === b));
    filter = b.dataset.f;
    rerender();
  }));
  $('#events-search').addEventListener('input', e => { search = e.target.value.trim().toLowerCase(); rerender(); });
  store.onRebuild(rerender);
  store.onEvents(fresh => {
    const visible = fresh.filter(passes);
    for (const e of visible) box.prepend(eventEl(e, true));
    while (box.children.length > 400) box.lastChild.remove();
  });
}

// ================================================================== bottom panel tabs

export function initSubtabs() {
  $$('.subtab').forEach(b => b.addEventListener('click', () => {
    $$('.subtab').forEach(x => x.classList.toggle('active', x === b));
    $$('.sub-page').forEach(p => p.classList.toggle('active', p.id === 'sub-' + b.dataset.sub));
    if (b.dataset.sub === 'charts') renderCharts();
    if (b.dataset.sub === 'partitions' && store.snap) renderPartitions($('#sub-partitions').lastChild, store.snap);
  }));
}

export function showSubtab(name) {
  $(`.subtab[data-sub="${name}"]`)?.click();
}

export function heal() {
  return actions.healAll().then(r => {
    if (r?.ok) toast(T('Healing started: ', 'Починка запущена: ') + (r.log?.join(', ') || T('everything is already fine', 'всё и так в порядке')), 'ok', 6000);
  });
}
