// Demo mode for the static copy of the site (GitHub Pages): replays a recording of the real lab.
// The recording (demo/recording.json) is made by tools/record-demo.py against a running lab:
// real cluster snapshots once a second + the real event log, not a simulation.
import { h, $, esc } from './util.js';
import { store } from './store.js';
import { setDemoMessages, REPO_URL } from './api.js';
import { T, pick } from './i18n.js';

let rec = null;
let idx = 0;
let timer = null;
let speed = 1;
let playing = true;
let ui = null;

const TS_KEYS = new Set(['ts', 'updatedAt', 'lastErrorTs']);

/** Shift every timestamp of the recording as if the frame was taken "now" — the UI compares them with Date.now(). */
function shifted(value, shift) {
  if (Array.isArray(value)) return value.map(v => shifted(v, shift));
  if (value && typeof value === 'object') {
    const out = {};
    for (const [k, v] of Object.entries(value)) {
      out[k] = TS_KEYS.has(k) && typeof v === 'number' && v > 1e12 ? v + shift : shifted(v, shift);
    }
    return out;
  }
  return value;
}

function mmss(sec) {
  sec = Math.max(0, Math.round(sec));
  return `${String(Math.floor(sec / 60)).padStart(2, '0')}:${String(sec % 60).padStart(2, '0')}`;
}

export async function startDemo() {
  const conn = $('#conn');
  conn.className = 'conn on';
  conn.title = T('Demo: a recording of the real lab', 'Демо: запись реального стенда');
  const bar = $('#demo-bar');
  bar.classList.remove('hidden');
  bar.textContent = T('Loading the lab recording…', 'Загружаем запись стенда…');
  try {
    const r = await fetch('demo/recording.json');
    if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
    rec = await r.json();
  } catch (e) {
    bar.textContent = T(`Failed to load the demo recording: ${e.message}`, `Не удалось загрузить демо-запись: ${e.message}`);
    return;
  }
  setDemoMessages(rec.messages ?? {});
  buildBar(bar);
  seek(0);
}

function buildBar(bar) {
  const playBtn = h('button', { class: 'btn btn-sm', title: T('Pause / resume', 'Пауза / продолжить') }, '⏸');
  const restart = h('button', { class: 'btn btn-sm', title: T('From the start', 'Сначала') }, '⟲');
  const speeds = h('div', { class: 'seg' }, [1, 2, 4].map(s => {
    const b = h('button', { type: 'button', class: s === speed ? 'active' : '' }, `${s}×`);
    b.onclick = () => { speed = s; [...speeds.children].forEach(x => x.classList.toggle('active', x === b)); schedule(); };
    return b;
  }));
  const clock = h('span', { class: 'mono demo-clock' });
  const progress = h('div', { class: 'demo-progress' }, h('i'));
  const total = rec.frames[rec.frames.length - 1].t;
  for (const a of rec.annotations ?? []) {
    const m = h('button', { class: 'demo-mark', style: `left:${(a.t / total) * 100}%`, 'data-tip': `<b>${mmss(a.t)}</b> ${esc(pick(a.title))}` });
    m.onclick = () => seek(rec.frames.findIndex(f => f.t >= a.t));
    progress.append(m);
  }
  progress.addEventListener('click', e => {
    if (e.target.classList.contains('demo-mark')) return;
    const rect = progress.getBoundingClientRect();
    const t = ((e.clientX - rect.left) / rect.width) * total;
    seek(Math.max(0, rec.frames.findIndex(f => f.t >= t)));
  });
  const title = h('div', { class: 'demo-title' });
  const text = h('div', { class: 'demo-text' });
  const repoText = T('repository', 'репозиторий');
  const repo = REPO_URL ? h('a', { href: REPO_URL, target: '_blank', rel: 'noopener' }, repoText) : repoText;

  playBtn.onclick = () => { playing = !playing; playBtn.textContent = playing ? '⏸' : '▶'; schedule(); };
  restart.onclick = () => { playing = true; playBtn.textContent = '⏸'; seek(0); };

  bar.replaceChildren(
    h('div', { class: 'demo-row' },
      h('span', { class: 'badge warn' }, T('DEMO', 'ДЕМО')),
      h('span', { class: 'small' },
        T('A recording of the real lab: Kafka 4.1 (KRaft, 3 nodes) and services in C#, Python, Go. The control buttons do nothing here — to break the cluster yourself, run it locally (see the ',
          'Запись реального стенда: Kafka 4.1 (KRaft, 3 узла) и сервисы на C#, Python, Go. Кнопки управления здесь не работают — чтобы ломать кластер самому, запусти его локально (см. '),
        repo, '): ', h('code', {}, 'docker compose up -d --build')),
    ),
    h('div', { class: 'demo-row' }, playBtn, speeds, restart, clock, progress),
    h('div', { class: 'demo-now' }, title, text),
  );
  ui = { playBtn, clock, progress, title, text, total };
}

function updateBar() {
  const f = rec.frames[idx];
  ui.clock.textContent = `${mmss(f.t)} / ${mmss(ui.total)}`;
  ui.progress.firstChild.style.width = `${(f.t / ui.total) * 100}%`;
  const current = [...(rec.annotations ?? [])].reverse().find(a => a.t <= f.t);
  ui.title.textContent = current ? `${mmss(current.t)} · ${pick(current.title)}` : '';
  ui.text.innerHTML = pick(current?.text) ?? '';
}

function feed(i, silent, shift = Date.now() - rec.frames[i].snap.ts) {
  const f = rec.frames[i];
  store.setSnapshot(shifted(f.snap, shift), silent);
  if (f.events?.length) store.addEvents(shifted(f.events, shift), silent);
}

/** Seeking: the state (chart history, event log) is rebuilt from the start of the recording up to frame i. */
function seek(i) {
  clearTimeout(timer);
  store.reset();
  // the same shift for all "skipped" frames — so the charts keep the intervals between them
  const shift = Date.now() - rec.frames[i].snap.ts;
  for (let k = 0; k < i; k++) feed(k, true, shift);
  store.rebuild();
  idx = i;
  step();
}

function step() {
  feed(idx, false);
  updateBar();
  schedule();
}

function schedule() {
  clearTimeout(timer);
  if (!playing) return;
  if (idx >= rec.frames.length - 1) {
    // the recording is over — start again in a few seconds
    timer = setTimeout(() => seek(0), 6000);
    return;
  }
  const dt = (rec.frames[idx + 1].t - rec.frames[idx].t) * 1000;
  timer = setTimeout(() => { idx++; step(); }, Math.max(100, dt / speed));
}
