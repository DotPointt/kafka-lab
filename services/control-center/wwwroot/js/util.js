// Small helpers: DOM creation, formatting, murmur2 (same as Kafka), toasts, tooltips.
import { T, locale } from './i18n.js';

/** h('div', {class: 'x', onclick: fn}, child1, 'text', ...) */
export function h(tag, attrs, ...children) {
  const el = document.createElement(tag);
  if (attrs) {
    for (const [k, v] of Object.entries(attrs)) {
      if (v === undefined || v === null || v === false) continue;
      if (k === 'class') el.className = v;
      else if (k === 'style' && typeof v === 'object') Object.assign(el.style, v);
      else if (k.startsWith('on') && typeof v === 'function') el.addEventListener(k.slice(2), v);
      else if (k === 'html') el.innerHTML = v;
      else if (k === 'dataset') Object.assign(el.dataset, v);
      else el.setAttribute(k, v === true ? '' : v);
    }
  }
  for (const c of children.flat(Infinity)) {
    if (c === null || c === undefined || c === false) continue;
    el.append(c instanceof Node ? c : document.createTextNode(String(c)));
  }
  return el;
}

export const $ = (sel, root = document) => root.querySelector(sel);
export const $$ = (sel, root = document) => [...root.querySelectorAll(sel)];

export function esc(s) {
  return String(s ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

/** Установить textContent, только если изменился (меньше перерисовок). */
export function setText(el, text) {
  const s = String(text);
  if (el && el.textContent !== s) el.textContent = s;
}

export function setClass(el, cls) {
  if (el && el.className !== cls) el.className = cls;
}

export function fmtNum(n, digits = 0) {
  if (n === null || n === undefined || Number.isNaN(n)) return '—';
  const abs = Math.abs(n);
  if (abs >= 1e9) return (n / 1e9).toFixed(1) + 'B';
  if (abs >= 1e6) return (n / 1e6).toFixed(abs >= 1e7 ? 1 : 2) + 'M';
  if (abs >= 1e4) return (n / 1e3).toFixed(abs >= 1e5 ? 0 : 1) + 'k';
  return Number(n).toLocaleString(locale, { maximumFractionDigits: digits });
}

const PER_SEC = T('/s', '/с');
const SEC = T('s', 'с');
const MS = T('ms', 'мс');

export function fmtRate(n) {
  if (!n) return '0' + PER_SEC;
  if (n >= 1000) return (n / 1000).toFixed(n >= 10000 ? 0 : 1) + 'k' + PER_SEC;
  return fmtNum(n, n < 10 ? 1 : 0) + PER_SEC;
}

export function fmtMs(n) {
  if (n === null || n === undefined) return '—';
  if (n >= 10000) return (n / 1000).toFixed(1) + SEC;
  if (n >= 1000) return (n / 1000).toFixed(2) + SEC;
  return (n < 10 ? n.toFixed(1) : Math.round(n)) + MS;
}

export function fmtBytes(n) {
  if (!n) return '0 B';
  if (n >= 1 << 30) return (n / (1 << 30)).toFixed(2) + ' GB';
  if (n >= 1 << 20) return (n / (1 << 20)).toFixed(2) + ' MB';
  if (n >= 1 << 10) return (n / (1 << 10)).toFixed(1) + ' KB';
  return n + ' B';
}

export function fmtTime(ts) {
  const d = new Date(ts);
  return d.toLocaleTimeString(locale, { hour12: false });
}

// ---------- цвета топиков ----------
const TOPIC_COLORS = {
  'orders': 'var(--t-orders)',
  'payments': 'var(--t-payments)',
  'clickstream': 'var(--t-clickstream)',
  'orders.dlq': 'var(--t-dlq)',
  'customer-profiles': 'var(--t-profiles)',
};
const TOPIC_HEX = {
  'orders': '#f5a524', 'payments': '#22c55e', 'clickstream': '#60a5fa', 'orders.dlq': '#ef4444', 'customer-profiles': '#c084fc',
};
const EXTRA = ['#f472b6', '#2dd4bf', '#facc15', '#fb7185', '#a3e635'];
export function topicColor(name) {
  return TOPIC_COLORS[name] ?? EXTRA[hashStr(name) % EXTRA.length];
}
export function topicHex(name) {
  return TOPIC_HEX[name] ?? EXTRA[hashStr(name) % EXTRA.length];
}
function hashStr(s) { let x = 0; for (const c of s) x = (x * 31 + c.charCodeAt(0)) | 0; return Math.abs(x); }

export const LANG_CLASS = { 'C#': 'cs', 'Python': 'py', 'Go': 'go', 'Java': 'java' };

// ---------- murmur2: тот же хеш, что использует Java-клиент Kafka (и librdkafka с partitioner=murmur2) ----------
export function murmur2(str) {
  const data = new TextEncoder().encode(str);
  const length = data.length;
  const seed = 0x9747b28c;
  const m = 0x5bd1e995;
  const r = 24;
  let h = seed ^ length;
  const length4 = Math.floor(length / 4);
  for (let i = 0; i < length4; i++) {
    const i4 = i * 4;
    let k = (data[i4] & 0xff) + ((data[i4 + 1] & 0xff) << 8) + ((data[i4 + 2] & 0xff) << 16) + ((data[i4 + 3] & 0xff) << 24);
    k = Math.imul(k, m);
    k ^= k >>> r;
    k = Math.imul(k, m);
    h = Math.imul(h, m);
    h ^= k;
  }
  const rest = length % 4;
  const base = length & ~3;
  if (rest === 3) h ^= (data[base + 2] & 0xff) << 16;
  if (rest >= 2) h ^= (data[base + 1] & 0xff) << 8;
  if (rest >= 1) {
    h ^= data[base] & 0xff;
    h = Math.imul(h, m);
  }
  h ^= h >>> 13;
  h = Math.imul(h, m);
  h ^= h >>> 15;
  return h | 0;
}

/** partition = toPositive(murmur2(key)) % numPartitions */
export function partitionFor(key, numPartitions) {
  return (murmur2(key) & 0x7fffffff) % numPartitions;
}

// ---------- тосты ----------
/** content — строка или узлы (массив: текст + ссылка). Пока курсор над тостом, он не исчезает — ссылку можно успеть нажать. */
export function toast(content, kind = 'info', ms = 4500) {
  const box = $('#toasts');
  const el = h('div', { class: `toast ${kind}` }, content);
  box.append(el);
  let timer = setTimeout(() => el.remove(), ms);
  el.addEventListener('mouseenter', () => clearTimeout(timer));
  el.addEventListener('mouseleave', () => { timer = setTimeout(() => el.remove(), 2000); });
}

// ---------- тултип (делегирование по data-tip) ----------
export function initTooltips() {
  const tip = $('#tooltip');
  let current = null;
  document.addEventListener('mouseover', e => {
    const t = e.target.closest('[data-tip]');
    if (t === current) return;
    current = t;
    if (!t) { tip.style.display = 'none'; return; }
    tip.innerHTML = t.dataset.tip;
    tip.style.display = 'block';
    position(e);
  });
  document.addEventListener('mousemove', e => { if (current) position(e); });
  function position(e) {
    const pad = 14;
    const w = tip.offsetWidth, hgt = tip.offsetHeight;
    let x = e.clientX + pad, y = e.clientY + pad;
    if (x + w > window.innerWidth - 8) x = e.clientX - w - pad;
    if (y + hgt > window.innerHeight - 8) y = e.clientY - hgt - pad;
    tip.style.left = x + 'px';
    tip.style.top = y + 'px';
  }
}

// ---------- поповер ----------
export function openPopover(anchor, content) {
  const pop = $('#popover');
  pop.innerHTML = '';
  pop.append(content);
  pop.style.display = 'block';
  const r = anchor.getBoundingClientRect();
  let x = r.left, y = r.bottom + 6;
  if (x + pop.offsetWidth > window.innerWidth - 10) x = window.innerWidth - pop.offsetWidth - 10;
  if (y + pop.offsetHeight > window.innerHeight - 10) y = r.top - pop.offsetHeight - 6;
  pop.style.left = Math.max(10, x) + 'px';
  pop.style.top = Math.max(10, y) + 'px';
  setTimeout(() => document.addEventListener('mousedown', closeOnOutside), 0);
}
export function closePopover() {
  $('#popover').style.display = 'none';
  document.removeEventListener('mousedown', closeOnOutside);
}
function closeOnOutside(e) {
  if (!e.target.closest('#popover')) closePopover();
}

/** Ступенчатый слайдер: позиция → значение из списка (удобно для 0…50 000). */
export function stepSlider(steps, value, onChange, format = v => v) {
  const idx = nearestIndex(steps, value);
  const input = h('input', { type: 'range', min: 0, max: steps.length - 1, step: 1, value: idx });
  const label = h('span', { class: 'val' }, format(steps[idx]));
  input.addEventListener('input', () => setText(label, format(steps[+input.value])));
  input.addEventListener('change', () => onChange(steps[+input.value]));
  const wrap = h('div', { class: 'ctl-row' }, input, label);
  wrap.sync = v => {
    if (document.activeElement === input) return;
    const i = nearestIndex(steps, v);
    if (+input.value !== i) input.value = i;
    setText(label, format(v));
  };
  return wrap;
}
function nearestIndex(steps, v) {
  let best = 0;
  for (let i = 0; i < steps.length; i++) if (Math.abs(steps[i] - v) < Math.abs(steps[best] - v)) best = i;
  return best;
}

/** Сегментированный переключатель. */
export function segmented(options, value, onChange) {
  const wrap = h('div', { class: 'seg' });
  const buttons = options.map(([val, label, tip]) => {
    const b = h('button', { type: 'button', 'data-tip': tip, class: val === value ? 'active' : '' }, label);
    b.addEventListener('click', () => onChange(val));
    wrap.append(b);
    return [val, b];
  });
  wrap.sync = v => buttons.forEach(([val, b]) => b.classList.toggle('active', val === v));
  return wrap;
}

export function selectBox(options, value, onChange) {
  const s = h('select', {}, options.map(([val, label]) => h('option', { value: val }, label)));
  s.value = value;
  s.addEventListener('change', () => onChange(s.value));
  s.sync = v => { if (document.activeElement !== s && s.value !== String(v)) s.value = v; };
  return s;
}

export function toggle(label, value, onChange, tip) {
  const input = h('input', { type: 'checkbox' });
  input.checked = !!value;
  input.addEventListener('change', () => onChange(input.checked));
  const el = h('label', { class: 'chk', 'data-tip': tip }, input, label);
  el.sync = v => { if (input.checked !== !!v) input.checked = !!v; };
  return el;
}

/** Синхронизировать дочерние элементы по ключу: создаёт новые, удаляет исчезнувшие, сохраняет порядок. */
export function syncList(container, items, keyFn, create, update) {
  const existing = new Map();
  for (const el of [...container.children]) if (el.dataset.key !== undefined) existing.set(el.dataset.key, el);
  const seen = new Set();
  let prev = null;
  for (const item of items) {
    const key = String(keyFn(item));
    seen.add(key);
    let el = existing.get(key);
    if (!el) {
      el = create(item);
      el.dataset.key = key;
    }
    update?.(el, item);
    const expectedNext = prev ? prev.nextSibling : container.firstChild;
    if (expectedNext !== el) container.insertBefore(el, expectedNext);
    prev = el;
  }
  for (const [key, el] of existing) if (!seen.has(key)) el.remove();
}
