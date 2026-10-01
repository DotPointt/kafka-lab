// Состояние приложения: последний снимок от control-center, история для графиков, лента событий.

const HISTORY = 300; // 5 минут при тике 1 с

export const store = {
  snap: null,
  prev: null,
  events: [],
  history: { t: [], series: new Map() },
  connected: false,
  listeners: new Set(),
  eventListeners: new Set(),
  rebuildListeners: new Set(),

  onSnapshot(fn) { this.listeners.add(fn); },
  onEvents(fn) { this.eventListeners.add(fn); },
  /** Вызывается после reset()/перемотки демо-записи: перерисовать всё по текущему состоянию стора. */
  onRebuild(fn) { this.rebuildListeners.add(fn); },

  /** silent = true: обновить состояние без перерисовки (используется при перемотке демо-записи). */
  setSnapshot(s, silent = false) {
    this.prev = this.snap;
    this.snap = s;
    recordHistory(this, s);
    if (silent) return;
    for (const fn of this.listeners) {
      try { fn(s, this.prev); } catch (e) { console.error(e); }
    }
  },

  addEvents(list, silent = false) {
    const known = new Set(this.events.map(e => e.id));
    const fresh = list.filter(e => !known.has(e.id));
    if (!fresh.length) return;
    this.events.push(...fresh);
    if (this.events.length > 1500) this.events.splice(0, this.events.length - 1500);
    if (silent) return;
    for (const fn of this.eventListeners) {
      try { fn(fresh); } catch (e) { console.error(e); }
    }
  },

  /** Полностью очистить состояние (демо-запись начинается заново). */
  reset() {
    this.snap = null;
    this.prev = null;
    this.events = [];
    this.history = { t: [], series: new Map() };
    this.rebuild();
  },

  rebuild() {
    for (const fn of this.rebuildListeners) {
      try { fn(); } catch (e) { console.error(e); }
    }
  },

  service(name) {
    return this.snap?.services?.find(s => s.name === name) ?? null;
  },

  /** Статистика первого работающего экземпляра сервиса. */
  stats(name) {
    const s = this.service(name);
    return s?.instances?.find(i => i.ok)?.stats ?? null;
  },

  group(id) {
    return this.snap?.groups?.find(g => g.id === id) ?? null;
  },

  topic(name) {
    return this.snap?.topics?.find(t => t.name === name) ?? null;
  },

  series(key) {
    return this.history.series.get(key) ?? [];
  },
};

function push(h, key, value) {
  let arr = h.series.get(key);
  if (!arr) {
    arr = new Array(h.t.length - 1).fill(null);
    h.series.set(key, arr);
  }
  arr.push(value ?? null);
}

function recordHistory(st, s) {
  const h = st.history;
  h.t.push(s.ts);
  const touched = new Set();
  const rec = (k, v) => { push(h, k, v); touched.add(k); };

  for (const t of s.topics ?? []) if (!t.internal) rec(`topic:${t.name}`, t.rate);
  for (const g of s.groups ?? []) {
    rec(`lag:${g.id}`, g.totalLag);
    rec(`grate:${g.id}`, g.rate);
  }
  for (const svc of s.services ?? []) {
    const st0 = svc.instances?.find(i => i.ok)?.stats;
    const p = st0?.producer;
    if (p) {
      rec(`lat:${svc.name}`, p.latencyMs?.p99 ?? null);
      rec(`err:${svc.name}`, p.rates?.failed ?? 0);
      rec(`ack:${svc.name}`, p.rates?.acked ?? 0);
    }
  }
  // выровнять серии, которых не было в этом тике
  for (const [k, arr] of h.series) if (!touched.has(k)) arr.push(null);

  if (h.t.length > HISTORY) {
    const cut = h.t.length - HISTORY;
    h.t.splice(0, cut);
    for (const arr of h.series.values()) arr.splice(0, cut);
  }
}
