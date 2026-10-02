// HTTP-вызовы к control-center и SSE-подписка.
// Пути относительные: UI работает и в корне (локальный стенд), и в подкаталоге (GitHub Pages: /kafka-lab/).
import { toast } from './util.js';
import { store } from './store.js';
import { T, isRu } from './i18n.js';

/** Демо-режим: статическая копия сайта (GitHub Pages) проигрывает запись реального стенда — бэкенда нет. */
export const DEMO = window.KAFKA_LAB?.mode === 'demo';
export const REPO_URL = window.KAFKA_LAB?.repo ?? '';

let demoMessages = null; // записанные сообщения топиков — для вкладки «Сообщения» в демо-режиме
export function setDemoMessages(messages) { demoMessages = messages; }

export function demoNotice() {
  toast(T('This is a recording of the real lab: you can control the cluster only in the local lab (docker compose up -d --build).',
    'Это запись реального стенда: управлять кластером можно только на локальном стенде (docker compose up -d --build).'), 'info', 6000);
}

/** В демо-режиме показывает подсказку и возвращает true — действие выполнять не нужно. */
export function demoGuard() {
  if (!DEMO) return false;
  demoNotice();
  return true;
}

export async function api(method, url, body, { quiet = false } = {}) {
  if (DEMO) return demoApi(method, url, quiet);
  try {
    const r = await fetch(url, {
      method,
      headers: body !== undefined ? { 'content-type': 'application/json' } : {},
      body: body !== undefined ? JSON.stringify(body) : undefined,
    });
    let data = null;
    try { data = await r.json(); } catch { /* пустой ответ */ }
    if (!r.ok || (data && data.ok === false)) {
      const msg = (isRu ? data?.error : (data?.errorEn ?? data?.error)) || data?.reason || `${r.status} ${r.statusText}`;
      if (!quiet) toast(msg, 'err', 7000);
      return data ?? { ok: false, error: msg };
    }
    return data ?? { ok: true };
  } catch (e) {
    if (!quiet) toast(`${T('Network', 'Сеть')}: ${e.message}`, 'err');
    return { ok: false, error: e.message };
  }
}

function demoApi(method, url, quiet) {
  const peek = url.match(/^api\/topics\/([^/?]+)\/messages\?limit=(\d+)(?:&partition=(\d+))?/);
  if (method === 'GET' && peek) {
    const topic = decodeURIComponent(peek[1]);
    const rec = demoMessages?.[topic] ?? { messages: [], watermarks: [], errors: [] };
    const partition = peek[3] === undefined ? null : +peek[3];
    return {
      topic,
      errors: rec.errors ?? [],
      watermarks: (rec.watermarks ?? []).filter(w => partition === null || w.partition === partition),
      messages: (rec.messages ?? []).filter(m => partition === null || m.partition === partition).slice(0, +peek[2]),
    };
  }
  if (!quiet) demoNotice();
  return { ok: false, error: 'demo' };
}

export const svc = {
  config: (service, patch) => api('PUT', `api/svc/${service}/config`, patch),
  post: (service, path, body) => api('POST', `api/svc/${service}/${path}`, body ?? {}),
  del: (service, path) => api('DELETE', `api/svc/${service}/${path}`),
};

export const actions = {
  broker: (id, action) => api('POST', `api/brokers/${id}/${action}`),
  network: (id, chaos) => api('POST', `api/brokers/${id}/network`, chaos),
  container: (name, action) => api('POST', `api/containers/${name}/${action}`),
  healAll: () => api('POST', 'api/heal-all'),
  preferred: () => api('POST', 'api/leaders/preferred'),
  resetGroup: (group, to, shiftBy) => api('POST', `api/groups/${group}/reset`, { to, shiftBy }),
  addPartitions: (topic, count) => api('POST', `api/topics/${topic}/partitions`, { count }),
  topicConfig: (topic, key, value) => api('POST', `api/topics/${topic}/config`, { key, value }),
  produce: (msg) => api('POST', 'api/produce', msg),
  peek: (topic, partition, limit) =>
    api('GET', `api/topics/${encodeURIComponent(topic)}/messages?limit=${limit}` + (partition !== '' && partition !== null && partition !== undefined ? `&partition=${partition}` : '')),
};

export function connectStream() {
  if (DEMO) {
    import('./demo.js').then(m => m.startDemo());
    return;
  }
  let es;
  const conn = document.getElementById('conn');
  const open = () => {
    es = new EventSource('api/stream');
    es.addEventListener('snapshot', e => {
      store.connected = true;
      conn.className = 'conn on';
      conn.title = T('Connected to control-center', 'Соединение с control-center: OK');
      store.setSnapshot(JSON.parse(e.data));
    });
    es.addEventListener('events', e => store.addEvents(JSON.parse(e.data)));
    es.onerror = () => {
      store.connected = false;
      conn.className = 'conn off';
      conn.title = T('No connection to control-center — reconnecting…', 'Нет соединения с control-center — переподключаемся…');
      es.close();
      setTimeout(open, 2000);
    };
  };
  open();
}
