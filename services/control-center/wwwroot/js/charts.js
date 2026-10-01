// Простые линейные графики на canvas (без внешних библиотек).
import { h, fmtNum, topicHex } from './util.js';
import { store } from './store.js';

const PALETTE = ['#f5a524', '#60a5fa', '#22c55e', '#c084fc', '#ef4444', '#2dd4bf', '#f472b6', '#facc15'];

const CHARTS = [
  {
    title: 'Запись в топики, сообщений/с',
    keys: () => [...store.history.series.keys()].filter(k => k.startsWith('topic:')),
    label: k => k.slice(6), color: k => topicHex(k.slice(6)),
  },
  {
    title: 'Lag consumer groups, сообщений',
    keys: () => [...store.history.series.keys()].filter(k => k.startsWith('lag:')),
    label: k => k.slice(4),
  },
  {
    title: 'Чтение группами (скорость коммитов), сообщений/с',
    keys: () => [...store.history.series.keys()].filter(k => k.startsWith('grate:')),
    label: k => k.slice(6),
  },
  {
    title: 'Задержка подтверждения записи p99, мс',
    keys: () => [...store.history.series.keys()].filter(k => k.startsWith('lat:')),
    label: k => k.slice(4),
  },
  {
    title: 'Ошибки доставки у producer-ов, /с',
    keys: () => [...store.history.series.keys()].filter(k => k.startsWith('err:')),
    label: k => k.slice(4),
  },
];

let built = false;
const els = [];

export function initCharts() {
  const root = document.getElementById('sub-charts');
  root.append(h('div', { class: 'small muted', style: 'margin-bottom:8px' }, 'Последние 5 минут. Данные собираются, пока открыта страница.'));
  const grid = h('div', { class: 'charts' });
  root.append(grid);
  for (const c of CHARTS) {
    const canvas = h('canvas');
    const legend = h('div', { class: 'chart-legend' });
    grid.append(h('div', { class: 'chart' }, h('h4', {}, c.title), canvas, legend));
    els.push({ def: c, canvas, legend });
  }
  built = true;
  store.onSnapshot(() => {
    if (document.getElementById('sub-charts').classList.contains('active')) renderAll();
  });
}

export function renderAll() {
  if (!built) return;
  for (const e of els) {
    const keys = e.def.keys();
    const series = keys.map((k, i) => ({
      label: e.def.label(k),
      color: e.def.color?.(k) ?? PALETTE[i % PALETTE.length],
      values: store.series(k),
    }));
    drawChart(e.canvas, series);
    e.legend.innerHTML = series.map(s => {
      const lastVal = [...s.values].reverse().find(v => v !== null && v !== undefined);
      return `<span><i style="background:${s.color}"></i>${s.label} <b>${fmtNum(lastVal ?? 0, 1)}</b></span>`;
    }).join('');
  }
}

function drawChart(canvas, series) {
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth, hgt = canvas.clientHeight;
  if (!w) return;
  if (canvas.width !== Math.round(w * dpr) || canvas.height !== Math.round(hgt * dpr)) {
    canvas.width = Math.round(w * dpr);
    canvas.height = Math.round(hgt * dpr);
  }
  const ctx = canvas.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, w, hgt);
  const padL = 46, padR = 8, padT = 6, padB = 16;
  const n = store.history.t.length;
  let max = 0;
  for (const s of series) for (const v of s.values) if (v > max) max = v;
  max = niceMax(max || 1);

  ctx.font = '10px ui-monospace, monospace';
  ctx.fillStyle = '#7f8aa0';
  ctx.strokeStyle = 'rgba(49,64,95,.6)';
  ctx.lineWidth = 1;
  for (let i = 0; i <= 3; i++) {
    const y = padT + (hgt - padT - padB) * (1 - i / 3);
    ctx.beginPath(); ctx.moveTo(padL, y); ctx.lineTo(w - padR, y); ctx.stroke();
    ctx.fillText(fmtNum((max * i) / 3, 1), 2, y + 3);
  }
  const span = Math.max(1, n - 1);
  const secs = Math.round(((store.history.t[n - 1] ?? 0) - (store.history.t[0] ?? 0)) / 1000);
  ctx.fillText(`−${secs}с`, padL, hgt - 3);
  ctx.fillText('сейчас', w - padR - 36, hgt - 3);

  for (const s of series) {
    ctx.beginPath();
    let started = false;
    s.values.forEach((v, i) => {
      if (v === null || v === undefined) { started = false; return; }
      const x = padL + ((w - padL - padR) * i) / span;
      const y = padT + (hgt - padT - padB) * (1 - v / max);
      if (!started) { ctx.moveTo(x, y); started = true; } else ctx.lineTo(x, y);
    });
    ctx.strokeStyle = s.color;
    ctx.lineWidth = 1.6;
    ctx.stroke();
  }
}

function niceMax(v) {
  const p = Math.pow(10, Math.floor(Math.log10(v)));
  for (const m of [1, 2, 2.5, 5, 10]) if (m * p >= v) return m * p;
  return 10 * p;
}
