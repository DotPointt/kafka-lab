// Анимация потоков сообщений поверх схемы: producer → лидер партиции → (реплики) → консюмер.
// Скорость появления частиц ~ логарифм реальной скорости (msg/s), цвет = топик.
import { store } from './store.js';
import { topicHex } from './util.js';

const TOPIC_PRODUCER = {
  'orders': 'svc:order-service',
  'customer-profiles': 'svc:order-service',
  'clickstream': 'svc:clickstream-generator',
  'payments': 'group:order-processing',
  'orders.dlq': 'group:order-processing',
};

let canvas, ctx, grid;
let flows = [];
let particles = [];
let last = performance.now();
let enabled = true;
let replication = true;

export function initFlow() {
  canvas = document.getElementById('flow');
  grid = document.getElementById('live-grid');
  ctx = canvas.getContext('2d');
  const chk = document.getElementById('chk-flow');
  const rep = document.getElementById('chk-replication');
  try {
    enabled = localStorage.getItem('flow') !== '0';
    replication = localStorage.getItem('replication') !== '0';
  } catch { /* storage недоступен */ }
  chk.checked = enabled;
  rep.checked = replication;
  chk.addEventListener('change', () => { enabled = chk.checked; try { localStorage.setItem('flow', enabled ? '1' : '0'); } catch { } });
  rep.addEventListener('change', () => { replication = rep.checked; try { localStorage.setItem('replication', replication ? '1' : '0'); } catch { } });
  new ResizeObserver(resize).observe(grid);
  resize();
  store.onSnapshot(() => requestAnimationFrame(computeFlows));
  requestAnimationFrame(frame);
}

function resize() {
  const dpr = window.devicePixelRatio || 1;
  canvas.width = grid.clientWidth * dpr;
  canvas.height = grid.clientHeight * dpr;
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
}

function center(el, side) {
  const g = grid.getBoundingClientRect();
  const r = el.getBoundingClientRect();
  const x = side === 'right' ? r.right - 4 : side === 'left' ? r.left + 4 : r.left + r.width / 2;
  return { x: x - g.left, y: r.top + r.height / 2 - g.top };
}

function sourceFor(topic) {
  const node = TOPIC_PRODUCER[topic];
  if (!node) return null;
  const [kind, name] = node.split(':');
  return kind === 'svc' ? grid.querySelector(`[data-node="${node}"]`) : grid.querySelector(`[data-group="${name}"]`);
}

function particlesPerSec(rate) {
  if (!rate || rate <= 0) return 0;
  return Math.min(12, 0.6 + 2.6 * Math.log10(1 + rate));
}

/** Пересчитать список потоков по свежему снимку (раз в секунду). */
function computeFlows() {
  const snap = store.snap;
  if (!snap || !enabled) { flows = []; return; }
  const next = [];
  const chips = new Map();
  for (const el of grid.querySelectorAll('[data-chip]')) if (el.dataset.chip) chips.set(el.dataset.chip, el);

  for (const t of snap.topics) {
    if (t.internal) continue;
    const src = sourceFor(t.name);
    const color = topicHex(t.name);
    for (const p of t.partitions) {
      if (p.leader < 0 || !p.rate) continue;
      const leaderChip = chips.get(`${p.leader}|${t.name}|${p.id}`);
      if (!leaderChip) continue;
      const to = center(leaderChip);
      if (src) {
        const groupSource = src.dataset.group !== undefined;
        const from = center(src, groupSource ? 'left' : 'right');
        next.push({ from, to, pps: particlesPerSec(p.rate), color, size: 2.2, acc: 0 });
      }
      if (replication) {
        for (const r of p.isr) {
          if (r === p.leader) continue;
          const f = chips.get(`${r}|${t.name}|${p.id}`);
          if (f) next.push({ from: to, to: center(f), pps: particlesPerSec(p.rate) * 0.4, color, size: 1.4, alpha: 0.5, acc: 0, curve: 0.3 });
        }
      }
    }
  }

  for (const g of snap.groups) {
    for (const o of g.offsets) {
      if (!o.rate) continue;
      const t = snap.topics.find(x => x.name === o.topic);
      const p = t?.partitions.find(x => x.id === o.partition);
      if (!p || p.leader < 0) continue;
      const leaderChip = chips.get(`${p.leader}|${o.topic}|${o.partition}`);
      const owner = g.members.find(m => m.assignment.some(a => a.topic === o.topic && a.partition === o.partition));
      const target = (owner && grid.querySelector(`[data-member="${CSS.escape(`${g.id}|${owner.clientId}`)}"]`)) || grid.querySelector(`[data-group="${CSS.escape(g.id)}"]`);
      if (!leaderChip || !target) continue;
      next.push({ from: center(leaderChip), to: center(target, 'left'), pps: particlesPerSec(o.rate), color: topicHex(o.topic), size: 2.2, acc: 0 });
    }
  }

  // сохранить накопленный «остаток» частиц для похожих потоков, чтобы не было рывков
  flows = next;
}

function frame(now) {
  const dt = Math.min(0.1, (now - last) / 1000);
  last = now;
  const w = grid.clientWidth, hgt = grid.clientHeight;
  ctx.clearRect(0, 0, w, hgt);
  if (enabled && !document.hidden) {
    for (const f of flows) {
      f.acc += f.pps * dt;
      while (f.acc >= 1) {
        f.acc -= 1;
        if (particles.length < 900) {
          const dx = f.to.x - f.from.x, dy = f.to.y - f.from.y;
          const len = Math.hypot(dx, dy) || 1;
          const bend = (f.curve ?? 0.06) * len * (Math.random() * 0.6 + 0.7) * (Math.random() < 0.5 ? -1 : 1);
          particles.push({
            x0: f.from.x, y0: f.from.y, x1: f.to.x, y1: f.to.y,
            cx: (f.from.x + f.to.x) / 2 - (dy / len) * bend, cy: (f.from.y + f.to.y) / 2 + (dx / len) * bend,
            t: 0, dur: 0.85 + Math.random() * 0.35, color: f.color, size: f.size, alpha: f.alpha ?? 0.85,
          });
        }
      }
    }
    for (const p of particles) {
      p.t += dt / p.dur;
      const t = Math.min(1, p.t), u = 1 - t;
      const x = u * u * p.x0 + 2 * u * t * p.cx + t * t * p.x1;
      const y = u * u * p.y0 + 2 * u * t * p.cy + t * t * p.y1;
      ctx.globalAlpha = p.alpha * (t < 0.1 ? t / 0.1 : t > 0.85 ? (1 - t) / 0.15 : 1);
      ctx.fillStyle = p.color;
      ctx.beginPath();
      ctx.arc(x, y, p.size, 0, Math.PI * 2);
      ctx.fill();
    }
    ctx.globalAlpha = 1;
    particles = particles.filter(p => p.t < 1);
  } else {
    particles = [];
  }
  requestAnimationFrame(frame);
}
