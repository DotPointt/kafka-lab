// Карточки для запоминания (интервальное повторение по системе Лейтнера, прогресс в localStorage).
import { h, $ } from './util.js';
import { CARDS } from './flashcards-data.js';

const BOXES = 5; // 0 — новые/не знаю … 4 — выучено
let progress = {};
let category = 'все';
let card = null;
let flipped = false;
let mode = 'learn';
let built = false;

function load() {
  try { progress = JSON.parse(localStorage.getItem('cards-progress') ?? '{}'); } catch { progress = {}; }
}
function save() {
  try { localStorage.setItem('cards-progress', JSON.stringify(progress)); } catch { /* приватный режим */ }
}
const idOf = c => c.q;
const boxOf = c => progress[idOf(c)]?.box ?? 0;

function pool() {
  return CARDS.filter(c => category === 'все' || c.cat === category);
}

function pick() {
  const cards = pool();
  if (!cards.length) return null;
  // чаще показываем карточки из младших коробок, но не повторяем ту же подряд
  const weights = cards.map(c => (c === card ? 0.01 : Math.pow(2.4, BOXES - 1 - boxOf(c))));
  let r = Math.random() * weights.reduce((a, b) => a + b, 0);
  for (let i = 0; i < cards.length; i++) { r -= weights[i]; if (r <= 0) return cards[i]; }
  return cards[cards.length - 1];
}

function answer(knew) {
  if (!card) return;
  const id = idOf(card);
  const box = boxOf(card);
  progress[id] = { box: knew === 2 ? Math.min(BOXES - 1, box + 1) : knew === 1 ? box : 0, at: Date.now() };
  save();
  next();
}

function next() {
  card = pick();
  flipped = false;
  render();
}

export function initCards() {
  load();
  document.addEventListener('keydown', e => {
    if (!$('#tab-cards').classList.contains('active') || mode !== 'learn') return;
    if (e.target.closest('input, textarea, select')) return;
    if (e.code === 'Space') { e.preventDefault(); flipped = !flipped; render(); }
    if (flipped && e.key === '1') answer(0);
    if (flipped && e.key === '2') answer(1);
    if (flipped && e.key === '3') answer(2);
  });
}

export function showCards() {
  if (!built) { built = true; next(); }
}

function render() {
  const root = $('#cards');
  const cats = ['все', ...new Set(CARDS.map(c => c.cat))];
  const all = pool();
  const counts = Array.from({ length: BOXES }, (_, b) => all.filter(c => boxOf(c) === b).length);
  const colors = ['#475569', '#f97316', '#f59e0b', '#84cc16', '#22c55e'];

  const top = h('div', { class: 'cards-top' },
    ...cats.map(c => h('button', { class: 'chip-btn' + (c === category ? ' active' : ''), onclick: () => { category = c; next(); } }, c)),
    h('span', { style: 'flex:1' }),
    h('button', { class: 'chip-btn' + (mode === 'learn' ? ' active' : ''), onclick: () => { mode = 'learn'; render(); } }, 'учить'),
    h('button', { class: 'chip-btn' + (mode === 'list' ? ' active' : ''), onclick: () => { mode = 'list'; render(); } }, 'списком'));

  const bar = h('div', { class: 'cards-progress', 'data-tip': counts.map((n, b) => `коробка ${b}: ${n}`).join('<br>') },
    ...counts.map((n, b) => h('i', { style: `width:${(n / Math.max(1, all.length)) * 100}%;background:${colors[b]}` })));
  const learned = counts[BOXES - 1] + counts[BOXES - 2];
  const stats = h('div', { class: 'small muted', style: 'display:flex;gap:12px;align-items:center;margin-bottom:14px' },
    bar, h('span', {}, `выучено ${learned} из ${all.length}`),
    h('button', { class: 'btn btn-xs', onclick: () => { if (confirm('Сбросить прогресс карточек?')) { progress = {}; save(); render(); } } }, 'сбросить'));

  if (mode === 'list') {
    root.replaceChildren(top, stats, h('div', { class: 'cards-list' },
      ...all.map(c => h('details', {}, h('summary', {}, c.q, h('span', { class: 'muted small' }, `  · ${c.cat}`)), h('div', { class: 'a', html: c.a })))));
    return;
  }

  if (!card) { root.replaceChildren(top, stats, h('div', { class: 'empty-state' }, 'Нет карточек')); return; }
  const flash = h('div', { class: 'flash' + (flipped ? ' flipped' : ''), onclick: () => { flipped = !flipped; render(); } },
    h('div', { class: 'flash-inner' },
      h('div', { class: 'flash-face front' },
        h('div', { class: 'cat' }, `${card.cat} · коробка ${boxOf(card)}`),
        h('div', { class: 'q' }, card.q),
        h('div', { class: 'hint' }, 'клик или пробел — показать ответ')),
      h('div', { class: 'flash-face back' },
        h('div', { class: 'cat' }, card.cat),
        h('div', { class: 'q', style: 'font-size:16px;text-align:left' }, card.q),
        h('div', { class: 'a', html: card.a }))));
  const actionsRow = h('div', { class: 'cards-actions' },
    flipped
      ? [h('button', { class: 'btn btn-danger', onclick: () => answer(0) }, '1 · не знал'),
        h('button', { class: 'btn btn-warn', onclick: () => answer(1) }, '2 · сомневался'),
        h('button', { class: 'btn btn-heal', onclick: () => answer(2) }, '3 · знал')]
      : [h('button', { class: 'btn btn-primary', onclick: () => { flipped = true; render(); } }, 'Показать ответ'),
        h('button', { class: 'btn', onclick: next }, 'Пропустить')]);
  root.replaceChildren(top, stats, flash, actionsRow);
}
