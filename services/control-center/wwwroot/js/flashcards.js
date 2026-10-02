// Flashcards (Leitner spaced repetition, progress in localStorage).
import { h, $ } from './util.js';
import { CARDS, CATEGORIES } from './flashcards-data.js';
import { T, pick } from './i18n.js';

const BOXES = 5; // 0 — new / didn't know … 4 — learned
const ALL = 'all';
let progress = {};
let category = ALL;
let card = null;
let flipped = false;
let mode = 'learn';
let built = false;

function load() {
  try { progress = JSON.parse(localStorage.getItem('cards-progress') ?? '{}'); } catch { progress = {}; }
  // Progress used to be keyed by the Russian question text — move it to the stable card ids
  let migrated = false;
  for (const c of CARDS) {
    if (progress[c.q.ru] && !progress[c.id]) { progress[c.id] = progress[c.q.ru]; migrated = true; }
    if (progress[c.q.ru]) { delete progress[c.q.ru]; migrated = true; }
  }
  if (migrated) save();
}
function save() {
  try { localStorage.setItem('cards-progress', JSON.stringify(progress)); } catch { /* private mode */ }
}
const boxOf = c => progress[c.id]?.box ?? 0;
const catLabel = cat => cat === ALL ? T('all', 'все') : pick(CATEGORIES[cat]) ?? cat;

function pool() {
  return CARDS.filter(c => category === ALL || c.cat === category);
}

function pickCard() {
  const cards = pool();
  if (!cards.length) return null;
  // show cards from the lower boxes more often, but never the same one twice in a row
  const weights = cards.map(c => (c === card ? 0.01 : Math.pow(2.4, BOXES - 1 - boxOf(c))));
  let r = Math.random() * weights.reduce((a, b) => a + b, 0);
  for (let i = 0; i < cards.length; i++) { r -= weights[i]; if (r <= 0) return cards[i]; }
  return cards[cards.length - 1];
}

function answer(knew) {
  if (!card) return;
  const box = boxOf(card);
  progress[card.id] = { box: knew === 2 ? Math.min(BOXES - 1, box + 1) : knew === 1 ? box : 0, at: Date.now() };
  save();
  next();
}

function next() {
  card = pickCard();
  flipped = false;
  render();
}

export function initCards() {
  load();
  document.addEventListener('keydown', e => {
    if (!$('#tab-cards').classList.contains('active') || mode !== 'learn') return;
    if (e.target.closest('input, textarea, select')) return;
    if (document.querySelector('.modal-backdrop:not(.hidden)')) return;
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
  const cats = [ALL, ...new Set(CARDS.map(c => c.cat))];
  const all = pool();
  const counts = Array.from({ length: BOXES }, (_, b) => all.filter(c => boxOf(c) === b).length);
  const colors = ['#475569', '#f97316', '#f59e0b', '#84cc16', '#22c55e'];

  const top = h('div', { class: 'cards-top' },
    ...cats.map(c => h('button', { class: 'chip-btn' + (c === category ? ' active' : ''), onclick: () => { category = c; next(); } }, catLabel(c))),
    h('span', { style: 'flex:1' }),
    h('button', { class: 'chip-btn' + (mode === 'learn' ? ' active' : ''), onclick: () => { mode = 'learn'; render(); } }, T('learn', 'учить')),
    h('button', { class: 'chip-btn' + (mode === 'list' ? ' active' : ''), onclick: () => { mode = 'list'; render(); } }, T('as a list', 'списком')));

  const bar = h('div', { class: 'cards-progress', 'data-tip': counts.map((n, b) => `${T('box', 'коробка')} ${b}: ${n}`).join('<br>') },
    ...counts.map((n, b) => h('i', { style: `width:${(n / Math.max(1, all.length)) * 100}%;background:${colors[b]}` })));
  const learned = counts[BOXES - 1] + counts[BOXES - 2];
  const stats = h('div', { class: 'small muted', style: 'display:flex;gap:12px;align-items:center;margin-bottom:14px' },
    bar, h('span', {}, T(`learned ${learned} of ${all.length}`, `выучено ${learned} из ${all.length}`)),
    h('button', {
      class: 'btn btn-xs',
      onclick: () => { if (confirm(T('Reset your flashcard progress?', 'Сбросить прогресс карточек?'))) { progress = {}; save(); render(); } },
    }, T('reset', 'сбросить')));

  if (mode === 'list') {
    root.replaceChildren(top, stats, h('div', { class: 'cards-list' },
      ...all.map(c => h('details', {}, h('summary', {}, pick(c.q), h('span', { class: 'muted small' }, `  · ${catLabel(c.cat)}`)), h('div', { class: 'a', html: pick(c.a) })))));
    return;
  }

  if (!card) { root.replaceChildren(top, stats, h('div', { class: 'empty-state' }, T('No cards', 'Нет карточек'))); return; }
  const flash = h('div', { class: 'flash' + (flipped ? ' flipped' : ''), onclick: () => { flipped = !flipped; render(); } },
    h('div', { class: 'flash-inner' },
      h('div', { class: 'flash-face front' },
        h('div', { class: 'cat' }, `${catLabel(card.cat)} · ${T('box', 'коробка')} ${boxOf(card)}`),
        h('div', { class: 'q' }, pick(card.q)),
        h('div', { class: 'hint' }, T('click or press space to show the answer', 'клик или пробел — показать ответ'))),
      h('div', { class: 'flash-face back' },
        h('div', { class: 'cat' }, catLabel(card.cat)),
        h('div', { class: 'q', style: 'font-size:16px;text-align:left' }, pick(card.q)),
        h('div', { class: 'a', html: pick(card.a) }))));
  const actionsRow = h('div', { class: 'cards-actions' },
    flipped
      ? [h('button', { class: 'btn btn-danger', onclick: () => answer(0) }, T("1 · didn't know", '1 · не знал')),
        h('button', { class: 'btn btn-warn', onclick: () => answer(1) }, T('2 · unsure', '2 · сомневался')),
        h('button', { class: 'btn btn-heal', onclick: () => answer(2) }, T('3 · knew it', '3 · знал'))]
      : [h('button', { class: 'btn btn-primary', onclick: () => { flipped = true; render(); } }, T('Show answer', 'Показать ответ')),
        h('button', { class: 'btn', onclick: next }, T('Skip', 'Пропустить'))]);
  root.replaceChildren(top, stats, flash, actionsRow);
}
