// Точка входа UI.
import { $, $$, initTooltips } from './util.js';
import { store } from './store.js';
import { connectStream } from './api.js';
import { initLive } from './live.js';
import { initFlow } from './flow.js';
import { initCharts } from './charts.js';
import { initPartitions, initMessages, initEvents, initSubtabs, renderHealth, heal } from './panels.js';
import { initScenarios, openScenarios } from './scenarios.js';
import { initDocs, showDocs } from './docs.js';
import { initCards, showCards } from './flashcards.js';

function initTabs() {
  const open = name => {
    $$('.tab-btn').forEach(b => b.classList.toggle('active', b.dataset.tab === name));
    $$('.tab-page').forEach(p => p.classList.toggle('active', p.id === 'tab-' + name));
    if (name === 'docs') showDocs();
    if (name === 'cards') showCards();
    if (location.hash.slice(1).split('/')[0] !== name) history.replaceState(null, '', '#' + name);
  };
  $$('.tab-btn').forEach(b => b.addEventListener('click', () => open(b.dataset.tab)));
  const fromHash = () => {
    const name = location.hash.slice(1).split('/')[0];
    if (['live', 'docs', 'cards'].includes(name)) open(name);
  };
  window.addEventListener('hashchange', fromHash);
  fromHash();
  return open;
}

initTooltips();
const openTab = initTabs();
initLive();
initFlow();
initCharts();
initPartitions();
initMessages();
initEvents();
initSubtabs();
initScenarios(openTab);
initDocs();
initCards();

store.onSnapshot(renderHealth);
$('#btn-heal').addEventListener('click', heal);
$('#btn-scenarios').addEventListener('click', () => { openTab('live'); openScenarios(); });

connectStream();
