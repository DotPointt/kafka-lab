// A very short "what is this site" modal: shown on the first visit and from the "?" button.
// Also wires the clickable brand (→ cheat sheets) and the EN/RU language switch.
import { h, $, $$ } from './util.js';
import { DEMO, REPO_URL } from './api.js';
import { T, lang, setLang } from './i18n.js';
import { showDoc } from './docs.js';

const SEEN_KEY = 'intro-seen';

export function initIntro(openTab) {
  const modal = $('#intro');
  let lastFocus = null;

  for (const b of $$('.lang-switch button[data-lang]')) {
    b.classList.toggle('active', b.dataset.lang === lang);
    b.setAttribute('aria-pressed', String(b.dataset.lang === lang));
    b.addEventListener('click', () => setLang(b.dataset.lang));
  }

  $('#brand').addEventListener('click', e => {
    if (e.ctrlKey || e.metaKey || e.shiftKey || e.button !== 0) return; // let "open in a new tab" work
    e.preventDefault();
    openTab('docs');
    window.scrollTo({ top: 0 });
  });

  if (DEMO) {
    const demo = $('#intro-demo');
    const repoText = T('the repository', 'репозитории');
    demo.replaceChildren(
      T('This is a replay of a recording of the real lab, so the control buttons are disabled. To break the cluster yourself, run it locally (instructions in ',
        'Это запись реального стенда, поэтому кнопки управления отключены. Чтобы ломать кластер самому, запусти его локально (инструкция в '),
      REPO_URL ? h('a', { href: REPO_URL, target: '_blank', rel: 'noopener' }, repoText) : repoText,
      '): ', h('code', {}, 'docker compose up -d --build'));
    demo.classList.remove('hidden');
  }

  const open = () => {
    lastFocus = document.activeElement;
    modal.classList.remove('hidden');
    $('#intro-go').focus();
  };
  const close = () => {
    if (modal.classList.contains('hidden')) return;
    modal.classList.add('hidden');
    try { localStorage.setItem(SEEN_KEY, '1'); } catch { /* private mode: the modal will show again next time */ }
    lastFocus?.focus?.();
  };

  $('#btn-about').addEventListener('click', open);
  $('#intro-close').addEventListener('click', close);
  $('#intro-go').addEventListener('click', close);
  $('#intro-docs').addEventListener('click', () => { close(); openTab('docs'); showDoc('01-basics.md'); });
  modal.addEventListener('click', e => { if (e.target === modal) close(); });
  document.addEventListener('keydown', e => {
    if (modal.classList.contains('hidden')) return;
    if (e.key === 'Escape') { e.preventDefault(); close(); return; }
    if (e.key === 'Tab') {
      // keep the focus inside the dialog
      const items = [...modal.querySelectorAll('button, a[href]')].filter(x => x.offsetParent !== null);
      const first = items[0], last = items[items.length - 1];
      if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last.focus(); }
      else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first.focus(); }
    }
  });

  let seen = false;
  try { seen = localStorage.getItem(SEEN_KEY) === '1'; } catch { /* ignore */ }
  if (!seen) open();
}
