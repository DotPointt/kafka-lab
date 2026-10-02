// Interface languages: English (default) and Russian.
// The choice comes from ?lang=… in the URL, then localStorage; switching the language reloads the page,
// so every module simply renders its strings once in the current language via T('English', 'Русский').

export const LANGS = ['en', 'ru'];

function detect() {
  try {
    const fromUrl = new URLSearchParams(location.search).get('lang');
    if (LANGS.includes(fromUrl)) {
      localStorage.setItem('lang', fromUrl);
      return fromUrl;
    }
    const saved = localStorage.getItem('lang');
    if (LANGS.includes(saved)) return saved;
  } catch { /* storage unavailable (private mode) — fall back to the default */ }
  return 'en';
}

export const lang = detect();
export const isRu = lang === 'ru';

/** The string in the current language: T('Partitions', 'Партиции'). */
export function T(en, ru) {
  return isRu ? ru : en;
}

/** Pick a localized value from { en, ru } objects (or return plain strings as is). */
export function pick(value) {
  if (value && typeof value === 'object' && ('en' in value || 'ru' in value)) return isRu ? (value.ru ?? value.en) : (value.en ?? value.ru);
  return value;
}

/** Event / error text from the backend: text — Russian, textEn — English. */
export function evText(e) {
  return isRu ? e.text : (e.textEn ?? e.text);
}

export const locale = isRu ? 'ru-RU' : 'en-US';

export function setLang(next) {
  if (!LANGS.includes(next) || next === lang) return;
  try { localStorage.setItem('lang', next); } catch { /* ignore */ }
  const url = new URL(location.href);
  url.searchParams.set('lang', next);
  location.href = url.toString();
}

/** Static HTML is written in English; elements carry data-ru / data-ru-title / data-ru-placeholder for Russian. */
export function applyStatic(root = document) {
  document.documentElement.lang = lang;
  if (!isRu) return;
  for (const el of root.querySelectorAll('[data-ru]')) el.textContent = el.dataset.ru;
  for (const el of root.querySelectorAll('[data-ru-title]')) el.title = el.dataset.ruTitle;
  for (const el of root.querySelectorAll('[data-ru-placeholder]')) el.placeholder = el.dataset.ruPlaceholder;
  for (const el of root.querySelectorAll('[data-ru-tip]')) el.dataset.tip = el.dataset.ruTip;
}
