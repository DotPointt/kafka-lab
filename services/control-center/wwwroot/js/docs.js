// Вкладка «Памятки»: список docs/*.md, мини-рендерер Markdown, оглавление и поиск.
import { h, $, esc } from './util.js';
import { DEMO } from './api.js';

let docs = [];
const cache = new Map();
let currentFile = null;
let loaded = false;
let query = '';

export function initDocs() {
  $('#docs-search').addEventListener('input', async e => {
    query = e.target.value.trim().toLowerCase();
    await loadAll();
    renderList();
    if (currentFile) openDoc(currentFile, false);
  });
  $('#doc').addEventListener('click', e => {
    const a = e.target.closest('a[data-doc]');
    if (a) {
      e.preventDefault();
      openDoc(a.dataset.doc, true, a.dataset.anchor);
    }
  });
}

export async function showDocs() {
  if (!loaded) {
    loaded = true;
    try {
      // На локальном стенде список отдаёт control-center, в статической копии (GitHub Pages) — готовый JSON
      docs = await (await fetch(DEMO ? 'api/docs.json' : 'api/docs')).json();
    } catch {
      docs = [];
    }
    renderList();
    const fromHash = decodeURIComponent(location.hash.split('/')[1] ?? '');
    const first = docs.find(d => d.file === fromHash)?.file ?? docs[0]?.file;
    if (first) openDoc(first);
    else $('#doc').innerHTML = '<p class="muted">Памятки не найдены: каталог docs не смонтирован в control-center.</p>';
  }
}

async function getDoc(file) {
  if (cache.has(file)) return cache.get(file);
  try {
    const r = await fetch('docs/' + encodeURIComponent(file));
    if (!r.ok) return `# Ошибка\n\nНе удалось загрузить ${file} (${r.status})`;
    const text = await r.text();
    cache.set(file, text);
    return text;
  } catch {
    return `# Нет связи с control-center\n\nПамятка «${file}» не загрузилась — попробуй ещё раз через пару секунд.`;
  }
}

async function loadAll() {
  await Promise.all(docs.map(d => getDoc(d.file)));
}

function countMatches(text) {
  if (!query) return 0;
  let n = 0, i = 0;
  const lower = text.toLowerCase();
  while ((i = lower.indexOf(query, i)) >= 0) { n++; i += query.length; }
  return n;
}

function renderList() {
  const list = $('#docs-list');
  list.replaceChildren(...docs
    .map(d => ({ ...d, hits: countMatches(cache.get(d.file) ?? '') }))
    .filter(d => !query || d.hits > 0)
    .map(d => h('a', { class: 'doc-link' + (d.file === currentFile ? ' active' : ''), onclick: () => openDoc(d.file) },
      d.title, query ? h('span', { class: 'muted small' }, ` · ${d.hits}`) : null)));
  if (query && !list.children.length) list.append(h('div', { class: 'muted small' }, 'ничего не найдено'));
}

async function openDoc(file, scrollTop = true, anchor) {
  currentFile = file;
  const md = await getDoc(file);
  const el = $('#doc');
  el.innerHTML = renderMarkdown(md);
  if (query) highlight(el, query);
  renderList();
  const toc = $('#docs-toc');
  toc.replaceChildren(h('div', { class: 'muted small', style: 'margin-bottom:4px' }, 'Содержание'),
    ...[...el.querySelectorAll('h2, h3')].map(x => h('a', {
      class: x.tagName.toLowerCase(), href: '#',
      onclick: e => { e.preventDefault(); x.scrollIntoView({ behavior: 'smooth', block: 'start' }); },
    }, x.textContent)));
  history.replaceState(null, '', `#docs/${encodeURIComponent(file)}`);
  if (anchor) document.getElementById(anchor)?.scrollIntoView({ block: 'start' });
  else if (query) el.querySelector('mark')?.scrollIntoView({ block: 'center' });
  else if (scrollTop) window.scrollTo({ top: 0 });
}

function highlight(root, q) {
  const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
  const nodes = [];
  while (walker.nextNode()) nodes.push(walker.currentNode);
  for (const node of nodes) {
    const text = node.nodeValue;
    const lower = text.toLowerCase();
    let idx = lower.indexOf(q);
    if (idx < 0) continue;
    const frag = document.createDocumentFragment();
    let last = 0;
    while (idx >= 0) {
      frag.append(text.slice(last, idx), h('mark', {}, text.slice(idx, idx + q.length)));
      last = idx + q.length;
      idx = lower.indexOf(q, last);
    }
    frag.append(text.slice(last));
    node.replaceWith(frag);
  }
}

// ================================================================== Markdown → HTML (подмножество GFM)

function slug(s) {
  return s.toLowerCase().replace(/<[^>]+>/g, '').replace(/[^\p{L}\p{N}]+/gu, '-').replace(/^-|-$/g, '');
}

function inline(s) {
  const codes = [];
  s = s.replace(/`([^`]+)`/g, (_, c) => { codes.push(c); return `\u0000${codes.length - 1}\u0000`; });
  s = esc(s);
  s = s.replace(/\*\*(.+?)\*\*/g, '<b>$1</b>');
  s = s.replace(/(^|[\s(«])\*(?!\s)(.+?)\*(?=[\s.,;:!?)»]|$)/g, '$1<i>$2</i>');
  s = s.replace(/\[([^\]]+)\]\(([^)\s]+)\)/g, (_, text, url) => {
    const m = url.match(/^([\w.-]+\.md)(?:#(.*))?$/);
    if (m) return `<a href="#" data-doc="${m[1]}" data-anchor="${m[2] ?? ''}">${text}</a>`;
    return `<a href="${url}" target="_blank" rel="noopener">${text}</a>`;
  });
  s = s.replace(/\u0000(\d+)\u0000/g, (_, n) => `<code>${esc(codes[+n])}</code>`);
  return s;
}

const isFence = l => /^\s*```/.test(l);
const isHeading = l => /^#{1,6}\s/.test(l);
const isHr = l => /^\s*(-{3,}|\*{3,})\s*$/.test(l);
const isQuote = l => /^>/.test(l);
const isListItem = l => /^\s*([-*+]|\d+[.)])\s+/.test(l);
const isTableStart = (l, next) => /^\s*\|/.test(l) && next !== undefined && /^\s*\|?\s*:?-{2,}/.test(next);

export function renderMarkdown(src) {
  const lines = src.replace(/\r\n?/g, '\n').split('\n');
  const out = [];
  let i = 0;
  while (i < lines.length) {
    const line = lines[i];
    if (isFence(line)) {
      const lang = line.trim().slice(3).trim();
      const buf = [];
      i++;
      while (i < lines.length && !isFence(lines[i])) buf.push(lines[i++]);
      i++;
      out.push(`<pre><code class="lang-${esc(lang)}">${esc(buf.join('\n'))}</code></pre>`);
      continue;
    }
    if (isHeading(line)) {
      const level = line.match(/^#+/)[0].length;
      const text = line.slice(level).trim();
      out.push(`<h${level} id="${slug(text)}">${inline(text)}</h${level}>`);
      i++;
      continue;
    }
    if (isHr(line)) { out.push('<hr>'); i++; continue; }
    if (isQuote(line)) {
      const buf = [];
      while (i < lines.length && isQuote(lines[i])) buf.push(lines[i++].replace(/^>\s?/, ''));
      out.push(`<blockquote>${renderMarkdown(buf.join('\n'))}</blockquote>`);
      continue;
    }
    if (isTableStart(line, lines[i + 1])) {
      const split = l => l.trim().replace(/^\|/, '').replace(/\|$/, '').replace(/\\\|/g, '\u0001').split('|').map(c => c.trim().replace(/\u0001/g, '|'));
      const head = split(line);
      i += 2;
      const rows = [];
      while (i < lines.length && /^\s*\|/.test(lines[i])) rows.push(split(lines[i++]));
      out.push('<table><thead><tr>' + head.map(c => `<th>${inline(c)}</th>`).join('') + '</tr></thead><tbody>' +
        rows.map(r => '<tr>' + r.map(c => `<td>${inline(c)}</td>`).join('') + '</tr>').join('') + '</tbody></table>');
      continue;
    }
    if (isListItem(line)) {
      const items = [];
      while (i < lines.length) {
        const l = lines[i];
        const m = l.match(/^(\s*)([-*+]|\d+[.)])\s+(.*)$/);
        if (m) { items.push({ indent: m[1].length, ordered: /\d/.test(m[2]), text: m[3] }); i++; continue; }
        if (l.trim() && /^\s{2,}/.test(l) && items.length) { items[items.length - 1].text += ' ' + l.trim(); i++; continue; }
        break;
      }
      out.push(renderMdList(items));
      continue;
    }
    if (!line.trim()) { i++; continue; }
    const buf = [];
    while (i < lines.length && lines[i].trim() && !isFence(lines[i]) && !isHeading(lines[i]) && !isQuote(lines[i]) &&
      !isListItem(lines[i]) && !isHr(lines[i]) && !isTableStart(lines[i], lines[i + 1])) buf.push(lines[i++].trim());
    out.push(`<p>${inline(buf.join(' '))}</p>`);
  }
  return out.join('\n');
}

function renderMdList(items) {
  let idx = 0;
  const render = base => {
    const tag = items[idx].ordered ? 'ol' : 'ul';
    let s = `<${tag}>`;
    while (idx < items.length && items[idx].indent >= base) {
      if (items[idx].indent > base) {
        const nested = render(items[idx].indent);
        s = s.endsWith('</li>') ? s.slice(0, -5) + nested + '</li>' : s + `<li>${nested}</li>`;
        continue;
      }
      s += `<li>${inline(items[idx].text)}</li>`;
      idx++;
    }
    return s + `</${tag}>`;
  };
  return render(items[0].indent);
}
