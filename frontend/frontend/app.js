const API_BASE = window.API_BASE || '';
document.getElementById('apiBase').textContent = API_BASE;

// Helpers
const qs = (sel) => document.querySelector(sel);

function renderTable(rows, container) {
  if (!rows || !rows.length) {
    container.innerHTML = '<p>Nenhum dado retornado.</p>';
    return;
  }
  const cols = [...new Set(rows.flatMap(r => Object.keys(r)))];
  const thead = '<thead><tr>' + cols.map(c => `<th>${c}</th>`).join('') + '</tr></thead>';
  const tbody = '<tbody>' + rows.map(r =>
    '<tr>' + cols.map(c => `<td>${r[c] ?? ''}</td>`).join('') + '</tr>'
  ).join('') + '</tbody>';
  container.innerHTML = `<div class="tableWrap"><table>${thead}${tbody}</table></div>`;
}

function renderJSON(obj, container) {
  container.innerHTML = `<pre>${JSON.stringify(obj, null, 2)}</pre>`;
}

async function GET(path, params = {}) {
  const url = new URL(API_BASE + path);
  Object.entries(params).forEach(([k, v]) => {
    if (v !== undefined && v !== null && String(v).trim() !== '') {
      url.searchParams.set(k, v);
    }
  });
  const res = await fetch(url.toString(), { headers: { accept: 'application/json' } });
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`HTTP ${res.status} - ${text || res.statusText}`);
  }
  return res.json();
}

// Top Prestadores (impacto)
qs('#formTop').addEventListener('submit', async (e) => {
  e.preventDefault();
  const comp = qs('#compTop').value;
  const limit = parseInt(qs('#limitTop').value || '10', 10);
  const out = qs('#outTop');
  out.innerHTML = 'Carregando...';

  try {
    // usa o endpoint /kpi/prestador/impacto (que você habilitou)
    const data = await GET('/kpi/prestador/impacto', { competencia: comp, top: limit });
    // data.ex: { competencia: '2025-06', itens: [ { id_prestador, nome, total, ... } ] }
    const rows = (data.itens || data.top || data.rows || data) ?? [];
    renderTable(rows, out);
  } catch (err) {
    out.innerHTML = `<div class="error">${err.message}</div>`;
  }
});

// Sinistralidade (última & média)
qs('#btnUltima').addEventListener('click', async () => {
  const out = qs('#outSini');
  out.innerHTML = 'Carregando...';
  try {
    const data = await GET('/kpi/sinistralidade/ultima');
    renderJSON(data, out);
  } catch (err) {
    out.innerHTML = `<div class="error">${err.message}</div>`;
  }
});

qs('#btnMedia').addEventListener('click', async () => {
  const out = qs('#outSini');
  out.innerHTML = 'Carregando...';
  try {
    const data = await GET('/kpi/sinistralidade/media');
    renderJSON(data, out);
  } catch (err) {
    out.innerHTML = `<div class="error">${err.message}</div>`;
  }
});

// Utilização — Resumo
qs('#formUtil').addEventListener('submit', async (e) => {
  e.preventDefault();
  const params = {
    competencia: qs('#compUtil').value,
    produto: qs('#prodUtil').value,
    uf: qs('#ufUtil').value,
    cidade: qs('#cidUtil').value,
    sexo: qs('#sexoUtil').value,
    faixa: qs('#faixaUtil').value,
  };
  const out = qs('#outUtil');
  out.innerHTML = 'Carregando...';

  try {
    const data = await GET('/kpi/utilizacao/resumo', params);
    // Pode ser objeto com agregados; mostramos JSON por padrão
    renderJSON(data, out);
  } catch (err) {
    out.innerHTML = `<div class="error">${err.message}</div>`;
  }
});
