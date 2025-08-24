# Operadora Bot Starter (sem LLM) — FastAPI + DuckDB

Este starter resolve **acurácia** primeiro, depois a gente pluga LLM.
- Backend **FastAPI** com endpoints de KPI (sinistralidade, prestador_top).
- Base de dados **DuckDB** (rápido e gratuito) criada a partir dos seus CSVs.
- **Sem Streamlit, sem Supabase REST**. Só números certos, auditáveis.
- Pronto para publicar na **Render** (free) ou Railway.

## Como funciona
1) **Carrega CSVs** (`beneficiario.csv`, `conta.csv`, `mensalidade.csv`, `prestador.csv`) da pasta `backend/data/`.
2) Normaliza tipos/decimais e **materializa** KPIs em tabelas DuckDB:
   - `kpi_sinistralidade_mensal (competencia, receita_vl_premio, custo_vl_liberado, sinistralidade)`
3) Endpoints FastAPI entregam os números já prontos.

> Depois, plugamos um módulo NL→SQL (LLM) *por cima* destes KPIs, sem trocar nada aqui.

## O que você precisa fazer (sem programar)
1) Coloque seus CSVs em `backend/data/` com estes nomes exatos:
   - `beneficiario.csv`, `conta.csv`, `mensalidade.csv`, `prestador.csv`
2) (Opcional) Use os CSVs de amostra que você já tem.
3) Siga estes passos:

### Rodar localmente (opcional, só para testar)
Requer **Python 3.10+** instalado.

```bash
cd backend
pip install -r requirements.txt
python load_data.py   # cria/atualiza o banco DuckDB com os KPIs
uvicorn app:app --host 0.0.0.0 --port 8000
```
Abra: http://localhost:8000/docs

### Publicar grátis na Render
1) Crie um **novo repositório** no GitHub e suba esta pasta.
2) Na Render:
   - New → Web Service → conecte seu repo
   - Runtime: **Python**
   - Build: `pip install -r backend/requirements.txt && python backend/load_data.py`
   - Start: `uvicorn backend.app:app --host 0.0.0.0 --port 10000`
   - Port: `10000`
   - Add Environment Variable: `PORT=10000`
3) Faça upload dos seus CSVs pela própria UI do GitHub (pasta `backend/data/`) e clique **Deploy** novamente.

> Importante: toda vez que novos CSVs forem atualizados, a Render recompila e roda `load_data.py`, atualizando os KPIs.

## Endpoints principais
- `GET /health` — status
- `GET /kpi/sinistralidade/ultima` — sinistralidade da última competência
- `GET /kpi/sinistralidade/media?meses=6` — média dos últimos N meses
- `GET /kpi/prestador/top?competencia=YYYY-MM` — prestador com maior custo
- `GET /kpi/faixa/custo?competencia=YYYY-MM` — custo por faixa etária (0–18, 19–59, 60+)

## Próximos passos (fase 2)
- Adicionar mais KPIs (utilização, inadimplência, glosas, etc).
- Adicionar módulo **NL→SQL** com *guardrails* (somente SELECT sobre tabelas `kpi_*`).

---
Qualquer dúvida, me chame no chat que eu sigo com você passo a passo.
