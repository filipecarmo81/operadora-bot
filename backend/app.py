# backend/app.py
from __future__ import annotations

import os
import re
from typing import Dict, List, Optional

import duckdb
from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware

# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------

DB_PATH = os.getenv(
    "OPERADORA_DB",
    os.path.join(os.path.dirname(__file__), "data", "operadora.duckdb"),
)

# ------------------------------------------------------------------------------
# FastAPI + CORS (ABERTO TEMPORARIAMENTE)
# ------------------------------------------------------------------------------

app = FastAPI(title="Operadora KPIs", version="0.2.1")

# Libera tudo para eliminar qualquer bloqueio de CORS no front
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],   # GET, POST, OPTIONS, HEAD...
    allow_headers=["*"],
)

# garante resposta 204 para preflight
@app.options("/{rest_of_path:path}")
def preflight(rest_of_path: str):
    return Response(status_code=204)

# ------------------------------------------------------------------------------
# Helpers de DB
# ------------------------------------------------------------------------------

def connect() -> duckdb.DuckDBPyConnection:
    try:
        return duckdb.connect(DB_PATH, read_only=True)
    except Exception as e:
        raise HTTPException(500, detail=f"Falha ao abrir DuckDB: {e}")

def list_tables(con) -> List[str]:
    rows = con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema='main' ORDER BY 1"
    ).fetchall()
    return [r[0] for r in rows]

def get_cols(con, table: str) -> List[str]:
    rows = con.execute(f"PRAGMA table_info('{table}')").fetchall()
    return [r[1] for r in rows]

def find_col(con, table: str, candidates: List[str]) -> Optional[str]:
    cols = get_cols(con, table)
    lower = {c.lower(): c for c in cols}
    for want in candidates:
        if want.lower() in lower:
            return lower[want.lower()]
    for want in candidates:
        w = want.lower()
        for c in cols:
            cl = c.lower()
            if cl == w or cl.startswith(w) or w in cl:
                return c
    return None

def month_predicate(col: str, param_name: str = "comp") -> str:
    return (
        f"(strftime('%Y-%m', TRY_CAST({col} AS DATE)) = ${param_name} "
        f"OR substr(CAST({col} AS VARCHAR), 1, 7) = ${param_name} "
        f"OR {col} = ${param_name})"
    )

def sum_value(con, table: str, val_cols: List[str], dt_cols: List[str], competencia: str) -> float:
    val = find_col(con, table, val_cols)
    dt = find_col(con, table, dt_cols)
    if not val or not dt:
        return 0.0
    sql = f"""
        SELECT COALESCE(SUM(CAST({val} AS DOUBLE)), 0) AS total
        FROM {table}
        WHERE {month_predicate(dt)}
    """
    try:
        row = con.execute(sql, {"comp": competencia}).fetchone()
        return float(row[0] or 0.0)
    except Exception:
        return 0.0

# ------------------------------------------------------------------------------
# Meta
# ------------------------------------------------------------------------------

@app.get("/", tags=["Meta"])
def root():
    con = connect()
    return {
        "ok": True,
        "message": "API do Operadora Bot. Use /docs para testar.",
        "db": DB_PATH,
        "tables": list_tables(con),
        "endpoints": [
            "/health",
            "/debug/cols",
            "/kpi/sinistralidade/ultima",
            "/kpi/sinistralidade/competencia?competencia=YYYY-MM",
            "/kpi/sinistralidade/media",
            "/kpi/prestador/top?competencia=YYYY-MM&limite=10",
            "/kpi/prestador/impacto?competencia=YYYY-MM&top=10",
            "/kpi/utilizacao/resumo?competencia=YYYY-MM",
        ],
    }

@app.get("/health", tags=["Meta"])
def health():
    con = connect()
    return {"ok": True, "db": DB_PATH, "tables": list_tables(con)}

@app.get("/debug/cols", tags=["Meta"])
def debug_cols():
    con = connect()
    return {t: get_cols(con, t) for t in list_tables(con)}

# ------------------------------------------------------------------------------
# Sinistralidade (apenas: conta.vl_liberado e mensalidade.vl_premio)
# ------------------------------------------------------------------------------

def sinis_comp(con, competencia: str):
    custo = sum_value(
        con, "conta",
        val_cols=["vl_liberado", "valor_liberado", "vl_pago"],
        dt_cols=["dt_mes_competencia", "dt_competencia", "dt_conta", "dt_lancamento"],
        competencia=competencia,
    )
    premio = sum_value(
        con, "mensalidade",
        val_cols=["vl_premio", "valor_premio", "vl_receita"],
        dt_cols=["dt_competencia", "dt_mes_competencia", "dt_pagamento"],
        competencia=competencia,
    )
    sin = (custo / premio) if premio and premio > 0 else 0.0
    return {"competencia": competencia, "sinistro": custo, "receita": premio, "sinistralidade": sin}

@app.get("/kpi/sinistralidade/competencia", tags=["Sinistralidade"])
def sinistralidade_competencia(competencia: str = Query(..., pattern=r"^\d{4}-\d{2}$")):
    con = connect()
    return sinis_comp(con, competencia)

@app.get("/kpi/sinistralidade/ultima", tags=["Sinistralidade"])
def sinistralidade_ultima():
    con = connect()
    mdt = find_col(con, "mensalidade", ["dt_competencia", "dt_mes_competencia", "dt_pagamento"])
    cdt = find_col(con, "conta", ["dt_mes_competencia", "dt_competencia", "dt_conta", "dt_lancamento"])
    meses = []
    if mdt:
        try:
            r = con.execute(
                f"SELECT strftime('%Y-%m', TRY_CAST({mdt} AS DATE)) AS m "
                f"FROM mensalidade WHERE m IS NOT NULL ORDER BY 1 DESC LIMIT 1"
            ).fetchone()
            if r and r[0]: meses.append(r[0])
        except Exception:
            pass
    if cdt:
        try:
            r = con.execute(
                f"SELECT strftime('%Y-%m', TRY_CAST({cdt} AS DATE)) AS m "
                f"FROM conta WHERE m IS NOT NULL ORDER BY 1 DESC LIMIT 1"
            ).fetchone()
            if r and r[0]: meses.append(r[0])
        except Exception:
            pass
    if not meses:
        raise HTTPException(400, detail="Não foi possível detectar a última competência.")
    comp = sorted(set(meses), reverse=True)[0]
    return sinis_comp(con, comp)

@app.get("/kpi/sinistralidade/media", tags=["Sinistralidade"])
def sinistralidade_media(janela: int = 6):
    con = connect()
    mdt = find_col(con, "mensalidade", ["dt_competencia", "dt_mes_competencia", "dt_pagamento"])
    if not mdt:
        raise HTTPException(400, detail="Não encontrei competência em 'mensalidade'.")
    meses = [r[0] for r in con.execute(
        f"SELECT DISTINCT strftime('%Y-%m', TRY_CAST({mdt} AS DATE)) AS m "
        f"FROM mensalidade WHERE m IS NOT NULL ORDER BY 1 DESC LIMIT {int(janela)}"
    ).fetchall()]
    if not meses:
        return {"janela": janela, "media": 0.0, "series": []}
    series = [sinis_comp(con, m) for m in meses]
    vals = [s['sinistralidade'] for s in series]
    media = sum(vals) / len(vals) if vals else 0.0
    return {"janela": janela, "media": media, "series": series}

# ------------------------------------------------------------------------------
# Prestador (top/impacto) – score = soma do vl_liberado no mês
# ------------------------------------------------------------------------------

def prestador_top_data(con, competencia: str, limite: int):
    cdt = find_col(con, "conta", ["dt_mes_competencia", "dt_competencia", "dt_conta"])
    vl  = find_col(con, "conta", ["vl_liberado", "vl_pago", "valor_liberado"])
    idc = find_col(con, "conta", ["id_prestador_envio", "id_prestador", "cd_prestador"])
    idp = find_col(con, "prestador", ["id_prestador", "cd_prestador"])
    nmp = find_col(con, "prestador", ["nm_prestador", "nome", "nm_razao_social"])
    if not all([cdt, vl, idc, idp, nmp]):
        return []
    sql = f"""
        WITH ag AS (
            SELECT {idc} AS id_prestador, COALESCE(SUM(CAST({vl} AS DOUBLE)),0) AS score
            FROM conta
            WHERE {month_predicate(cdt)}
            GROUP BY 1
        )
        SELECT a.id_prestador, p.{nmp} AS nome, a.score
        FROM ag a LEFT JOIN prestador p ON p.{idp} = a.id_prestador
        ORDER BY a.score DESC
        LIMIT {int(limite)}
    """
    rows = con.execute(sql, {"comp": competencia}).fetchall()
    return [{"id_prestador": r[0], "nome": r[1], "score": r[2]} for r in rows]

@app.get("/kpi/prestador/top", tags=["Prestador"])
def kpi_prestador_top(competencia: str = Query(..., pattern=r"^\d{4}-\d{2}$"), limite: int = 10):
    con = connect()
    return {"competencia": competencia, "top": prestador_top_data(con, competencia, limite)}

@app.get("/kpi/prestador/impacto", tags=["Prestador"])
def kpi_prestador_impacto(competencia: str = Query(..., pattern=r"^\d{4}-\d{2}$"), top: int = 10):
    con = connect()
    return {"competencia": competencia, "top": prestador_top_data(con, competencia, top)}

# ------------------------------------------------------------------------------
# Utilização – resumo (filtros simples)
# ------------------------------------------------------------------------------

@app.get("/kpi/utilizacao/resumo", tags=["Utilização"])
def kpi_utilizacao_resumo(
    competencia: str = Query(..., pattern=r"^\d{4}-\d{2}$"),
    produto: Optional[str] = None,
    uf: Optional[str] = None,
    cidade: Optional[str] = None,
    sexo: Optional[str] = None,
    faixa: Optional[str] = None,
):
    con = connect()

    ben_id = find_col(con, "beneficiario", ["id_beneficiario", "cd_beneficiario"])
    ben_uf = find_col(con, "beneficiario", ["uf", "ds_uf", "sg_uf"])
    ben_cidade = find_col(con, "beneficiario", ["cidade", "nm_cidade", "ds_municipio"])
    ben_sexo = find_col(con, "beneficiario", ["sexo", "ds_sexo", "cd_sexo"])
    ben_nasc = find_col(con, "beneficiario", ["dt_nascimento", "nascimento"])
    ben_prod_id = find_col(con, "beneficiario", ["id_produto", "cd_plano", "id_plano"])
    ben_prod_nm = find_col(con, "beneficiario", ["ds_produto", "ds_plano", "nome_plano"])

    aut_ben = find_col(con, "autorizacao", ["id_beneficiario", "cd_beneficiario"])
    aut_dt  = find_col(con, "autorizacao", ["dt_autorizacao", "dt_solicitacao"])

    if not ben_id:
        raise HTTPException(400, detail="Não encontrei identificador em 'beneficiario'.")

    where_b = []
    if uf and ben_uf:
        where_b.append(f"{ben_uf} IN (SELECT TRIM(x) FROM string_split($uf_csv, ','))")
    if cidade and ben_cidade:
        where_b.append(f"{ben_cidade} IN (SELECT TRIM(x) FROM string_split($cid_csv, ','))")
    if sexo and ben_sexo:
        where_b.append(f"upper({ben_sexo}) = upper($sexo)")
    if produto and (ben_prod_id or ben_prod_nm):
        conds = []
        if ben_prod_id:
            conds.append(f"CAST({ben_prod_id} AS VARCHAR) = $produto OR {ben_prod_id} IN (SELECT TRIM(x) FROM string_split($produto, ','))")
        if ben_prod_nm:
            conds.append(f"upper({ben_prod_nm}) LIKE upper('%' || $produto || '%')")
        where_b.append("(" + " OR ".join(conds) + ")")

    if faixa and ben_nasc:
        parts = [p.strip() for p in faixa.split(",")]
        age = f"(EXTRACT('year' FROM current_date) - EXTRACT('year' FROM TRY_CAST({ben_nasc} AS DATE)))"
        ors = []
        for p in parts:
            m = re.match(r"(\d+)\s*-\s*(\d+)", p)
            m2 = re.match(r"(\d+)\s*\+$", p)
            if m:
                a, b = int(m.group(1)), int(m.group(2))
                ors.append(f"({age} BETWEEN {a} AND {b})")
            elif m2:
                a = int(m2.group(1))
                ors.append(f"({age} >= {a})")
        if ors:
            where_b.append("(" + " OR ".join(ors) + ")")

    where_b_sql = " AND ".join(where_b) if where_b else "TRUE"

    base = con.execute(
        f"SELECT COUNT(DISTINCT {ben_id}) FROM beneficiario WHERE {where_b_sql}",
        {"uf_csv": uf or "", "cid_csv": cidade or "", "sexo": sexo or "", "produto": produto or ""},
    ).fetchone()[0]

    utilizados = 0
    autorizacoes = 0
    if aut_ben and aut_dt:
        con.execute(
            "CREATE TEMP TABLE tmp_ben AS "
            f"SELECT DISTINCT {ben_id} AS id FROM beneficiario WHERE {where_b_sql}",
            {"uf_csv": uf or "", "cid_csv": cidade or "", "sexo": sexo or "", "produto": produto or ""},
        )
        r = con.execute(
            f"""
            SELECT COUNT(DISTINCT a.{aut_ben}) AS usados, COUNT(*) AS total_aut
            FROM autorizacao a
            JOIN tmp_ben b ON b.id = a.{aut_ben}
            WHERE {month_predicate(aut_dt)}
            """,
            {"comp": competencia},
        ).fetchone()
        if r:
            utilizados = int(r[0] or 0)
            autorizacoes = int(r[1] or 0)
        con.execute("DROP TABLE IF EXISTS tmp_ben")

    return {
        "competencia": competencia,
        "beneficiarios_base": int(base or 0),
        "beneficiarios_utilizados": int(utilizados or 0),
        "autorizacoes": int(autorizacoes or 0),
        "filtros_aplicados": {
            **({"uf": uf} if uf else {}),
            **({"cidade": cidade} if cidade else {}),
            **({"sexo": sexo} if sexo else {}),
            **({"produto": produto} if produto else {}),
            **({"faixa": faixa} if faixa else {}),
        },
    }
