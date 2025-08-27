# app.py
import os
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple

import duckdb
from fastapi import FastAPI, HTTPException, Query

# ---------- DuckDB path & connection ----------

def resolve_duck_path() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(here, "data", "operadora.duckdb"),
        os.path.join(os.path.dirname(here), "backend", "data", "operadora.duckdb"),
        os.path.join(here, "..", "data", "operadora.duckdb"),
        "/opt/render/project/src/backend/data/operadora.duckdb",
    ]
    for p in candidates:
        p = os.path.normpath(p)
        if os.path.exists(p):
            return p
    return os.path.normpath(os.path.join(here, "data", "operadora.duckdb"))

CONN_STR = resolve_duck_path()
con = duckdb.connect(CONN_STR, read_only=True)

app = FastAPI(title="Operadora Bot API", version="0.3.0")

# ---------- Utils ----------

def yyyymm_to_range(competencia: str) -> Tuple[date, date]:
    try:
        dt = datetime.strptime(competencia, "%Y-%m")
    except ValueError:
        raise HTTPException(status_code=400, detail="Formato inválido. Use YYYY-MM")
    first = date(dt.year, dt.month, 1)
    nxt = date(dt.year + (dt.month // 12), (dt.month % 12) + 1, 1)
    return first, nxt

def list_tables() -> List[str]:
    rows = con.execute("SHOW TABLES").fetchall()
    return [r[0] for r in rows]

def get_cols(table: str) -> List[str]:
    if table not in list_tables():
        raise HTTPException(status_code=400, detail=f"Tabela inexistente: {table}")
    rows = con.execute(f"PRAGMA table_info('{table}')").fetchall()
    return [r[1].lower() for r in rows]

def find_col(table: str, *candidates) -> Optional[str]:
    cols = set(get_cols(table))
    flat = []
    for c in candidates:
        if isinstance(c, (list, tuple)):
            flat.extend([str(x).lower() for x in c])
        else:
            flat.append(str(c).lower())
    for name in flat:
        if name in cols:
            return name
    return None

def must_col(table: str, *candidates) -> str:
    col = find_col(table, *candidates)
    if not col:
        raise HTTPException(
            status_code=422,
            detail=f"Coluna não encontrada em '{table}'. Procure por: {list(candidates)}"
        )
    return col

def as_yyyymm(colname: str) -> str:
    return f"strftime({colname}, '%Y-%m')"

# ---------- Column map (do seu dicionário + CSVs) ----------

COLMAP = {
    "beneficiario": {
        "id": ["id_beneficiario"],
        "situacao": ["ds_situacao"],
        "sexo": ["ds_sexo"],
        "nascimento": ["dt_nascimento"],
    },
    "conta": {
        "id_beneficiario": ["id_beneficiario"],
        "id_prestador": ["id_prestador_pagamento", "id_prestador_envio", "id_prestador"],
        "dt_comp": ["dt_mes_competencia", "dt_competencia", "dt_atendimento"],
        "vl_custo": ["vl_liberado"],  # <- custo = vl_liberado (fixo)
        "ds_item_n1": ["ds_classificacao_item_n1"],
        "ds_item_n2": ["ds_classificacao_item_n2"],
        "ds_item_n3": ["ds_classificacao_item_n3"],
        "municipio": ["ds_municipio"],
        "estado": ["ds_estado"],
    },
    "mensalidade": {
        "id_beneficiario": ["id_beneficiario"],
        "dt_comp": ["dt_competencia"],
        "vl_premio": ["vl_premio"],  # <- receita = vl_premio (fixo)
        # ignoramos copart, sca, pre_estab por exigência do usuário
    },
    "prestador": {
        "id": ["id_prestador"],
        "nome": ["nm_prestador"],
        "situacao": ["ds_situacao"],
        "municipio": ["ds_municipio"],
        "estado": ["ds_estado"],
        "tipo": ["ds_tipo"],
        "classificacao": ["ds_classificacao"],
    },
    "autorizacao": {
        "id_beneficiario": ["id_beneficiario"],
        "dt_aut": ["dt_autorizacao"],
        "id_prestador": ["id_prestador"],
        "qt_aut": ["qt_autorizada"],
    },
}

# ---------- Root & health ----------

@app.get("/")
def root():
    return {
        "ok": True,
        "message": "API do Operadora Bot. Use /docs para testar.",
        "db": CONN_STR,
        "tables": list_tables(),
        "endpoints": [
            "/health",
            "/debug/cols",
            "/kpi/sinistralidade/ultima",
            "/kpi/sinistralidade/competencia?competencia=YYYY-MM",
            "/kpi/prestador/top?competencia=YYYY-MM&limite=10",
            "/kpi/prestador/impacto?competencia=YYYY-MM&top=10",
            "/kpi/utilizacao/resumo?competencia=YYYY-MM",
        ],
    }

@app.get("/health")
def health():
    con.execute("SELECT 1")
    return {"status": "ok"}

@app.get("/debug/cols")
def debug_cols():
    out: Dict[str, List[str]] = {}
    for t in ["beneficiario","conta","mensalidade","prestador","autorizacao"]:
        try:
            out[t] = get_cols(t)
        except HTTPException as e:
            out[t] = [f"erro: {e.detail}"]
    return out

# ---------- KPI: Sinistralidade (vl_liberado / vl_premio) ----------

@app.get("/kpi/sinistralidade/competencia")
def sinistralidade_competencia(competencia: str = Query(..., description="YYYY-MM")):
    c_dt = must_col("conta", COLMAP["conta"]["dt_comp"])
    c_val = must_col("conta", COLMAP["conta"]["vl_custo"])          # vl_liberado
    m_dt = must_col("mensalidade", COLMAP["mensalidade"]["dt_comp"])
    m_val = must_col("mensalidade", COLMAP["mensalidade"]["vl_premio"])  # vl_premio

    first, nxt = yyyymm_to_range(competencia)

    (custo,) = con.execute(
        f"SELECT COALESCE(SUM({c_val}),0.0) FROM conta WHERE {c_dt} >= ? AND {c_dt} < ?",
        [first, nxt]
    ).fetchone()

    (receita,) = con.execute(
        f"SELECT COALESCE(SUM({m_val}),0.0) FROM mensalidade WHERE {m_dt} >= ? AND {m_dt} < ?",
        [first, nxt]
    ).fetchone()

    sin = float(custo) / float(receita) if receita and float(receita) != 0 else None
    return {
        "competencia": competencia,
        "custo": float(custo),
        "receita": float(receita),
        "sinistralidade": sin,
    }

@app.get("/kpi/sinistralidade/ultima")
def sinistralidade_ultima():
    c_dt = must_col("conta", COLMAP["conta"]["dt_comp"])
    m_dt = must_col("mensalidade", COLMAP["mensalidade"]["dt_comp"])

    (ultima_c,) = con.execute(f"SELECT MAX({as_yyyymm(c_dt)}) FROM conta").fetchone()
    (ultima_m,) = con.execute(f"SELECT MAX({as_yyyymm(m_dt)}) FROM mensalidade").fetchone()

    if not ultima_c and not ultima_m:
        raise HTTPException(status_code=404, detail="Sem dados em conta/mensalidade")

    comp = min([x for x in [ultima_c, ultima_m] if x is not None])
    return sinistralidade_competencia(comp)

# ---------- KPI: Prestador ----------

@app.get("/kpi/prestador/top")
def prestador_top(competencia: str = Query(..., description="YYYY-MM"),
                  limite: int = 10):
    dtc = must_col("conta", COLMAP["conta"]["dt_comp"])
    idp = must_col("conta", COLMAP["conta"]["id_prestador"])
    pid = must_col("prestador", COLMAP["prestador"]["id"])
    nm  = must_col("prestador", COLMAP["prestador"]["nome"])

    first, nxt = yyyymm_to_range(competencia)
    rows = con.execute(f"""
        SELECT c.{idp} AS id_prestador, p.{nm} AS nome, COUNT(*) AS score
        FROM conta c
        LEFT JOIN prestador p ON p.{pid} = c.{idp}
        WHERE c.{dtc} >= ? AND c.{dtc} < ?
        GROUP BY 1,2
        ORDER BY score DESC
        LIMIT ?
    """, [first, nxt, limite]).fetchall()

    return {
        "competencia": competencia,
        "top": [{"id_prestador": r[0], "nome": r[1], "score": int(r[2])} for r in rows]
    }

@app.get("/kpi/prestador/impacto")
def prestador_impacto(competencia: str = Query(..., description="YYYY-MM"),
                      top: int = 10):
    dtc = must_col("conta", COLMAP["conta"]["dt_comp"])
    idp = must_col("conta", COLMAP["conta"]["id_prestador"])
    val = must_col("conta", COLMAP["conta"]["vl_custo"])  # vl_liberado
    pid = must_col("prestador", COLMAP["prestador"]["id"])
    nm  = must_col("prestador", COLMAP["prestador"]["nome"])

    first, nxt = yyyymm_to_range(competencia)
    rows = con.execute(f"""
        SELECT c.{idp} AS id_prestador, p.{nm} AS nome, SUM(c.{val}) AS valor
        FROM conta c
        LEFT JOIN prestador p ON p.{pid} = c.{idp}
        WHERE c.{dtc} >= ? AND c.{dtc} < ?
        GROUP BY 1,2
        ORDER BY valor DESC
        LIMIT ?
    """, [first, nxt, top]).fetchall()

    return {
        "competencia": competencia,
        "impacto": [{"id_prestador": r[0], "nome": r[1], "valor": float(r[2] or 0)} for r in rows]
    }

# ---------- KPI: Utilização ----------

@app.get("/kpi/utilizacao/resumo")
def utilizacao_resumo(competencia: str = Query(..., description="YYYY-MM")):
    b_sit = must_col("beneficiario", COLMAP["beneficiario"]["situacao"])
    (ativos_total,) = con.execute(
        f"SELECT COUNT(*) FROM beneficiario WHERE UPPER({b_sit}) LIKE 'ATIV%'"
    ).fetchone()

    dtc  = must_col("conta", COLMAP["conta"]["dt_comp"])
    c_bid = must_col("conta", COLMAP["conta"]["id_beneficiario"])

    first, nxt = yyyymm_to_range(competencia)
    (usaram,) = con.execute(f"""
        SELECT COUNT(DISTINCT c.{c_bid})
        FROM conta c
        WHERE c.{dtc} >= ? AND c.{dtc} < ?
    """, [first, nxt]).fetchone()

    taxa = float(usaram) / float(ativos_total) if ativos_total else None
    return {
        "competencia": competencia,
        "ativos_total": int(ativos_total),
        "utilizaram": int(usaram),
        "taxa_utilizacao": taxa
    }
