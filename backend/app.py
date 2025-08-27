# backend/app.py
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import re
import duckdb
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

APP_TITLE = "Operadora KPIs"
APP_VERSION = "0.2.2"

app = FastAPI(title=APP_TITLE, version=APP_VERSION)

# ---------------------------------------------------------------------
# DB path (local e Render)
# ---------------------------------------------------------------------
def _resolve_db_path() -> Path:
    here = Path(__file__).resolve().parent
    candidates = [
        here / "data" / "operadora.duckdb",
        here.parent / "backend" / "data" / "operadora.duckdb",
        Path.cwd() / "backend" / "data" / "operadora.duckdb",
        Path.cwd() / "data" / "operadora.duckdb",
        Path("/opt/render/project/src/backend/data/operadora.duckdb"),
    ]
    for p in candidates:
        if p.exists():
            return p
    raise HTTPException(
        status_code=500,
        detail="Falha ao abrir DuckDB: operadora.duckdb não encontrado em backend/data/.",
    )

DB_PATH = _resolve_db_path()
try:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    con.execute("PRAGMA threads=4")
except Exception as e:
    raise HTTPException(status_code=500, detail=f"Falha ao abrir DuckDB: {e}")

# ---------------------------------------------------------------------
# Utilitários de esquema
# ---------------------------------------------------------------------
def get_cols(table: str) -> List[str]:
    rows = con.execute(f"PRAGMA table_info('{table}')").fetchall()
    return [r[1] for r in rows]

def _norm(s: str) -> str:
    s = s.lower()
    return re.sub(r"[^a-z0-9_]+", "", s)

def find_col(table: str, candidates: List[str]) -> Optional[str]:
    cols = get_cols(table)
    by_lower = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand in by_lower:
            return by_lower[cand]
    by_norm = {_norm(c): c for c in cols}
    for cand in candidates:
        if _norm(cand) in by_norm:
            return by_norm[_norm(cand)]
    return None

# candidatos “exatos”
DATE_CANDIDATES = [
    "dt_competencia","competencia","dt_referencia","dt_data",
    "dt_mes_competencia","mes_competencia","dt_mes_ref","mes_ref","mes",
    "dt_autorizacao","dt_entrada",
]
VALUE_CANDIDATES = [
    # "conta" usuais
    "vl_liberado","vl_pago","vl_apresentado","vl_total","vl_bruto","vl_liquido","valor","vl_cobranca",
    # "mensalidade" do seu CSV
    "vl_premio","vl_sca","vl_coparticipacao","vl_pre_estabelecido",
]

def _pick_date_col_fuzzy(cols: List[str]) -> Optional[str]:
    for c in cols:
        cl = c.lower()
        if cl.startswith("dt") and (("comp" in cl) or ("ref" in cl) or ("mes" in cl)):
            return c
    for c in cols:
        cl = c.lower()
        if ("compet" in cl) or (re.search(r"\bmes\b", cl) is not None):
            return c
    for c in cols:
        if c.lower().startswith("dt"):
            return c
    return None

def _pick_value_col_fuzzy(cols: List[str]) -> Optional[str]:
    for c in cols:
        cl = c.lower()
        if cl.startswith("vl") and (("liber" in cl) or ("pago" in cl) or ("apres" in cl) or ("total" in cl) or ("liq" in cl) or ("brut" in cl)):
            return c
    for c in cols:
        cl = c.lower()
        if ("valor" in cl) or cl.endswith("_valor"):
            return c
    for c in cols:
        if c.lower().startswith("vl"):
            return c
    return None

def find_date_value_cols(table: str) -> Tuple[str, str]:
    cols = get_cols(table)
    by_lower = {c.lower(): c for c in cols}
    date_col = next((by_lower[n] for n in DATE_CANDIDATES if n in by_lower), None)
    value_col = next((by_lower[n] for n in VALUE_CANDIDATES if n in by_lower), None)
    if not date_col:
        date_col = _pick_date_col_fuzzy(cols)
    if not value_col:
        value_col = _pick_value_col_fuzzy(cols)
    if not date_col or not value_col:
        raise HTTPException(
            status_code=400,
            detail=f"Não encontrei DATA/VALOR em '{table}'. Colunas disponíveis: {cols}",
        )
    return date_col, value_col

# Receita em “mensalidade”: soma automática do que existir
def receita_expr_mensalidade() -> str:
    cols = get_cols("mensalidade")
    candidatos = ["vl_premio","vl_sca","vl_coparticipacao","vl_pre_estabelecido"]
    presentes = [c for c in candidatos if c in cols]
    if presentes:
        soma = " + ".join([f"COALESCE({c},0)" for c in presentes])
        return f"({soma})"
    # fallback para 1 coluna “vl_*”
    _, vcol = find_date_value_cols("mensalidade")
    return f"COALESCE({vcol},0)"

def month_str(d: date) -> str:
    return d.strftime("%Y-%m")

def parse_competencia(s: str) -> str:
    try:
        dt = datetime.strptime(s, "%Y-%m").date()
        return month_str(dt)
    except Exception:
        raise HTTPException(status_code=422, detail="competencia deve ser YYYY-MM")

def latest_common_month() -> str:
    c_date, _ = find_date_value_cols("conta")
    m_date, _ = find_date_value_cols("mensalidade")
    rows = con.execute(
        f"""
        WITH c AS (SELECT DISTINCT strftime('%Y-%m', CAST({c_date} AS DATE)) AS mes FROM conta),
             m AS (SELECT DISTINCT strftime('%Y-%m', CAST({m_date} AS DATE)) AS mes FROM mensalidade)
        SELECT mes FROM c INTERSECT SELECT mes FROM m
        ORDER BY mes DESC LIMIT 1
        """
    ).fetchall()
    if not rows:
        rows = con.execute(f"SELECT strftime('%Y-%m', max(CAST({c_date} AS DATE))) FROM conta").fetchall()
    return rows[0][0]

# ---------------------------------------------------------------------
# Contagens (base, utilizados, autorizações)
# ---------------------------------------------------------------------
def count_beneficiarios_base(
    sexo: Optional[str] = None,
    uf: Optional[str] = None,
    cidade: Optional[str] = None,
    faixa: Optional[str] = None,
    competencia_ref: Optional[str] = None,
) -> int:
    tbl = "beneficiario"

    def maybe(colnames: List[str]) -> Optional[str]:
        return find_col(tbl, colnames)

    id_benef = maybe(["id_beneficiario","id_benef","cd_beneficiario"]) or "rowid"
    status_col = maybe(["ds_situacao","st_ativo","fl_ativo","status"])
    sexo_col = maybe(["ds_sexo","sexo","sx","cd_sexo"])
    uf_col = maybe(["sg_uf","uf","ds_uf","estado"])
    cidade_col = maybe(["ds_cidade","cidade","nm_cidade","municipio"])
    nasc_col = maybe(["dt_nascimento","nascimento","dt_nasc"])

    where, params = [], []

    if status_col:
        where.append(f"(upper({status_col}) IN ('ATIVO','ATIVA') OR {status_col} IN (1,'1','S','Y'))")

    if sexo and sexo_col:
        where.append(f"upper({sexo_col}) = ?")
        params.append(sexo.strip().upper())

    if uf and uf_col:
        ufs = [u.strip().upper() for u in uf.split(",") if u.strip()]
        if ufs:
            where.append(f"upper({uf_col}) IN ({','.join(['?']*len(ufs))})")
            params.extend(ufs)

    if cidade and cidade_col:
        cidades = [c.strip() for c in cidade.split(",") if c.strip()]
        if cidades:
            likes = [f"upper({cidade_col}) LIKE ?" for _ in cidades]
            where.append("(" + " OR ".join(likes) + ")")
            params.extend([f"%{c.upper()}%" for c in cidades])

    if faixa and nasc_col and competencia_ref:
        ref = datetime.strptime(competencia_ref + "-15", "%Y-%m-%d").date()
        parts = []
        for token in [f.strip() for f in faixa.split(",") if f.strip()]:
            if "-" in token:
                try:
                    a,b = token.split("-",1)
                    parts.append(f"(date_diff('year', CAST({nasc_col} AS DATE), DATE '{ref}') BETWEEN {int(a)} AND {int(b)})")
                except:
                    pass
            elif token.endswith("+"):
                try:
                    a = int(token[:-1])
                    parts.append(f"(date_diff('year', CAST({nasc_col} AS DATE), DATE '{ref}') >= {a})")
                except:
                    pass
        if parts:
            where.append("(" + " OR ".join(parts) + ")")

    sql = f"SELECT COUNT(*) FROM {tbl}"
    if where:
        sql += " WHERE " + " AND ".join(where)
    (n,) = con.execute(sql, params).fetchone()
    return int(n)

def count_utilizados_e_autorizacoes(
    competencia: str,
    produto: Optional[str],
    uf: Optional[str],
    cidade: Optional[str],
    sexo: Optional[str],
    faixa: Optional[str],
) -> Tuple[int,int]:
    aut_tbl, ben_tbl = "autorizacao", "beneficiario"

    dt_aut = find_col(aut_tbl, ["dt_autorizacao","dt_entrada","dt_data"]) or \
             _pick_date_col_fuzzy(get_cols(aut_tbl))
    if not dt_aut:
        raise HTTPException(status_code=400, detail=f"Coluna de data não encontrada em '{aut_tbl}'.")

    id_ben_aut = find_col(aut_tbl, ["id_beneficiario","id_benef","cd_beneficiario"])
    if not id_ben_aut:
        raise HTTPException(status_code=400, detail=f"id_beneficiario não encontrado em '{aut_tbl}'.")

    id_ben = find_col(ben_tbl, ["id_beneficiario","id_benef","cd_beneficiario"]) or id_ben_aut
    sexo_col = find_col(ben_tbl, ["ds_sexo","sexo","sx","cd_sexo"])
    uf_col = find_col(ben_tbl, ["sg_uf","uf","ds_uf","estado"])
    cidade_col = find_col(ben_tbl, ["ds_cidade","cidade","nm_cidade","municipio"])
    nasc_col = find_col(ben_tbl, ["dt_nascimento","nascimento","dt_nasc"])

    cd_item = find_col(aut_tbl, ["cd_item","id_item","cd_procedimento","cd_produto"])
    ds_item = find_col(aut_tbl, ["ds_item","produto","ds_procedimento","nm_item"])

    where = [f"strftime('%Y-%m', CAST(a.{dt_aut} AS DATE)) = ?"]
    params: List = [competencia]

    if produto:
        p = produto.strip()
        if p:
            if p.isdigit() and cd_item:
                where.append(f"a.{cd_item} = ?")
                params.append(int(p))
            elif ds_item:
                where.append(f"upper(a.{ds_item}) LIKE ?")
                params.append(f"%{p.upper()}%")

    if sexo and sexo_col:
        where.append(f"upper(b.{sexo_col}) = ?")
        params.append(sexo.strip().upper())

    if uf and uf_col:
        ufs = [u.strip().upper() for u in uf.split(",") if u.strip()]
        if ufs:
            where.append(f"upper(b.{uf_col}) IN ({','.join(['?']*len(ufs))})")
            params.extend(ufs)

    if cidade and cidade_col:
        cidades = [c.strip() for c in cidade.split(",") if c.strip()]
        if cidades:
            likes = [f"upper(b.{cidade_col}) LIKE ?" for _ in cidades]
            where.append("(" + " OR ".join(likes) + ")")
            params.extend([f"%{c.upper()}%" for c in cidades])

    if faixa and nasc_col:
        ref = datetime.strptime(competencia + "-15", "%Y-%m-%d").date()
        parts = []
        for token in [f.strip() for f in faixa.split(",") if f.strip()]:
            if "-" in token:
                try:
                    a,b = token.split("-",1)
                    parts.append(f"(date_diff('year', CAST(b.{nasc_col} AS DATE), DATE '{ref}') BETWEEN {int(a)} AND {int(b)})")
                except:
                    pass
            elif token.endswith("+"):
                try:
                    a = int(token[:-1])
                    parts.append(f"(date_diff('year', CAST(b.{nasc_col} AS DATE), DATE '{ref}') >= {a})")
                except:
                    pass
        if parts:
            where.append("(" + " OR ".join(parts) + ")")

    sql_where = " AND ".join(where)

    (utilizados,) = con.execute(
        f"""
        SELECT COUNT(DISTINCT a.{id_ben_aut})
        FROM {aut_tbl} a
        LEFT JOIN {ben_tbl} b ON b.{id_ben} = a.{id_ben_aut}
        WHERE {sql_where}
        """,
        params,
    ).fetchone()

    (aut_count,) = con.execute(
        f"""
        SELECT COUNT(*)
        FROM {aut_tbl} a
        LEFT JOIN {ben_tbl} b ON b.{id_ben} = a.{id_ben_aut}
        WHERE {sql_where}
        """,
        params,
    ).fetchone()

    return int(utilizados), int(aut_count)

# ---------------------------------------------------------------------
# CORS
# ---------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------
@app.get("/health", tags=["default"])
def health():
    return {"ok": True, "db": str(DB_PATH), "version": APP_VERSION}

# -------------------- SINISTRALIDADE -------------------------------
@app.get("/kpi/sinistralidade/ultima", tags=["default"])
def kpi_sinistralidade_ultima():
    mes = latest_common_month()
    c_date, c_val = find_date_value_cols("conta")
    m_date, _ = find_date_value_cols("mensalidade")  # só para pegar a data
    receita_expr = receita_expr_mensalidade()

    (sinistro,) = con.execute(
        f"SELECT COALESCE(SUM({c_val}),0) FROM conta WHERE strftime('%Y-%m', CAST({c_date} AS DATE)) = ?",
        [mes],
    ).fetchone()
    (receita,) = con.execute(
        f"SELECT COALESCE(SUM({receita_expr}),0) FROM mensalidade WHERE strftime('%Y-%m', CAST({m_date} AS DATE)) = ?",
        [mes],
    ).fetchone()

    return {
        "competencia": mes,
        "sinistro": float(sinistro),
        "receita": float(receita),
        "sinistralidade": (float(sinistro)/float(receita) if receita else None),
    }

@app.get("/kpi/sinistralidade/media", tags=["default"])
def kpi_sinistralidade_media(janela_meses: int = Query(12, ge=1, le=60)):
    c_date, c_val = find_date_value_cols("conta")
    m_date, _ = find_date_value_cols("mensalidade")
    receita_expr = receita_expr_mensalidade()

    rows = con.execute(
        f"""
        WITH c AS (SELECT DISTINCT strftime('%Y-%m', CAST({c_date} AS DATE)) AS mes FROM conta),
             m AS (SELECT DISTINCT strftime('%Y-%m', CAST({m_date} AS DATE)) AS mes FROM mensalidade)
        SELECT mes FROM c INTERSECT SELECT mes FROM m
        ORDER BY mes DESC LIMIT {janela_meses}
        """
    ).fetchall()
    meses = [r[0] for r in rows]
    detalhe, acum, n = [], 0.0, 0
    for mes in meses:
        (s,) = con.execute(
            f"SELECT COALESCE(SUM({c_val}),0) FROM conta WHERE strftime('%Y-%m', CAST({c_date} AS DATE)) = ?",
            [mes],
        ).fetchone()
        (r,) = con.execute(
            f"SELECT COALESCE(SUM({receita_expr}),0) FROM mensalidade WHERE strftime('%Y-%m', CAST({m_date} AS DATE)) = ?",
            [mes],
        ).fetchone()
        ratio = float(s)/float(r) if r else None
        detalhe.append({"competencia": mes, "sinistro": float(s), "receita": float(r), "sinistralidade": ratio})
        if ratio is not None:
            acum += ratio
            n += 1
    media = (acum / n) if n else None
    return {"meses": meses[::-1], "media": media, "detalhe": detalhe[::-1]}

# -------------------- PRESTADOR TOP / IMPACTO ----------------------
@app.get("/kpi/prestador/top", tags=["default"])
def kpi_prestador_top(competencia: str = Query(..., description="YYYY-MM"), limite: int = Query(10, ge=1, le=100)):
    mes = parse_competencia(competencia)

    prest_col = find_col("conta", ["id_prestador","id_prestador_envio","id_prestador_pagamento"])
    if not prest_col:
        raise HTTPException(status_code=400, detail="Coluna de prestador não encontrada em 'conta'.")

    c_date, c_val = find_date_value_cols("conta")

    p_id = find_col("prestador", ["id_prestador","cd_prestador"]) or "id_prestador"
    p_nome = find_col("prestador", ["nm_prestador","ds_prestador","nm_razao_social","nome"]) or "nm_prestador"

    rows = con.execute(
        f"""
        SELECT
            c.{prest_col} AS id_prestador,
            COALESCE(p.{p_nome}, CAST(c.{prest_col} AS VARCHAR)) AS nome,
            SUM(c.{c_val}) AS score
        FROM conta c
        LEFT JOIN prestador p ON p.{p_id} = c.{prest_col}
        WHERE strftime('%Y-%m', CAST(c.{c_date} AS DATE)) = ?
        GROUP BY 1,2
        ORDER BY 3 DESC
        LIMIT ?
        """,
        [mes, limite],
    ).fetchall()

    return {"competencia": mes, "top": [{"id_prestador": r[0], "nome": r[1], "score": float(r[2])} for r in rows]}

@app.get("/kpi/prestador/impacto", tags=["default"])
def kpi_prestador_impacto(competencia: str = Query(..., description="YYYY-MM"), top: int = Query(10, ge=1, le=100)):
    return kpi_prestador_top(competencia, top)

# -------------------- FAIXA x CUSTO --------------------------------
@app.get("/kpi/faixa/custo", tags=["default"])
def kpi_faixa_custo(competencia: str = Query(..., description="YYYY-MM")):
    mes = parse_competencia(competencia)
    c_date, c_val = find_date_value_cols("conta")
    id_ben_conta = find_col("conta", ["id_beneficiario","id_benef","cd_beneficiario"])
    nasc = find_col("beneficiario", ["dt_nascimento","nascimento","dt_nasc"])
    id_ben = find_col("beneficiario", ["id_beneficiario","id_benef","cd_beneficiario"])
    if not (id_ben_conta and nasc and id_ben):
        raise HTTPException(status_code=400, detail="Colunas para faixa-etária insuficientes.")

    ref = datetime.strptime(mes + "-15", "%Y-%m-%d").date()
    rows = con.execute(
        f"""
        WITH base AS (
            SELECT
                CASE
                    WHEN date_diff('year', CAST(b.{nasc} AS DATE), DATE '{ref}') <= 18 THEN '0-18'
                    WHEN date_diff('year', CAST(b.{nasc} AS DATE), DATE '{ref}') BETWEEN 19 AND 59 THEN '19-59'
                    ELSE '60+'
                END AS faixa,
                c.{c_val} AS valor
            FROM conta c
            JOIN beneficiario b ON b.{id_ben} = c.{id_ben_conta}
            WHERE strftime('%Y-%m', CAST(c.{c_date} AS DATE)) = ?
        )
        SELECT faixa, SUM(valor) AS valor
        FROM base
        GROUP BY 1
        ORDER BY CASE faixa WHEN '0-18' THEN 1 WHEN '19-59' THEN 2 ELSE 3 END
        """,
        [mes],
    ).fetchall()
    return {"competencia": mes, "faixas": [{"faixa": r[0], "valor": float(r[1])} for r in rows]}

# -------------------- UTILIZAÇÃO: RESUMO & EVOLUÇÃO -----------------
@app.get("/kpi/utilizacao/resumo", tags=["default"])
def kpi_utilizacao_resumo(
    competencia: str = Query(..., description="YYYY-MM"),
    produto: Optional[str] = Query(None),
    uf: Optional[str] = None,
    cidade: Optional[str] = None,
    sexo: Optional[str] = Query(None, description="M/F"),
    faixa: Optional[str] = Query(None, description="0-18, 19-59, 60+"),
):
    mes = parse_competencia(competencia)
    base = count_beneficiarios_base(sexo=sexo, uf=uf, cidade=cidade, faixa=faixa, competencia_ref=mes)
    utilizados, autoriz = count_utilizados_e_autorizacoes(
        competencia=mes, produto=produto, uf=uf, cidade=cidade, sexo=sexo, faixa=faixa
    )
    filtros: Dict[str,str] = {}
    if produto: filtros["produto"] = produto
    if uf: filtros["uf"] = uf
    if cidade: filtros["cidade"] = cidade
    if sexo: filtros["sexo"] = sexo
    if faixa: filtros["faixa"] = faixa
    return {
        "competencia": mes,
        "beneficiarios_base": base,
        "beneficiarios_utilizados": utilizados,
        "autorizacoes": autoriz,
        "filtros_aplicados": filtros,
    }

@app.get("/kpi/utilizacao/evolucao", tags=["default"])
def kpi_utilizacao_evolucao(meses: int = Query(12, ge=1, le=60)):
    aut_tbl = "autorizacao"
    dt_aut = find_col(aut_tbl, ["dt_autorizacao","dt_entrada","dt_data"]) or _pick_date_col_fuzzy(get_cols(aut_tbl))
    id_ben_aut = find_col(aut_tbl, ["id_beneficiario","id_benef","cd_beneficiario"])
    if not (dt_aut and id_ben_aut):
        raise HTTPException(status_code=400, detail="Colunas necessárias ausentes em 'autorizacao'.")

    rows = con.execute(
        f"""
        WITH m AS (
            SELECT strftime('%Y-%m', CAST({dt_aut} AS DATE)) AS mes FROM {aut_tbl}
            GROUP BY 1 ORDER BY 1 DESC LIMIT {meses}
        )
        SELECT
            m.mes,
            (SELECT COUNT(DISTINCT a.{id_ben_aut}) FROM {aut_tbl} a WHERE strftime('%Y-%m', CAST(a.{dt_aut} AS DATE)) = m.mes) AS utilizados,
            (SELECT COUNT(*) FROM {aut_tbl} a WHERE strftime('%Y-%m', CAST(a.{dt_aut} AS DATE)) = m.mes) AS autorizacoes
        FROM m ORDER BY m.mes
        """
    ).fetchall()
    return [{"competencia": r[0], "beneficiarios_utilizados": int(r[1]), "autorizacoes": int(r[2])} for r in rows]

# ---------------------------------------------------------------------
@app.get("/", include_in_schema=False)
def root():
    return {"msg": f"{APP_TITLE} — veja /docs"}
