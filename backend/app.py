# backend/app.py
from __future__ import annotations

import os
import re
from typing import Dict, List, Optional, Tuple

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

# Origens permitidas (front em Render + localhost)
ALLOWED_ORIGINS = [
    "https://operadora-bot-1.onrender.com",  # FRONT (Render)
    "https://operadora-bot.onrender.com",    # acessar API no navegador
    "http://localhost:5173",                 # Vite (dev)
    "http://localhost:3000",                 # React (dev)
]

# ------------------------------------------------------------------------------
# App
# ------------------------------------------------------------------------------

app = FastAPI(title="Operadora KPIs", version="0.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=False,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

# (Opcional) aceita pré-flight genérico
@app.options("/{rest_of_path:path}")
def preflight(rest_of_path: str):
    return Response(status_code=204)


# ------------------------------------------------------------------------------
# DB helpers
# ------------------------------------------------------------------------------

def connect() -> duckdb.DuckDBPyConnection:
    try:
        con = duckdb.connect(DB_PATH, read_only=True)
        return con
    except Exception as e:
        raise HTTPException(
            500, detail=f"Falha ao abrir DuckDB: {e}"
        )


def list_tables(con: duckdb.DuckDBPyConnection) -> List[str]:
    rows = con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema='main' ORDER BY 1"
    ).fetchall()
    return [r[0] for r in rows]


def get_cols(con: duckdb.DuckDBPyConnection, table: str) -> List[str]:
    rows = con.execute(f"PRAGMA table_info('{table}')").fetchall()
    # columns: (cid, name, type, notnull, dflt_value, pk)
    return [r[1] for r in rows]


def find_col(con: duckdb.DuckDBPyConnection, table: str, candidates: List[str]) -> Optional[str]:
    """
    Procura por uma coluna em 'table' que "pareça" com uma das candidates
    (case-insensitive; aceita prefixos/contains).
    """
    cols = get_cols(con, table)
    cols_lower = {c.lower(): c for c in cols}

    # 1) match exato
    for want in candidates:
        if want.lower() in cols_lower:
            return cols_lower[want.lower()]

    # 2) contains / prefix
    for want in candidates:
        pat = want.lower()
        for c in cols:
            cl = c.lower()
            if cl == pat or cl.startswith(pat) or pat in cl:
                return c

    return None


def month_predicate(col: str, param_name: str = "comp") -> str:
    """
    Gera um predicado robusto para filtrar por competência YYYY-MM:
    - tenta strftime se a coluna for date/timestamp
    - tenta SUBSTR para texto
    - tenta igualdade direta
    """
    # DuckDB aceita TRY_CAST
    return (
        f"(strftime('%Y-%m', TRY_CAST({col} AS DATE)) = ${param_name} "
        f"OR substr(CAST({col} AS VARCHAR), 1, 7) = ${param_name} "
        f"OR {col} = ${param_name})"
    )


def sum_value(
    con: duckdb.DuckDBPyConnection,
    table: str,
    val_col_candidates: List[str],
    date_col_candidates: List[str],
    competencia: str,
    extra_where: str = "",
) -> float:
    val_col = find_col(con, table, val_col_candidates)
    date_col = find_col(con, table, date_col_candidates)

    if not val_col or not date_col:
        # Sem uma das colunas essenciais:
        return 0.0

    where_parts = [month_predicate(date_col)]
    if extra_where:
        where_parts.append(f"({extra_where})")
    where_sql = " AND ".join(where_parts)

    sql = f"""
        SELECT COALESCE(SUM(CAST({val_col} AS DOUBLE)), 0) AS total
        FROM {table}
        WHERE {where_sql}
    """
    try:
        res = con.execute(sql, {"comp": competencia}).fetchone()
        return float(res[0]) if res and res[0] is not None else 0.0
    except Exception:
        return 0.0


# ------------------------------------------------------------------------------
# Raiz / Health / utilidades
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
    return {
        "ok": True,
        "db": DB_PATH,
        "tables": list_tables(con),
    }


@app.get("/debug/cols", tags=["Meta"])
def debug_cols():
    con = connect()
    info: Dict[str, List[str]] = {}
    for t in list_tables(con):
        info[t] = get_cols(con, t)
    return info


# ------------------------------------------------------------------------------
# KPI – Sinistralidade
#   Definição acordada: custo = SUM(conta.vl_liberado)
#                        premio = SUM(mensalidade.vl_premio)
# ------------------------------------------------------------------------------

def sinis_comp(con: duckdb.DuckDBPyConnection, competencia: str) -> Dict[str, float | str]:
    custo = sum_value(
        con,
        table="conta",
        val_col_candidates=["vl_liberado", "valor_liberado", "vl_pago"],
        date_col_candidates=["dt_mes_competencia", "dt_competencia", "dt_conta", "dt_lancamento"],
        competencia=competencia,
    )

    premio = sum_value(
        con,
        table="mensalidade",
        val_col_candidates=["vl_premio", "valor_premio", "vl_receita"],
        date_col_candidates=["dt_competencia", "dt_mes_competencia", "dt_pagamento"],
        competencia=competencia,
    )

    sinistralidade = (custo / premio) if premio and premio > 0 else 0.0

    return {
        "competencia": competencia,
        "sinistro": custo,
        "receita": premio,
        "sinistralidade": sinistralidade,
    }


@app.get("/kpi/sinistralidade/competencia", tags=["Sinistralidade"])
def sinistralidade_competencia(competencia: str = Query(..., pattern=r"^\d{4}-\d{2}$")):
    con = connect()
    return sinis_comp(con, competencia)


@app.get("/kpi/sinistralidade/ultima", tags=["Sinistralidade"])
def sinistralidade_ultima():
    """
    Pega a última competência encontrada em 'mensalidade' OU 'conta'
    (ordena desc pelo mês detectado).
    """
    con = connect()

    # tenta extrair YYYY-MM da coluna de data
    m_date = find_col(con, "mensalidade", ["dt_competencia", "dt_mes_competencia", "dt_pagamento"])
    c_date = find_col(con, "conta", ["dt_mes_competencia", "dt_competencia", "dt_conta", "dt_lancamento"])

    meses: List[str] = []

    if m_date:
        sqlm = f"SELECT DISTINCT strftime('%Y-%m', TRY_CAST({m_date} AS DATE)) AS m FROM mensalidade WHERE m IS NOT NULL ORDER BY 1 DESC LIMIT 1"
        try:
            r = con.execute(sqlm).fetchone()
            if r and r[0]:
                meses.append(r[0])
        except Exception:
            pass

    if c_date:
        sqlc = f"SELECT DISTINCT strftime('%Y-%m', TRY_CAST({c_date} AS DATE)) AS m FROM conta WHERE m IS NOT NULL ORDER BY 1 DESC LIMIT 1"
        try:
            r = con.execute(sqlc).fetchone()
            if r and r[0]:
                meses.append(r[0])
        except Exception:
            pass

    if not meses:
        raise HTTPException(400, detail="Não foi possível detectar a última competência.")

    comp = sorted(set(meses), reverse=True)[0]
    return sinis_comp(con, comp)


@app.get("/kpi/sinistralidade/media", tags=["Sinistralidade"])
def sinistralidade_media(janela: int = 6):
    """
    Média simples da sinistralidade das últimas N competências (default 6).
    """
    con = connect()
    # pegamos meses da mensalidade por ser receita
    m_date = find_col(con, "mensalidade", ["dt_competencia", "dt_mes_competencia", "dt_pagamento"])
    if not m_date:
        raise HTTPException(400, detail="Não encontrei coluna de competência em 'mensalidade'.")

    sql_mes = f"""
        SELECT DISTINCT strftime('%Y-%m', TRY_CAST({m_date} AS DATE)) AS m
        FROM mensalidade
        WHERE m IS NOT NULL
        ORDER BY 1 DESC
        LIMIT {int(janela)}
    """
    meses = [r[0] for r in con.execute(sql_mes).fetchall()]
    if not meses:
        raise HTTPException(400, detail="Sem competências em 'mensalidade'.")

    pontos = [sinis_comp(con, m) for m in meses]
    # média da razão mês a mês
    vals = [p["sinistralidade"] for p in pontos if p["sinistralidade"] is not None]
    media = sum(vals) / len(vals) if vals else 0.0

    return {"janela": janela, "media": media, "series": pontos}


# ------------------------------------------------------------------------------
# KPI – Prestador (top / impacto)
# ------------------------------------------------------------------------------

def prestador_top_data(
    con: duckdb.DuckDBPyConnection, competencia: str, limite: int
) -> List[Dict[str, object]]:
    conta_date = find_col(con, "conta", ["dt_mes_competencia", "dt_competencia", "dt_conta"])
    vl_col = find_col(con, "conta", ["vl_liberado", "vl_pago", "valor_liberado"])
    id_prest_conta = find_col(con, "conta", ["id_prestador_envio", "id_prestador", "cd_prestador"])
    id_prest = find_col(con, "prestador", ["id_prestador", "cd_prestador"])
    nm_prest = find_col(con, "prestador", ["nm_prestador", "nome", "nm_razao_social"])

    if not all([conta_date, vl_col, id_prest_conta, id_prest, nm_prest]):
        return []

    sql = f"""
        WITH ag AS (
            SELECT
                {id_prest_conta} AS id_prestador,
                COALESCE(SUM(CAST({vl_col} AS DOUBLE)), 0) AS score
            FROM conta
            WHERE {month_predicate(conta_date)}
            GROUP BY 1
        )
        SELECT
            a.id_prestador,
            p.{nm_prest} AS nome,
            a.score
        FROM ag a
        LEFT JOIN prestador p ON p.{id_prest} = a.id_prestador
        ORDER BY a.score DESC
        LIMIT {int(limite)}
    """
    rows = con.execute(sql, {"comp": competencia}).fetchall()
    out = []
    for r in rows:
        out.append({"id_prestador": r[0], "nome": r[1], "score": r[2]})
    return out


@app.get("/kpi/prestador/top", tags=["Prestador"])
def kpi_prestador_top(
    competencia: str = Query(..., pattern=r"^\d{4}-\d{2}$"),
    limite: int = 10,
):
    con = connect()
    return {"competencia": competencia, "top": prestador_top_data(con, competencia, limite)}


@app.get("/kpi/prestador/impacto", tags=["Prestador"])
def kpi_prestador_impacto(
    competencia: str = Query(..., pattern=r"^\d{4}-\d{2}$"),
    top: int = 10,
):
    # aqui usamos o mesmo critério do "top"
    con = connect()
    data = prestador_top_data(con, competencia, top)
    return {"competencia": competencia, "top": data}


# ------------------------------------------------------------------------------
# KPI – Utilização (resumo por filtros simples)
# ------------------------------------------------------------------------------

@app.get("/kpi/utilizacao/resumo", tags=["Utilização"])
def kpi_utilizacao_resumo(
    competencia: str = Query(..., pattern=r"^\d{4}-\d{2}$"),
    produto: Optional[str] = None,
    uf: Optional[str] = None,
    cidade: Optional[str] = None,
    sexo: Optional[str] = None,
    faixa: Optional[str] = None,  # ex: "0-18, 19-59, 60+"
):
    con = connect()

    # Colunas em beneficiario
    ben_id = find_col(con, "beneficiario", ["id_beneficiario", "cd_beneficiario"])
    ben_uf = find_col(con, "beneficiario", ["uf", "ds_uf", "sg_uf"])
    ben_cidade = find_col(con, "beneficiario", ["cidade", "nm_cidade", "ds_municipio"])
    ben_sexo = find_col(con, "beneficiario", ["sexo", "ds_sexo", "cd_sexo"])
    ben_nasc = find_col(con, "beneficiario", ["dt_nascimento", "nascimento"])

    # produto (id ou nome)
    ben_prod_id = find_col(con, "beneficiario", ["id_produto", "cd_plano", "id_plano"])
    ben_prod_nome = find_col(con, "beneficiario", ["ds_produto", "ds_plano", "nome_plano"])

    # Colunas em autorizacao
    aut_ben = find_col(con, "autorizacao", ["id_beneficiario", "cd_beneficiario"])
    aut_date = find_col(con, "autorizacao", ["dt_autorizacao", "dt_solicitacao"])

    if not ben_id:
        raise HTTPException(400, detail="Não encontrei identificador em 'beneficiario'.")

    # Filtros aplicados
    filtros: Dict[str, object] = {}

    where_ben = []
    if uf and ben_uf:
        filtros["uf"] = uf
        # aceita múltiplas "SP, RJ"
        where_ben.append(f"{ben_uf} IN (SELECT TRIM(x) FROM string_split($uf_csv, ','))")
    if cidade and ben_cidade:
        filtros["cidade"] = cidade
        where_ben.append(f"{ben_cidade} IN (SELECT TRIM(x) FROM string_split($cid_csv, ','))")
    if sexo and ben_sexo:
        filtros["sexo"] = sexo
        where_ben.append(f"upper({ben_sexo}) = upper($sexo)")
    if produto and (ben_prod_id or ben_prod_nome):
        filtros["produto"] = produto
        conds = []
        if ben_prod_id:
            conds.append(f"CAST({ben_prod_id} AS VARCHAR) = $produto OR {ben_prod_id} IN (SELECT TRIM(x) FROM string_split($produto, ','))")
        if ben_prod_nome:
            conds.append(f"upper({ben_prod_nome}) LIKE upper('%' || $produto || '%')")
        where_ben.append("(" + " OR ".join(conds) + ")")

    # (faixa etária opcional) – cálculo simples por year
    if faixa and ben_nasc:
        filtros["faixa"] = faixa
        # Suporta "0-18, 19-59, 60+"
        # Monta um OR com intervalos
        parts = [p.strip() for p in faixa.split(",")]
        age_expr = f"(EXTRACT('year' FROM current_date) - EXTRACT('year' FROM TRY_CAST({ben_nasc} AS DATE)))"
        faixa_ors = []
        for p in parts:
            m = re.match(r"(\d+)\s*-\s*(\d+)", p)
            m2 = re.match(r"(\d+)\s*\+$", p)
            if m:
                a, b = int(m.group(1)), int(m.group(2))
                faixa_ors.append(f"({age_expr} BETWEEN {a} AND {b})")
            elif m2:
                a = int(m2.group(1))
                faixa_ors.append(f"({age_expr} >= {a})")
        if faixa_ors:
            where_ben.append("(" + " OR ".join(faixa_ors) + ")")

    where_ben_sql = " AND ".join(where_ben) if where_ben else "TRUE"

    # base de beneficiários (após filtros)
    sql_base = f"SELECT COUNT(DISTINCT {ben_id}) FROM beneficiario WHERE {where_ben_sql}"
    base = con.execute(
        sql_base,
        {
            "uf_csv": uf or "",
            "cid_csv": cidade or "",
            "sexo": sexo or "",
            "produto": produto or "",
        },
    ).fetchone()[0]

    # utilização: beneficiários que possuem autorização na competência
    utilizados = 0
    autorizacoes = 0

    if aut_ben and aut_date:
        # IDs filtrados de beneficiário
        sql_ids = f"SELECT DISTINCT {ben_id} AS id FROM beneficiario WHERE {where_ben_sql}"
        con.execute("CREATE TEMP TABLE tmp_ben AS " + sql_ids,
                    {"uf_csv": uf or "", "cid_csv": cidade or "", "sexo": sexo or "", "produto": produto or ""})

        where_aut = month_predicate(aut_date)
        sql_util = f"""
            SELECT COUNT(DISTINCT a.{aut_ben}) AS usados, COUNT(*) AS total_aut
            FROM autorizacao a
            INNER JOIN tmp_ben b ON b.id = a.{aut_ben}
            WHERE {where_aut}
        """
        r = con.execute(sql_util, {"comp": competencia}).fetchone()
        if r:
            utilizados = int(r[0] or 0)
            autorizacoes = int(r[1] or 0)

        con.execute("DROP TABLE IF EXISTS tmp_ben")

    return {
        "competencia": competencia,
        "beneficiarios_base": int(base or 0),
        "beneficiarios_utilizados": int(utilizados or 0),
        "autorizacoes": int(autorizacoes or 0),
        "filtros_aplicados": filtros,
    }
