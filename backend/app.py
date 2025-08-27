# backend/app.py
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import duckdb
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

APP_TITLE = "Operadora KPIs"
APP_VERSION = "0.2.0"

app = FastAPI(title=APP_TITLE, version=APP_VERSION)

# ---------------------------------------------------------------------
# DB: resolve caminho do DuckDB tanto local quanto no Render
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
        detail=(
            "Falha ao abrir DuckDB: arquivo operadora.duckdb não encontrado. "
            "Verifique o caminho em backend/data/."
        ),
    )


DB_PATH = _resolve_db_path()
try:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    con.execute("PRAGMA threads=4")
except Exception as e:
    raise HTTPException(status_code=500, detail=f"Falha ao abrir DuckDB: {e}")

# ---------------------------------------------------------------------
# Utilidades de esquema/colunas (tolerantes a variações de nomes)
# ---------------------------------------------------------------------
def get_cols(table: str) -> List[str]:
    rows = con.execute(f"PRAGMA table_info('{table}')").fetchall()
    return [r[1] for r in rows]  # r[1] é o nome da coluna


def find_col(table: str, candidates: List[str]) -> Optional[str]:
    cols = {c.lower(): c for c in get_cols(table)}
    for c in candidates:
        if c in cols:
            return cols[c]
    return None


# Candidatas genéricas de DATA/VALOR usadas nos seus CSVs
DATE_CANDIDATES = [
    # comuns
    "dt_competencia",
    "competencia",
    "dt_referencia",
    "dt_data",
    # seus arquivos
    "dt_mes_competencia",
    "mes_competencia",
    "dt_mes_ref",
    "mes_ref",
    "mes",
    "dt_autorizacao",
    "dt_entrada",
]

VALUE_CANDIDATES = [
    "vl_liberado",
    "vl_pago",
    "vl_apresentado",
    "vl_total",
    "vl_bruto",
    "vl_liquido",
    "valor",
    "vl_cobranca",
]


def find_date_value_cols(table: str) -> Tuple[str, str]:
    cols = {c.lower(): c for c in get_cols(table)}
    date_col = next((cols[n] for n in DATE_CANDIDATES if n in cols), None)
    value_col = next((cols[n] for n in VALUE_CANDIDATES if n in cols), None)

    if not date_col or not value_col:
        raise HTTPException(
            status_code=400,
            detail=f"Não encontrei DATA/VALOR em '{table}'. Colunas disponíveis: {list(cols.values())}",
        )
    return date_col, value_col


def month_str(d: date) -> str:
    return d.strftime("%Y-%m")


def parse_competencia(s: str) -> str:
    # Espera YYYY-MM
    try:
        dt = datetime.strptime(s, "%Y-%m").date()
        return month_str(dt)
    except Exception:
        raise HTTPException(status_code=422, detail="competencia deve ser YYYY-MM")


def latest_common_month() -> str:
    # Interseção de meses entre conta e mensalidade
    c_date, _ = find_date_value_cols("conta")
    m_date, _ = find_date_value_cols("mensalidade")
    rows = con.execute(
        f"""
        WITH c AS (
            SELECT DISTINCT strftime('%Y-%m', CAST({c_date} AS DATE)) AS mes FROM conta
        ),
        m AS (
            SELECT DISTINCT strftime('%Y-%m', CAST({m_date} AS DATE)) AS mes FROM mensalidade
        )
        SELECT mes FROM c
        INTERSECT
        SELECT mes FROM m
        ORDER BY mes DESC
        LIMIT 1
        """
    ).fetchall()
    if not rows:
        # fallback: último de conta
        rows = con.execute(
            f"SELECT strftime('%Y-%m', max(CAST({c_date} AS DATE))) FROM conta"
        ).fetchall()
    return rows[0][0]


def build_where_month(table: str, competencia: str) -> Tuple[str, List]:
    date_col = find_col(table, DATE_CANDIDATES)
    if not date_col:
        raise HTTPException(
            status_code=400,
            detail=f"Não encontrei coluna de data em '{table}'. Colunas: {get_cols(table)}",
        )
    return f"strftime('%Y-%m', CAST({date_col} AS DATE)) = ?", [competencia]


# ---------------------------------------------------------------------
# Contagens auxiliares para resumo/uso
# ---------------------------------------------------------------------
def count_beneficiarios_base(
    sexo: Optional[str] = None,
    uf: Optional[str] = None,
    cidade: Optional[str] = None,
    faixa: Optional[str] = None,
    competencia_ref: Optional[str] = None,
) -> int:
    """Conta base na tabela beneficiario com filtros quando as colunas existem."""
    tbl = "beneficiario"
    cols = {c.lower(): c for c in get_cols(tbl)}

    # coluna de ID
    id_benef = find_col(tbl, ["id_beneficiario", "id_benef", "cd_beneficiario"]) or "rowid"

    # status (se existir)
    status_col = find_col(tbl, ["ds_situacao", "st_ativo", "fl_ativo", "status"])

    # sexo, uf, cidade
    sexo_col = find_col(tbl, ["ds_sexo", "sexo", "sx", "cd_sexo"])
    uf_col = find_col(tbl, ["sg_uf", "uf", "ds_uf", "estado"])
    cidade_col = find_col(tbl, ["ds_cidade", "cidade", "nm_cidade", "municipio"])

    # nascimento para faixa etária
    nasc_col = find_col(tbl, ["dt_nascimento", "nascimento", "dt_nasc"])

    where = []
    params: List = []

    if status_col:
        # heurística: ativos
        where.append(f"(upper({status_col}) IN ('ATIVO','ATIVA') OR {status_col} IN (1, '1', 'S', 'Y'))")

    if sexo and sexo_col:
        where.append(f"upper({sexo_col}) = ?")
        params.append(sexo.strip().upper())

    if uf and uf_col:
        # aceita múltiplos separados por vírgula
        ufs = [u.strip().upper() for u in uf.split(",") if u.strip()]
        if ufs:
            where.append(f"upper({uf_col}) IN ({','.join(['?']*len(ufs))})")
            params.extend(ufs)

    if cidade and cidade_col:
        # LIKE para múltiplas cidades
        cidades = [c.strip() for c in cidade.split(",") if c.strip()]
        likes = [f"upper({cidade_col}) LIKE ?" for _ in cidades]
        where.append("(" + " OR ".join(likes) + ")")
        params.extend([f"%{c.upper()}%" for c in cidades])

    if faixa and nasc_col and competencia_ref:
        # faixa exemplo: "0-18, 19-59, 60+"
        ref = datetime.strptime(competencia_ref + "-15", "%Y-%m-%d").date()
        # Calcula idade aproximada em anos (DuckDB)
        # age = date_diff('year', nascimento, ref_date)
        where_age_parts = []
        for token in [f.strip() for f in faixa.split(",") if f.strip()]:
            if "-" in token:
                a, b = token.split("-", 1)
                try:
                    a_i = int(a)
                    b_i = int(b)
                    where_age_parts.append(
                        f"(date_diff('year', CAST({nasc_col} AS DATE), DATE '{ref}') BETWEEN {a_i} AND {b_i})"
                    )
                except Exception:
                    pass
            elif token.endswith("+"):
                try:
                    a_i = int(token[:-1])
                    where_age_parts.append(
                        f"(date_diff('year', CAST({nasc_col} AS DATE), DATE '{ref}') >= {a_i})"
                    )
                except Exception:
                    pass
        if where_age_parts:
            where.append("(" + " OR ".join(where_age_parts) + ")")

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
) -> Tuple[int, int]:
    """
    Conta distintos beneficiários e nº de autorizações no mês (tabela autorizacao),
    aplicando filtros quando existirem colunas.
    """
    aut_tbl = "autorizacao"
    ben_tbl = "beneficiario"
    aut_cols = {c.lower(): c for c in get_cols(aut_tbl)}
    ben_cols = {c.lower(): c for c in get_cols(ben_tbl)}

    dt_aut = find_col(aut_tbl, ["dt_autorizacao", "dt_entrada", "dt_data"])
    if not dt_aut:
        raise HTTPException(
            status_code=400,
            detail=f"Não encontrei coluna de data em '{aut_tbl}'. Colunas: {list(aut_cols.values())}",
        )

    id_ben_aut = find_col(aut_tbl, ["id_beneficiario", "id_benef", "cd_beneficiario"])
    if not id_ben_aut:
        # Sem id de beneficiário não dá pra contar distintos utilizados
        raise HTTPException(
            status_code=400,
            detail=f"Não encontrei id_beneficiario em '{aut_tbl}'. Colunas: {list(aut_cols.values())}",
        )

    id_ben = find_col(ben_tbl, ["id_beneficiario", "id_benef", "cd_beneficiario"]) or id_ben_aut
    sexo_col = find_col(ben_tbl, ["ds_sexo", "sexo", "sx", "cd_sexo"])
    uf_col = find_col(ben_tbl, ["sg_uf", "uf", "ds_uf", "estado"])
    cidade_col = find_col(ben_tbl, ["ds_cidade", "cidade", "nm_cidade", "municipio"])
    nasc_col = find_col(ben_tbl, ["dt_nascimento", "nascimento", "dt_nasc"])

    cd_item = find_col(aut_tbl, ["cd_item", "id_item", "cd_procedimento", "cd_produto"])
    ds_item = find_col(aut_tbl, ["ds_item", "produto", "ds_procedimento", "nm_item"])

    where = [f"strftime('%Y-%m', CAST(a.{dt_aut} AS DATE)) = ?"]
    params: List = [competencia]

    # produto: numérico => código; texto => nome
    if produto:
        p = produto.strip()
        if p:
            if p.isdigit() and cd_item:
                where.append(f"a.{cd_item} = ?")
                params.append(int(p))
            elif ds_item:
                where.append(f"upper(a.{ds_item}) LIKE ?")
                params.append(f"%{p.upper()}%")

    # filtros do beneficiário
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
        faixa_parts = []
        for token in [f.strip() for f in faixa.split(",") if f.strip()]:
            if "-" in token:
                try:
                    a, b = token.split("-", 1)
                    a_i, b_i = int(a), int(b)
                    faixa_parts.append(
                        f"(date_diff('year', CAST(b.{nasc_col} AS DATE), DATE '{ref}') BETWEEN {a_i} AND {b_i})"
                    )
                except Exception:
                    pass
            elif token.endswith("+"):
                try:
                    a_i = int(token[:-1])
                    faixa_parts.append(
                        f"(date_diff('year', CAST(b.{nasc_col} AS DATE), DATE '{ref}') >= {a_i})"
                    )
                except Exception:
                    pass
        if faixa_parts:
            where.append("(" + " OR ".join(faixa_parts) + ")")

    sql_where = " AND ".join(where)
    # distintos utilizados
    (utilizados,) = con.execute(
        f"""
        SELECT COUNT(DISTINCT a.{id_ben_aut})
        FROM {aut_tbl} a
        LEFT JOIN {ben_tbl} b ON b.{id_ben} = a.{id_ben_aut}
        WHERE {sql_where}
        """,
        params,
    ).fetchone()

    # nº autorizações
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
# CORS (front em outro serviço/domínio)
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


# -------------------- SINISTRALIDADE --------------------------------
@app.get("/kpi/sinistralidade/ultima", tags=["default"], summary="Sinistralidade Última")
def kpi_sinistralidade_ultima():
    mes = latest_common_month()
    c_date, c_val = find_date_value_cols("conta")
    m_date, m_val = find_date_value_cols("mensalidade")

    (sinistro,) = con.execute(
        f"""
        SELECT COALESCE(SUM({c_val}), 0)
        FROM conta
        WHERE strftime('%Y-%m', CAST({c_date} AS DATE)) = ?
        """,
        [mes],
    ).fetchone()

    (receita,) = con.execute(
        f"""
        SELECT COALESCE(SUM({m_val}), 0)
        FROM mensalidade
        WHERE strftime('%Y-%m', CAST({m_date} AS DATE)) = ?
        """,
        [mes],
    ).fetchone()

    sinistralidade = float(sinistro) / float(receita) if receita else None
    return {
        "competencia": mes,
        "sinistralidade": sinistralidade,
        "sinistro": float(sinistro),
        "receita": float(receita),
    }


@app.get("/kpi/sinistralidade/media", tags=["default"], summary="Sinistralidade Média")
def kpi_sinistralidade_media(janela_meses: int = Query(12, ge=1, le=60)):
    c_date, c_val = find_date_value_cols("conta")
    m_date, m_val = find_date_value_cols("mensalidade")
    # últimos N meses com interseção
    rows = con.execute(
        f"""
        WITH c AS (
            SELECT DISTINCT strftime('%Y-%m', CAST({c_date} AS DATE)) AS mes FROM conta
        ),
        m AS (
            SELECT DISTINCT strftime('%Y-%m', CAST({m_date} AS DATE)) AS mes FROM mensalidade
        )
        SELECT mes FROM c
        INTERSECT
        SELECT mes FROM m
        ORDER BY mes DESC
        LIMIT {janela_meses}
        """
    ).fetchall()
    meses = [r[0] for r in rows]
    if not meses:
        return {"meses": [], "media": None, "detalhe": []}

    detalhe = []
    acum = 0.0
    for mes in meses:
        (sinistro,) = con.execute(
            f"SELECT COALESCE(SUM({c_val}),0) FROM conta WHERE strftime('%Y-%m', CAST({c_date} AS DATE)) = ?",
            [mes],
        ).fetchone()
        (receita,) = con.execute(
            f"SELECT COALESCE(SUM({m_val}),0) FROM mensalidade WHERE strftime('%Y-%m', CAST({m_date} AS DATE)) = ?",
            [mes],
        ).fetchone()
        ratio = float(sinistro) / float(receita) if receita else None
        detalhe.append({"competencia": mes, "sinistro": float(sinistro), "receita": float(receita), "sinistralidade": ratio})
        if ratio is not None:
            acum += ratio
    media = acum / len([d for d in detalhe if d["sinistralidade"] is not None]) if detalhe else None
    return {"meses": meses[::-1], "media": media, "detalhe": detalhe[::-1]}


# -------------------- PRESTADOR TOP / IMPACTO -----------------------
@app.get("/kpi/prestador/top", tags=["default"], summary="Prestador Top")
def kpi_prestador_top(competencia: str = Query(..., description="YYYY-MM"), limite: int = Query(10, ge=1, le=100)):
    mes = parse_competencia(competencia)
    # conta: precisa de prestador + valor + data
    prest_col = find_col("conta", ["id_prestador", "id_prestador_envio", "id_prestador_pagamento"])
    if not prest_col:
        raise HTTPException(status_code=400, detail="Não encontrei coluna de prestador em 'conta'.")

    c_date, c_val = find_date_value_cols("conta")

    # prestador: nome
    p_id = find_col("prestador", ["id_prestador", "cd_prestador"]) or "id_prestador"
    p_nome = find_col("prestador", ["nm_prestador", "ds_prestador", "nm_razao_social", "nome"]) or "nm_prestador"

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

    top = [{"id_prestador": r[0], "nome": r[1], "score": float(r[2])} for r in rows]
    return {"competencia": mes, "top": top}


@app.get("/kpi/prestador/impacto", tags=["default"], summary="Prestador Top (impacto)")
def kpi_prestador_impacto(competencia: str = Query(..., description="YYYY-MM"), top: int = Query(10, ge=1, le=100)):
    # Alias do /kpi/prestador/top — mantido por compatibilidade
    return kpi_prestador_top(competencia, top)  # type: ignore


# -------------------- FAIXA x CUSTO (opcional) ----------------------
@app.get("/kpi/faixa/custo", tags=["default"], summary="Custo Por Faixa")
def kpi_faixa_custo(competencia: str = Query(..., description="YYYY-MM")):
    mes = parse_competencia(competencia)
    # usa conta + beneficiario
    c_date, c_val = find_date_value_cols("conta")
    id_ben_conta = find_col("conta", ["id_beneficiario", "id_benef", "cd_beneficiario"])
    nasc = find_col("beneficiario", ["dt_nascimento", "nascimento", "dt_nasc"])
    id_ben = find_col("beneficiario", ["id_beneficiario", "id_benef", "cd_beneficiario"])
    if not (id_ben_conta and nasc and id_ben):
        raise HTTPException(status_code=400, detail="Não encontrei colunas necessárias para faixa-etária.")

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


# -------------------- UTILIZAÇÃO: RESUMO & EVOLUÇÃO ------------------
@app.get("/kpi/utilizacao/resumo", tags=["default"], summary="Resumo de utilização na competência")
def kpi_utilizacao_resumo(
    competencia: str = Query(..., description="YYYY-MM"),
    produto: Optional[str] = Query(None, description="Código ou nome"),
    uf: Optional[str] = None,
    cidade: Optional[str] = None,
    sexo: Optional[str] = Query(None, description="M/F ou equivalente"),
    faixa: Optional[str] = Query(None, description="faixas: 0-18, 19-59, 60+"),
):
    mes = parse_competencia(competencia)

    base = count_beneficiarios_base(sexo=sexo, uf=uf, cidade=cidade, faixa=faixa, competencia_ref=mes)
    utilizados, autoriz = count_utilizados_e_autorizacoes(
        competencia=mes, produto=produto, uf=uf, cidade=cidade, sexo=sexo, faixa=faixa
    )

    filtros: Dict[str, str] = {}
    if produto:
        filtros["produto"] = produto
    if uf:
        filtros["uf"] = uf
    if cidade:
        filtros["cidade"] = cidade
    if sexo:
        filtros["sexo"] = sexo
    if faixa:
        filtros["faixa"] = faixa

    return {
        "competencia": mes,
        "beneficiarios_base": base,
        "beneficiarios_utilizados": utilizados,
        "autorizacoes": autoriz,
        "filtros_aplicados": filtros,
    }


@app.get("/kpi/utilizacao/evolucao", tags=["default"], summary="Evolução mensal da utilização")
def kpi_utilizacao_evolucao(meses: int = Query(12, ge=1, le=60)):
    aut_tbl = "autorizacao"
    dt_aut = find_col(aut_tbl, ["dt_autorizacao", "dt_entrada", "dt_data"])
    id_ben_aut = find_col(aut_tbl, ["id_beneficiario", "id_benef", "cd_beneficiario"])
    if not (dt_aut and id_ben_aut):
        raise HTTPException(status_code=400, detail="Colunas necessárias não encontradas em 'autorizacao'.")

    rows = con.execute(
        f"""
        WITH m AS (
            SELECT strftime('%Y-%m', CAST({dt_aut} AS DATE)) AS mes FROM {aut_tbl}
            GROUP BY 1
            ORDER BY 1 DESC
            LIMIT {meses}
        )
        SELECT
            m.mes,
            (SELECT COUNT(DISTINCT a.{id_ben_aut}) FROM {aut_tbl} a WHERE strftime('%Y-%m', CAST(a.{dt_aut} AS DATE)) = m.mes) AS utilizados,
            (SELECT COUNT(*) FROM {aut_tbl} a WHERE strftime('%Y-%m', CAST(a.{dt_aut} AS DATE)) = m.mes) AS autorizacoes
        FROM m
        ORDER BY m.mes
        """
    ).fetchall()

    return [
        {"competencia": r[0], "beneficiarios_utilizados": int(r[1]), "autorizacoes": int(r[2])}
        for r in rows
    ]


# ---------------------------------------------------------------------
# Raiz (opcional)
# ---------------------------------------------------------------------
@app.get("/", include_in_schema=False)
def root():
    return {"msg": f"{APP_TITLE} — veja /docs"}
