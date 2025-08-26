# backend/app.py
from __future__ import annotations
from fastapi import FastAPI, Query, HTTPException
from typing import Optional, Tuple, List, Dict, Any
from datetime import date
from pathlib import Path
import duckdb

APP_TITLE = "Operadora KPIs"
APP_VERSION = "0.2.0"

# ---------------------------------------------------------------------
# Conexão DuckDB (somente leitura)
# ---------------------------------------------------------------------
DB_PATH = Path(__file__).parent / "data" / "operadora.duckdb"
if not DB_PATH.exists():
    raise RuntimeError(f"Base DuckDB não encontrada em {DB_PATH}")
con = duckdb.connect(str(DB_PATH), read_only=True)

app = FastAPI(title=APP_TITLE, version=APP_VERSION)

# ---------------------------------------------------------------------
# Utilitários
# ---------------------------------------------------------------------
def month_bounds(competencia: str) -> Tuple[date, date]:
    """Converte 'YYYY-MM' -> (primeiro_dia, primeiro_dia_mes_seguinte)."""
    try:
        y, m = competencia.split("-")
        y, m = int(y), int(m)
        first = date(y, m, 1)
        nxt = date(y + 1, 1, 1) if m == 12 else date(y, m + 1, 1)
        return first, nxt
    except Exception:
        raise HTTPException(status_code=422, detail="competencia deve estar no formato YYYY-MM")


def table_exists(table: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM information_schema.tables WHERE lower(table_name)=lower(?) LIMIT 1",
        [table],
    ).fetchone()
    return row is not None


def get_cols(table: str) -> List[str]:
    """Lista colunas da tabela (minúsculas). Se a tabela não existir, retorna []."""
    if not table_exists(table):
        return []
    rows = con.execute(f"PRAGMA table_info('{table}')").fetchall()
    # (cid, name, type, notnull, dflt_value, pk)
    return [r[1].lower() for r in rows]


def find_col(table: str, candidates: List[str]) -> Optional[str]:
    cols = set(get_cols(table))
    for c in candidates:
        if c in cols:
            return c
    return None


def active_where_for_beneficiario() -> Tuple[str, str]:
    """
    Retorna (where_sql, origem) para filtrar BENEFICIÁRIOS ATIVOS.
    Detecta automaticamente a coluna.
    """
    col_str = find_col(
        "beneficiario",
        ["ds_situacao", "situacao", "st_situacao", "status", "cd_situacao", "ds_status"],
    )
    if col_str:
        return f"upper({col_str}) in ('ATIVO','ATV','AT','A')", f"string:{col_str}"

    col_flag = find_col(
        "beneficiario",
        ["fl_ativo", "in_ativo", "ic_ativo", "fg_ativo", "ativo"],
    )
    if col_flag:
        where = (
            f"({col_flag} in (1, TRUE) "
            f"or upper(cast({col_flag} as varchar)) in ('S','SIM','TRUE','T','1'))"
        )
        return where, f"flag:{col_flag}"

    return "1=1", "fallback_sem_coluna_de_situacao"


def pick_usage_source() -> Tuple[str, List[str], List[str]]:
    """
    Decide a tabela-fonte de utilização:
    - Se existir 'autorizacao', usa ela (preferível).
    - Senão, usa 'conta'.
    Retorna (table, candidates_data_cols, candidates_benef_cols).
    """
    if table_exists("autorizacao"):
        return (
            "autorizacao",
            ["dt_autorizacao", "data_autorizacao", "dt_solicitacao", "data_solicitacao", "dt_atendimento", "data_atendimento"],
            ["id_beneficiario", "cd_beneficiario", "nr_beneficiario", "id_matricula", "nr_matricula", "matricula"],
        )
    # fallback para CONTA
    return (
        "conta",
        ["dt_atendimento", "data_atendimento", "dt_emissao", "dt_competencia", "data_competencia"],
        [
            "id_beneficiario", "cd_beneficiario", "nr_beneficiario",
            "id_matricula", "nr_matricula", "matricula",
            "id_beneficiario_pagamento", "beneficiario"
        ],
    )


def faixa_case_expr(alias: str = "b") -> str:
    """Expressão SQL para faixa etária 0-18 / 19-59 / 60+."""
    if "dt_nascimento" not in get_cols("beneficiario"):
        return ""  # sem coluna
    return (
        f"CASE "
        f"WHEN timestampdiff('year', {alias}.dt_nascimento::timestamp, current_timestamp) <= 18 THEN '0-18' "
        f"WHEN timestampdiff('year', {alias}.dt_nascimento::timestamp, current_timestamp) < 60 THEN '19-59' "
        f"ELSE '60+' "
        f"END"
    )


def build_beneficiary_filters(
    produto: Optional[str],
    uf: Optional[str],
    cidade: Optional[str],
    sexo: Optional[str],
    faixa: Optional[str],
    alias: str = "b",
) -> Tuple[str, List[Any], Dict[str, Any]]:
    """
    Monta WHERE adicional para filtrar beneficiários por produto/uf/cidade/sexo/faixa.
    Retorna (sql, params, meta_colunas_usadas).
    """
    clauses: List[str] = []
    params: List[Any] = []
    meta: Dict[str, Any] = {}

    # produto
    col_prod = find_col("beneficiario", ["id_produto", "cd_produto", "nm_produto", "produto", "id_plano", "cd_plano", "nm_plano"])
    if produto:
        if not col_prod:
            raise HTTPException(status_code=500, detail="Filtro 'produto' solicitado mas não encontrei coluna equivalente em 'beneficiario'")
        clauses.append(f"upper({alias}.{col_prod}) = upper(?)")
        params.append(produto)
        meta["col_produto"] = col_prod

    # uf
    col_uf = find_col("beneficiario", ["uf", "sg_uf", "ds_uf", "estado", "cd_uf"])
    if uf:
        if not col_uf:
            raise HTTPException(status_code=500, detail="Filtro 'uf' solicitado mas não encontrei coluna equivalente em 'beneficiario'")
        clauses.append(f"upper({alias}.{col_uf}) = upper(?)")
        params.append(uf)
        meta["col_uf"] = col_uf

    # cidade
    col_cidade = find_col("beneficiario", ["cidade", "nm_cidade", "ds_cidade", "municipio", "nm_municipio", "cd_municipio"])
    if cidade:
        if not col_cidade:
            raise HTTPException(status_code=500, detail="Filtro 'cidade' solicitado mas não encontrei coluna equivalente em 'beneficiario'")
        clauses.append(f"upper({alias}.{col_cidade}) = upper(?)")
        params.append(cidade)
        meta["col_cidade"] = col_cidade

    # sexo
    col_sexo = find_col("beneficiario", ["sexo", "ds_sexo", "cd_sexo", "genero", "ds_genero"])
    if sexo:
        if not col_sexo:
            raise HTTPException(status_code=500, detail="Filtro 'sexo' solicitado mas não encontrei coluna equivalente em 'beneficiario'")
        clauses.append(f"upper({alias}.{col_sexo}) = upper(?)")
        params.append(sexo)
        meta["col_sexo"] = col_sexo

    # faixa (requer dt_nascimento)
    if faixa:
        expr = faixa_case_expr(alias)
        if not expr:
            raise HTTPException(status_code=500, detail="Filtro 'faixa' solicitado mas 'dt_nascimento' não existe em 'beneficiario'")
        clauses.append(f"{expr} = ?")
        params.append(faixa)
        meta["faixa_expr"] = "CASE(dt_nascimento)"

    sql = (" AND " + " AND ".join(clauses)) if clauses else ""
    return sql, params, meta


# ---------------------------------------------------------------------
# Endpoints básicos já existentes
# ---------------------------------------------------------------------
@app.get("/health", summary="Health")
def health():
    return {"ok": True}


@app.get("/kpi/sinistralidade/ultima", summary="Sinistralidade Ultima")
def kpi_sinistralidade_ultima():
    row = con.execute(
        """
        WITH ult AS (
            SELECT competencia
            FROM conta
            GROUP BY competencia
            ORDER BY competencia DESC
            LIMIT 1
        )
        SELECT
            ult.competencia,
            SUM(c.valor_aprovado) AS custo,
            COALESCE((
                SELECT SUM(m.valor_faturado)
                FROM mensalidade m
                WHERE m.competencia = ult.competencia
            ), 0) AS receita
        FROM ult
        JOIN conta c ON c.competencia = ult.competencia
        """
    ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Sem dados em conta/mensalidade")

    competencia, custo, receita = row
    sinistralidade = float(custo) / float(receita) if receita and float(receita) != 0 else None
    return {
        "competencia": competencia,
        "custo": float(custo or 0),
        "receita": float(receita or 0),
        "sinistralidade": sinistralidade,
    }


@app.get("/kpi/sinistralidade/media", summary="Sinistralidade Media")
def kpi_sinistralidade_media(meses: int = Query(6, ge=1, le=36)):
    rows = con.execute(
        """
        WITH meses_ord AS (
            SELECT competencia
            FROM conta
            GROUP BY competencia
            ORDER BY competencia DESC
            LIMIT ?
        ),
        base AS (
            SELECT
                mo.competencia,
                SUM(c.valor_aprovado) AS custo,
                COALESCE((
                    SELECT SUM(m.valor_faturado)
                    FROM mensalidade m
                    WHERE m.competencia = mo.competencia
                ), 0) AS receita
            FROM meses_ord mo
            JOIN conta c ON c.competencia = mo.competencia
            GROUP BY mo.competencia
        )
        SELECT AVG(CASE WHEN receita <> 0 THEN custo/receita ELSE NULL END)
        FROM base
        """,
        [meses],
    ).fetchone()
    media = float(rows[0]) if rows and rows[0] is not None else None
    return {"meses": meses, "media_sinistralidade": media}


@app.get("/kpi/prestador/top", summary="Prestador Top")
def kpi_prestador_top(competencia: str = Query(..., description="YYYY-MM"), limite: int = Query(5, ge=1, le=50)):
    dt_ini, dt_fim = month_bounds(competencia)
    col_data_conta = find_col("conta", ["dt_atendimento", "data_atendimento", "dt_emissao", "dt_competencia"])
    if not col_data_conta:
        raise HTTPException(status_code=500, detail="Não encontrei coluna de data em 'conta'")

    rows = con.execute(
        f"""
        SELECT
            c.id_prestador_pagamento AS id_prestador,
            SUM(c.valor_aprovado)     AS custo
        FROM conta c
        WHERE {col_data_conta} >= ? AND {col_data_conta} < ?
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT ?
        """,
        [dt_ini, dt_fim, limite],
    ).fetchall()
    return [{"id_prestador": r[0], "custo": float(r[1] or 0)} for r in rows]


@app.get("/kpi/faixa/custo", summary="Custo Por Faixa")
def kpi_custo_por_faixa():
    if "dt_nascimento" not in get_cols("beneficiario"):
        raise HTTPException(status_code=500, detail="Coluna 'dt_nascimento' não encontrada em beneficiario")

    # Placeholder simples (ajustaremos quando cruzarmos com 'conta')
    rows = con.execute(
        """
        WITH faixa AS (
            SELECT
                CASE
                    WHEN timestampdiff('year', dt_nascimento::timestamp, current_timestamp) <= 18 THEN '0-18'
                    WHEN timestampdiff('year', dt_nascimento::timestamp, current_timestamp) < 60 THEN '19-59'
                    ELSE '60+'
                END AS faixa
            FROM beneficiario
        )
        SELECT faixa, COUNT(*) AS qtd
        FROM faixa
        GROUP BY 1
        ORDER BY 1
        """
    ).fetchall()

    return [{"faixa": r[0], "qtd_beneficiarios": int(r[1] or 0)} for r in rows]

# ---------------------------------------------------------------------
# Utilização — com filtros e fallback para CONTA se AUTORIZACAO não existir
# ---------------------------------------------------------------------
@app.get("/kpi/utilizacao/resumo", summary="Resumo de utilização na competência")
def kpi_utilizacao_resumo(
    competencia: str = Query(..., description="YYYY-MM"),
    produto: Optional[str] = Query(None, description="Código ou nome do produto"),
    uf: Optional[str] = Query(None, description="UF, ex.: SP"),
    cidade: Optional[str] = Query(None, description="Cidade, ex.: São Paulo"),
    sexo: Optional[str] = Query(None, description="M/F ou equivalente"),
    faixa: Optional[str] = Query(None, description="faixas: 0-18, 19-59, 60+"),
):
    dt_ini, dt_fim = month_bounds(competencia)

    # 1) Critério de ATIVOS
    where_ativos, origem_ativo = active_where_for_beneficiario()

    # 2) Filtros opcionais (beneficiario)
    filt_sql, filt_params, filt_meta = build_beneficiary_filters(produto, uf, cidade, sexo, faixa, alias="b")

    # 3) Fonte de utilização
    src_table, date_candidates, ben_candidates = pick_usage_source()
    col_dt = find_col(src_table, date_candidates)
    if not col_dt:
        raise HTTPException(status_code=500, detail=f"Não encontrei coluna de data em '{src_table}'")

    col_ben_src = find_col(src_table, ben_candidates)
    col_ben_ben = find_col("beneficiario", ["id_beneficiario", "cd_beneficiario", "nr_beneficiario", "id_matricula", "nr_matricula", "matricula"])
    if not col_ben_src or not col_ben_ben:
        raise HTTPException(
            status_code=500,
            detail=f"Não encontrei chave de beneficiário para relacionar {src_table}↔beneficiario",
        )

    # 4) Contar ATIVOS (com filtros)
    (ativos_total,) = con.execute(
        f"""
        SELECT COUNT(*)
        FROM beneficiario b
        WHERE {where_ativos} {filt_sql}
        """,
        filt_params,
    ).fetchone()

    # 5) Contar UTILIZARAM (com filtros)
    params = [dt_ini, dt_fim] + filt_params
    (utilizaram_total,) = con.execute(
        f"""
        WITH a AS (
            SELECT DISTINCT {col_ben_src} AS ben
            FROM {src_table}
            WHERE {col_dt} >= ? AND {col_dt} < ?
        )
        SELECT COUNT(*)
        FROM a
        JOIN beneficiario b ON b.{col_ben_ben} = a.ben
        WHERE {where_ativos} {filt_sql}
        """,
        params,
    ).fetchone()

    perc = (float(utilizaram_total) / float(ativos_total)) if ativos_total else None
    return {
        "fonte_utilizacao": src_table,
        "competencia": competencia,
        "beneficiarios_ativos": int(ativos_total or 0),
        "beneficiarios_utilizaram": int(utilizaram_total or 0),
        "percentual_utilizacao": perc,
        "origem_criterio_ativo": origem_ativo,
        "origem_filtros": filt_meta,
    }


@app.get("/kpi/utilizacao/evolucao", summary="Evolução mensal da utilização")
def kpi_utilizacao_evolucao(
    desde: str = Query(..., description="YYYY-MM"),
    ate:   str = Query(..., description="YYYY-MM"),
    produto: Optional[str] = Query(None),
    uf: Optional[str] = Query(None),
    cidade: Optional[str] = Query(None),
    sexo: Optional[str] = Query(None),
    faixa: Optional[str] = Query(None, description="0-18, 19-59, 60+"),
):
    dt_ini, _ = month_bounds(desde)
    _, dt_fim = month_bounds(ate)

    src_table, date_candidates, ben_candidates = pick_usage_source()
    col_dt = find_col(src_table, date_candidates)
    if not col_dt:
        raise HTTPException(status_code=500, detail=f"Não encontrei coluna de data em '{src_table}'")

    col_ben_src = find_col(src_table, ben_candidates)
    col_ben_ben = find_col("beneficiario", ["id_beneficiario", "cd_beneficiario", "nr_beneficiario", "id_matricula", "nr_matricula", "matricula"])
    if not col_ben_src or not col_ben_ben:
        raise HTTPException(
            status_code=500,
            detail=f"Não encontrei chave de beneficiário para relacionar {src_table}↔beneficiario",
        )

    where_ativos, origem_ativo = active_where_for_beneficiario()
    filt_sql, filt_params, filt_meta = build_beneficiary_filters(produto, uf, cidade, sexo, faixa, alias="b")

    rows = con.execute(
        f"""
        WITH meses AS (
            SELECT strftime({col_dt}, '%Y-%m') AS competencia
            FROM {src_table}
            WHERE {col_dt} >= ? AND {col_dt} < ?
            GROUP BY 1
            ORDER BY 1
        ),
        ativos AS (
            SELECT COUNT(*) AS qtd
            FROM beneficiario b
            WHERE {where_ativos} {filt_sql}
        )
        SELECT
            m.competencia,
            (SELECT qtd FROM ativos) AS ativos,
            (
                SELECT COUNT(*)
                FROM (
                    SELECT DISTINCT a.{col_ben_src} AS ben
                    FROM {src_table} a
                    WHERE strftime(a.{col_dt}, '%Y-%m') = m.competencia
                ) x
                JOIN beneficiario b ON b.{col_ben_ben} = x.ben
                WHERE {where_ativos} {filt_sql}
            ) AS utilizaram
        FROM meses m
        ORDER BY m.competencia
        """,
        [dt_ini, dt_fim] + filt_params,
    ).fetchall()

    out = []
    for comp, ativos, util in rows:
        out.append(
            {
                "fonte_utilizacao": src_table,
                "competencia": comp,
                "beneficiarios_ativos": int(ativos or 0),
                "beneficiarios_utilizaram": int(util or 0),
                "percentual_utilizacao": (float(util) / float(ativos)) if ativos else None,
                "origem_criterio_ativo": origem_ativo,
                "origem_filtros": filt_meta,
            }
        )
    return out

# ---------------------------------------------------------------------
# Auditoria simples: schema detectado
# ---------------------------------------------------------------------
@app.get("/meta/schema", summary="Lista tabelas e colunas disponíveis (auditoria)")
def meta_schema():
    tables = con.execute(
        "SELECT table_name FROM information_schema.tables ORDER BY 1"
    ).fetchall()
    out = []
    for (tname,) in tables:
        out.append({"tabela": tname, "colunas": get_cols(tname)})
    return out
