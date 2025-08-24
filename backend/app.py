# backend/app.py
from __future__ import annotations
from fastapi import FastAPI, Query, HTTPException
from typing import Optional, Tuple, List
from datetime import date
from pathlib import Path
import duckdb

APP_TITLE = "Operadora KPIs"
APP_VERSION = "0.1.0"

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
    """
    Converte 'YYYY-MM' -> (primeiro_dia, primeiro_dia_mes_seguinte)
    """
    try:
        y, m = competencia.split("-")
        y, m = int(y), int(m)
        first = date(y, m, 1)
        if m == 12:
            nxt = date(y + 1, 1, 1)
        else:
            nxt = date(y, m + 1, 1)
        return first, nxt
    except Exception:
        raise HTTPException(status_code=422, detail="competencia deve estar no formato YYYY-MM")


def get_cols(table: str) -> List[str]:
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
    Tenta detectar automaticamente a coluna certa.
    """
    # strings de situação
    col_str = find_col(
        "beneficiario",
        [
            "ds_situacao",
            "situacao",
            "st_situacao",
            "status",
            "cd_situacao",
            "ds_status",
        ],
    )
    if col_str:
        where = f"upper({col_str}) in ('ATIVO','ATV','AT','A')"
        return where, f"string:{col_str}"

    # flags booleanas/numéricas
    col_flag = find_col(
        "beneficiario",
        [
            "fl_ativo",
            "in_ativo",
            "ic_ativo",
            "fg_ativo",
            "ativo",
        ],
    )
    if col_flag:
        # aceita várias representações de verdadeiro
        where = (
            f"({col_flag} in (1, TRUE) "
            f"or upper(cast({col_flag} as varchar)) in ('S','SIM','TRUE','T','1'))"
        )
        return where, f"flag:{col_flag}"

    # fallback: sem filtro (conta todo mundo) — mas deixa claro na resposta
    return "1=1", "fallback_sem_coluna_de_situacao"


# ---------------------------------------------------------------------
# Endpoints já existentes (mantidos)
# ---------------------------------------------------------------------
@app.get("/health", summary="Health")
def health():
    return {"ok": True}


@app.get("/kpi/sinistralidade/ultima", summary="Sinistralidade Ultima")
def kpi_sinistralidade_ultima():
    """
    Exemplo ilustrativo: busca a competência mais recente em CONTA e calcula custo/receita.
    """
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
    """
    Média simples de sinistralidade dos últimos N meses existentes em CONTA/MENSALIDADE.
    """
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
        SELECT
            AVG(CASE WHEN receita <> 0 THEN custo/receita ELSE NULL END)
        FROM base
        """,
        [meses],
    ).fetchone()
    media = float(rows[0]) if rows and rows[0] is not None else None
    return {"meses": meses, "media_sinistralidade": media}


@app.get("/kpi/prestador/top", summary="Prestador Top")
def kpi_prestador_top(competencia: str = Query(..., description="YYYY-MM"), limite: int = Query(5, ge=1, le=50)):
    dt_ini, dt_fim = month_bounds(competencia)
    # tenta achar coluna de data em conta
    col_data_conta = find_col("conta", ["dt_atendimento", "data_atendimento", "dt_atend", "dt_emissao", "dt_competencia"])
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
    """
    Exemplo: custo por faixa etária (0-18,19-59,60+).
    Depende de beneficiario.dt_nascimento estar presente.
    """
    if "dt_nascimento" not in get_cols("beneficiario"):
        raise HTTPException(status_code=500, detail="Coluna 'dt_nascimento' não encontrada em beneficiario")

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
        ),
        custo AS (
            SELECT '0-18' AS faixa, 0.0::DOUBLE AS custo UNION ALL
            SELECT '19-59', 0.0::DOUBLE UNION ALL
            SELECT '60+', 0.0::DOUBLE
        )
        SELECT c.faixa, SUM(0.0) AS custo -- placeholder (sem join real de custo por beneficiário)
        FROM custo c
        GROUP BY 1
        ORDER BY 1
        """
    ).fetchall()

    return [{"faixa": r[0], "custo": float(r[1] or 0)} for r in rows]


# ---------------------------------------------------------------------
# Novos: Utilização (corrigido para detectar coluna de ativo)
# ---------------------------------------------------------------------
@app.get("/kpi/utilizacao/resumo", summary="Resumo de utilização na competência")
def kpi_utilizacao_resumo(competencia: str = Query(..., description="YYYY-MM")):
    dt_ini, dt_fim = month_bounds(competencia)

    # 1) Beneficiários ativos
    where_ativos, origem = active_where_for_beneficiario()
    (ativos_total,) = con.execute(
        f"SELECT COUNT(*) FROM beneficiario WHERE {where_ativos}"
    ).fetchone()

    # 2) Quem usou (distinct beneficiário) em AUTORIZAÇÃO na competência
    col_dt_aut = find_col(
        "autorizacao",
        ["dt_autorizacao", "data_autorizacao", "dt_solicitacao", "data_solicitacao", "dt_atendimento", "data_atendimento"],
    )
    if not col_dt_aut:
        raise HTTPException(status_code=500, detail="Não encontrei coluna de data em 'autorizacao'")

    col_ben_aut = find_col(
        "autorizacao",
        ["id_beneficiario", "cd_beneficiario", "nr_beneficiario", "id_matricula", "nr_matricula", "matricula"],
    )
    col_ben_ben = find_col(
        "beneficiario",
        ["id_beneficiario", "cd_beneficiario", "nr_beneficiario", "id_matricula", "nr_matricula", "matricula"],
    )
    if not col_ben_aut or not col_ben_ben:
        raise HTTPException(status_code=500, detail="Não encontrei chave de beneficiário para relacionar autorizacao↔beneficiario")

    (utilizaram_total,) = con.execute(
        f"""
        WITH a AS (
            SELECT DISTINCT {col_ben_aut} AS ben
            FROM autorizacao
            WHERE {col_dt_aut} >= ? AND {col_dt_aut} < ?
        )
        SELECT COUNT(*)
        FROM a
        JOIN beneficiario b ON b.{col_ben_ben} = a.ben
        WHERE {where_ativos}
        """,
        [dt_ini, dt_fim],
    ).fetchone()

    perc_utilizacao = (float(utilizaram_total) / float(ativos_total)) if ativos_total else None

    return {
        "competencia": competencia,
        "origem_criterio_ativo": origem,  # para auditoria
        "beneficiarios_ativos": int(ativos_total or 0),
        "beneficiarios_utilizaram": int(utilizaram_total or 0),
        "percentual_utilizacao": perc_utilizacao,
    }


@app.get("/kpi/utilizacao/evolucao", summary="Evolução mensal da utilização")
def kpi_utilizacao_evolucao(
    desde: str = Query(..., description="YYYY-MM"),
    ate:   str = Query(..., description="YYYY-MM"),
):
    dt_ini, _ = month_bounds(desde)
    _, dt_fim = month_bounds(ate)

    # lista de competências entre dt_ini e dt_fim presentes em autorizacao
    col_dt_aut = find_col(
        "autorizacao",
        ["dt_autorizacao", "data_autorizacao", "dt_solicitacao", "data_solicitacao", "dt_atendimento", "data_atendimento"],
    )
    if not col_dt_aut:
        raise HTTPException(status_code=500, detail="Não encontrei coluna de data em 'autorizacao'")

    col_ben_aut = find_col(
        "autorizacao",
        ["id_beneficiario", "cd_beneficiario", "nr_beneficiario", "id_matricula", "nr_matricula", "matricula"],
    )
    col_ben_ben = find_col(
        "beneficiario",
        ["id_beneficiario", "cd_beneficiario", "nr_beneficiario", "id_matricula", "nr_matricula", "matricula"],
    )
    if not col_ben_aut or not col_ben_ben:
        raise HTTPException(status_code=500, detail="Não encontrei chave de beneficiário para relacionar autorizacao↔beneficiario")

    where_ativos, origem = active_where_for_beneficiario()

    rows = con.execute(
        f"""
        WITH meses AS (
            SELECT
                strftime({col_dt_aut}, '%Y-%m') AS competencia
            FROM autorizacao
            WHERE {col_dt_aut} >= ? AND {col_dt_aut} < ?
            GROUP BY 1
            ORDER BY 1
        ),
        ativos AS (
            SELECT COUNT(*) AS qtd FROM beneficiario WHERE {where_ativos}
        )
        SELECT
            m.competencia,
            (SELECT qtd FROM ativos) AS ativos,
            (
                SELECT COUNT(*)
                FROM (
                    SELECT DISTINCT a.{col_ben_aut} AS ben
                    FROM autorizacao a
                    WHERE strftime(a.{col_dt_aut}, '%Y-%m') = m.competencia
                ) x
                JOIN beneficiario b ON b.{col_ben_ben} = x.ben
                WHERE {where_ativos}
            ) AS utilizaram
        FROM meses m
        ORDER BY m.competencia
        """,
        [dt_ini, dt_fim],
    ).fetchall()

    out = []
    for comp, ativos, util in rows:
        out.append(
            {
                "competencia": comp,
                "beneficiarios_ativos": int(ativos or 0),
                "beneficiarios_utilizaram": int(util or 0),
                "percentual_utilizacao": (float(util) / float(ativos)) if ativos else None,
                "origem_criterio_ativo": origem,
            }
        )
    return out
