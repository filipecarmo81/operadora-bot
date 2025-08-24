from fastapi import FastAPI, HTTPException, Query
import duckdb
import pandas as pd
from datetime import datetime, date
from typing import Optional, List, Dict
import pathlib

app = FastAPI(title="Operadora KPIs")

# Caminho absoluto para o DuckDB (robusto para Windows/macOS/Linux)
DB_PATH = pathlib.Path(__file__).parent / "data" / "operadora.duckdb"
con = duckdb.connect(str(DB_PATH), read_only=True)

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _ultima_competencia_str() -> Optional[str]:
    """Retorna a última competência existente na tabela conta no formato YYYY-MM."""
    row = con.execute("SELECT strftime(max(dt_mes_competencia), '%Y-%m') FROM conta").fetchone()
    return row[0] if row and row[0] else None

def _min_competencia_str() -> Optional[str]:
    """Retorna a menor competência existente na tabela conta no formato YYYY-MM."""
    row = con.execute("SELECT strftime(min(dt_mes_competencia), '%Y-%m') FROM conta").fetchone()
    return row[0] if row and row[0] else None

def _parse_competencia(competencia: Optional[str]) -> str:
    """Valida (YYYY-MM) ou substitui pela última competência disponível."""
    if competencia:
        try:
            datetime.strptime(competencia + "-01", "%Y-%m-%d")
        except ValueError:
            raise HTTPException(400, "Formato de competência inválido (use YYYY-MM)")
        return competencia
    # pega a última existente
    ultima = _ultima_competencia_str()
    if not ultima:
        raise HTTPException(404, "Sem dados de competência")
    return ultima

def _primeiro_dia(competencia: str) -> date:
    return datetime.strptime(competencia + "-01", "%Y-%m-%d").date()

# ---------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------

@app.get("/health", tags=["Infra"])
def health():
    try:
        con.execute("SELECT 1").fetchone()
        return {"status": "ok", "db_path": str(DB_PATH)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ---------------------------------------------------------------------
# Sinistralidade
# ---------------------------------------------------------------------

@app.get("/kpi/sinistralidade/ultima", tags=["Sinistralidade"])
def sinistralidade_ultima():
    sql = """
    SELECT competencia, sinistralidade
    FROM kpi_sinistralidade_mensal
    ORDER BY competencia DESC
    LIMIT 1
    """
    row = con.execute(sql).fetchone()
    if not row:
        raise HTTPException(404, "Sem dados")
    competencia, sinist = row
    return {
        "competencia": competencia.strftime("%Y-%m"),
        "sinistralidade": float(sinist) if sinist is not None else None,
    }

@app.get("/kpi/sinistralidade/media", tags=["Sinistralidade"])
def sinistralidade_media(meses: int = Query(6, ge=1, le=36)):
    sql = f"""
    SELECT AVG(sinistralidade)
    FROM (
      SELECT sinistralidade
      FROM kpi_sinistralidade_mensal
      ORDER BY competencia DESC
      LIMIT {meses}
    )
    """
    row = con.execute(sql).fetchone()
    if not row or row[0] is None:
        raise HTTPException(404, "Sem dados")
    return {"meses": meses, "media_sinistralidade": float(row[0])}

# ---------------------------------------------------------------------
# Prestadores & Custos por faixa etária (custo assistencial)
# ---------------------------------------------------------------------

@app.get("/kpi/prestador/top", tags=["Prestadores"])
def prestador_top(competencia: str = Query(..., description="YYYY-MM")):
    comp = _parse_competencia(competencia)
    dt = _primeiro_dia(comp)

    sql = """
    WITH custos AS (
      SELECT id_prestador_pagamento, SUM(vl_liberado) AS custo
      FROM conta
      WHERE dt_mes_competencia = ?
      GROUP BY 1
    )
    SELECT p.id_prestador, p.nm_prestador, c.custo
    FROM custos c
    LEFT JOIN prestador p ON p.id_prestador = c.id_prestador_pagamento
    ORDER BY c.custo DESC
    LIMIT 1
    """
    row = con.execute(sql, [dt]).fetchone()
    if not row:
        raise HTTPException(404, "Sem dados para esta competência")
    id_prest, nome, custo = row
    return {
        "competencia": comp,
        "id_prestador": int(id_prest) if id_prest is not None else None,
        "nm_prestador": nome,
        "custo": float(custo) if custo is not None else None,
    }

@app.get("/kpi/faixa/custo", tags=["Sinistralidade"])
def custo_por_faixa(competencia: str = Query(..., description="YYYY-MM")):
    comp = _parse_competencia(competencia)
    dt = _primeiro_dia(comp)

    sql = """
    WITH contas AS (
      SELECT id_beneficiario, vl_liberado
      FROM conta
      WHERE dt_mes_competencia = ?
    ),
    idades AS (
      SELECT b.id_beneficiario,
             DATE_DIFF('year', b.dt_nascimento, ?)::INT AS idade
      FROM beneficiario b
    ),
    joined AS (
      SELECT c.vl_liberado, 
             CASE 
               WHEN i.idade IS NULL THEN 'Sem idade'
               WHEN i.idade <= 18 THEN '0-18'
               WHEN i.idade <= 59 THEN '19-59'
               ELSE '60+'
             END AS faixa
      FROM contas c
      LEFT JOIN idades i USING (id_beneficiario)
    )
    SELECT faixa, SUM(vl_liberado) AS custo
    FROM joined
    GROUP BY faixa
    ORDER BY CASE faixa 
                WHEN '0-18' THEN 1 
                WHEN '19-59' THEN 2 
                WHEN '60+' THEN 3 
                ELSE 4 
             END
    """
    rows = con.execute(sql, [dt, dt]).fetchdf()
    return {"competencia": comp, "faixas": rows.to_dict(orient="records")}

# ---------------------------------------------------------------------
# Utilização (Autorizações/Contas) — KPIs principais
# ---------------------------------------------------------------------

@app.get("/kpi/utilizacao/resumo", tags=["Utilização"])
def kpi_utilizacao_resumo(
    competencia: Optional[str] = Query(None, pattern=r"^\d{4}-\d{2}$", description="YYYY-MM opcional")
):
    comp = _parse_competencia(competencia)

    # total de beneficiários ativos
    (ativos_total,) = con.execute("""
        SELECT COUNT(DISTINCT id_beneficiario)
        FROM beneficiario
        WHERE upper(ds_situacao) = 'ATIVO'
    """).fetchone()

    # ativos que usaram no mês (tem conta no mês)
    (ativos_usaram,) = con.execute("""
        SELECT COUNT(DISTINCT c.id_beneficiario)
        FROM conta c
        JOIN beneficiario b USING(id_beneficiario)
        WHERE upper(b.ds_situacao) = 'ATIVO'
          AND strftime(c.dt_mes_competencia, '%Y-%m') = ?
    """, [comp]).fetchone()

    # consultas (N2 == CONSULTAS)
    (consultas_total,) = con.execute("""
        SELECT COUNT(*)
        FROM conta
        WHERE strftime(dt_mes_competencia, '%Y-%m') = ?
          AND upper(ds_classificacao_item_n2) = 'CONSULTAS'
    """, [comp]).fetchone()

    # internações (qualquer valor em ds_tipo_internacao)
    (internacoes,) = con.execute("""
        SELECT COUNT(DISTINCT id_conta)
        FROM conta
        WHERE strftime(dt_mes_competencia, '%Y-%m') = ?
          AND ds_tipo_internacao IS NOT NULL
    """, [comp]).fetchone()

    # exames laboratoriais
    (exames_lab,) = con.execute("""
        SELECT COUNT(*)
        FROM conta
        WHERE strftime(dt_mes_competencia, '%Y-%m') = ?
          AND upper(ds_classificacao_item_n2) = 'MEDICINA LABORATORIAL'
    """, [comp]).fetchone()

    # urgência/emergência (caráter atendimento contém 'URG')
    (urgencias,) = con.execute("""
        SELECT COUNT(DISTINCT id_conta)
        FROM conta
        WHERE strftime(dt_mes_competencia, '%Y-%m') = ?
          AND upper(ds_carater_atendimento) LIKE '%URG%'
    """, [comp]).fetchone()

    # top 5 beneficiários por custo
    top5 = con.execute("""
        SELECT c.id_beneficiario, b.nm_beneficiario, SUM(c.vl_liberado) AS custo
        FROM conta c
        JOIN beneficiario b USING(id_beneficiario)
        WHERE strftime(c.dt_mes_competencia, '%Y-%m') = ?
        GROUP BY 1,2
        ORDER BY custo DESC
        LIMIT 5
    """, [comp]).df().to_dict(orient="records")

    return {
        "competencia": comp,
        "ativos_total": int(ativos_total or 0),
        "ativos_utilizaram": int(ativos_usaram or 0),
        "percentual_utilizacao": (ativos_usaram / ativos_total) if ativos_total else 0.0,
        "media_consultas_por_ativo": (consultas_total / ativos_total) if ativos_total else 0.0,
        "internacoes": int(internacoes or 0),
        "exames_laboratoriais": int(exames_lab or 0),
        "atendimentos_urgencia_emergencia": int(urgencias or 0),
        "top5_beneficiarios_por_custo": top5,
    }

@app.get("/kpi/utilizacao/sexo", tags=["Utilização"])
def kpi_utilizacao_sexo(
    competencia: Optional[str] = Query(None, pattern=r"^\d{4}-\d{2}$", description="YYYY-MM opcional")
):
    comp = _parse_competencia(competencia)

    sql = """
    WITH ativos AS (
      SELECT id_beneficiario,
             CASE 
               WHEN upper(ds_sexo) IN ('M','MASCULINO') THEN 'M'
               WHEN upper(ds_sexo) IN ('F','FEMININO') THEN 'F'
               ELSE 'OUTRO'
             END AS sexo
      FROM beneficiario
      WHERE upper(ds_situacao) = 'ATIVO'
    ),
    usaram AS (
      SELECT DISTINCT id_beneficiario
      FROM conta
      WHERE strftime(dt_mes_competencia, '%Y-%m') = ?
    ),
    base AS (
      SELECT a.sexo,
             COUNT(*) AS ativos_total,
             SUM(CASE WHEN u.id_beneficiario IS NOT NULL THEN 1 ELSE 0 END) AS ativos_usaram
      FROM ativos a
      LEFT JOIN usaram u USING(id_beneficiario)
      GROUP BY a.sexo
    )
    SELECT sexo,
           ativos_total,
           ativos_usaram,
           CASE WHEN ativos_total=0 THEN 0 ELSE CAST(ativos_usaram AS DOUBLE)/ativos_total END AS taxa_utilizacao
    FROM base
    ORDER BY CASE sexo WHEN 'M' THEN 1 WHEN 'F' THEN 2 ELSE 3 END
    """
    rows = con.execute(sql, [comp]).fetchdf()
    return {"competencia": comp, "sexo": rows.to_dict(orient="records")}

@app.get("/kpi/utilizacao/faixa", tags=["Utilização"])
def kpi_utilizacao_faixa(
    competencia: Optional[str] = Query(None, pattern=r"^\d{4}-\d{2}$", description="YYYY-MM opcional")
):
    comp = _parse_competencia(competencia)
    dt = _primeiro_dia(comp)

    sql = """
    WITH ativos AS (
      SELECT b.id_beneficiario,
             DATE_DIFF('year', b.dt_nascimento, ?)::INT AS idade
      FROM beneficiario b
      WHERE upper(b.ds_situacao) = 'ATIVO'
    ),
    faixa AS (
      SELECT id_beneficiario,
             CASE 
               WHEN idade IS NULL THEN 'Sem idade'
               WHEN idade <= 18 THEN '0-18'
               WHEN idade <= 59 THEN '19-59'
               ELSE '60+'
             END AS faixa
      FROM ativos
    ),
    usaram AS (
      SELECT DISTINCT id_beneficiario
      FROM conta
      WHERE strftime(dt_mes_competencia, '%Y-%m') = ?
    ),
    base AS (
      SELECT f.faixa,
             COUNT(*) AS ativos_total,
             SUM(CASE WHEN u.id_beneficiario IS NOT NULL THEN 1 ELSE 0 END) AS ativos_usaram
      FROM faixa f
      LEFT JOIN usaram u USING(id_beneficiario)
      GROUP BY f.faixa
    )
    SELECT faixa,
           ativos_total,
           ativos_usaram,
           CASE WHEN ativos_total=0 THEN 0 ELSE CAST(ativos_usaram AS DOUBLE)/ativos_total END AS taxa_utilizacao
    FROM base
    ORDER BY CASE faixa WHEN '0-18' THEN 1 WHEN '19-59' THEN 2 WHEN '60+' THEN 3 ELSE 4 END
    """
    rows = con.execute(sql, [dt, comp]).fetchdf()
    return {"competencia": comp, "faixas": rows.to_dict(orient="records")}

# ---------------------------------------------------------------------
# Utilização - evolução mês a mês
# ---------------------------------------------------------------------

@app.get("/kpi/utilizacao/evolucao", tags=["Utilização"])
def kpi_utilizacao_evolucao(
    desde: Optional[str] = Query(None, pattern=r"^\d{4}-\d{2}$", description="YYYY-MM"),
    ate:   Optional[str] = Query(None, pattern=r"^\d{4}-\d{2}$", description="YYYY-MM")
):
    # intervalos padrão: da menor à maior competência
    minc = _min_competencia_str()
    maxc = _ultima_competencia_str()
    if not minc or not maxc:
        raise HTTPException(404, "Sem dados de competência")

    d = desde or minc
    a = ate   or maxc

    # validação do formato
    for x in (d, a):
        datetime.strptime(x + "-01", "%Y-%m-%d")

    # total de ativos (snapshot atual)
    (ativos_total,) = con.execute("""
        SELECT COUNT(DISTINCT id_beneficiario)
        FROM beneficiario
        WHERE upper(ds_situacao) = 'ATIVO'
    """).fetchone()
    ativos_total = int(ativos_total or 0)

    sql = """
    WITH base AS (
      SELECT
        strftime(c.dt_mes_competencia, '%Y-%m') AS competencia,
        c.id_conta,
        c.id_beneficiario,
        upper(c.ds_classificacao_item_n2) AS n2,
        c.ds_tipo_internacao,
        c.ds_carater_atendimento
      FROM conta c
      JOIN beneficiario b USING(id_beneficiario)
      WHERE strftime(c.dt_mes_competencia, '%Y-%m') BETWEEN ? AND ?
        AND upper(b.ds_situacao) = 'ATIVO'
    )
    SELECT
      competencia,
      COUNT(DISTINCT id_beneficiario)                              AS ativos_usaram,
      SUM(CASE WHEN n2 = 'CONSULTAS' THEN 1 ELSE 0 END)            AS consultas,
      COUNT(DISTINCT CASE WHEN ds_tipo_internacao IS NOT NULL
                          THEN id_conta END)                        AS internacoes,
      SUM(CASE WHEN n2 = 'MEDICINA LABORATORIAL' THEN 1 ELSE 0 END) AS exames_laboratoriais,
      COUNT(DISTINCT CASE WHEN upper(coalesce(ds_carater_atendimento,'')) LIKE '%URG%'
                          THEN id_conta END)                        AS urgencias
    FROM base
    GROUP BY competencia
    ORDER BY competencia
    """
    df = con.execute(sql, [d, a]).fetchdf()
    if df.empty:
        return {"desde": d, "ate": a, "ativos_total": ativos_total, "serie": []}

    # adiciona percentual de utilização
    if ativos_total > 0:
        df["percentual_utilizacao"] = df["ativos_usaram"] / ativos_total
    else:
        df["percentual_utilizacao"] = 0.0

    return {
        "desde": d,
        "ate": a,
        "ativos_total": ativos_total,
        "serie": df.to_dict(orient="records"),
    }
