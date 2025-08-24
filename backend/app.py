from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
import duckdb
import pandas as pd
from datetime import datetime, date
import pathlib

app = FastAPI(title="Operadora KPIs")

# Caminho absoluto para o banco DuckDB (funciona em Windows/macOS/Linux)
DB_PATH = pathlib.Path(__file__).parent / "data" / "operadora.duckdb"
con = duckdb.connect(str(DB_PATH), read_only=True)

@app.get("/health")
def health():
    try:
        con.execute("SELECT 1").fetchone()
        return {"status": "ok", "db_path": str(DB_PATH)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/kpi/sinistralidade/ultima")
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
    # competencia é DATE no DuckDB
    return {"competencia": competencia.strftime("%Y-%m"), "sinistralidade": float(sinist) if sinist is not None else None}

@app.get("/kpi/sinistralidade/media")
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

@app.get("/kpi/prestador/top")
def prestador_top(competencia: str = Query(..., description="YYYY-MM")):
    # Converte para o primeiro dia do mês
    try:
        dt = datetime.strptime(competencia + "-01", "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(400, "Formato de competência inválido (use YYYY-MM)")

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
        "competencia": competencia,
        "id_prestador": int(id_prest) if id_prest is not None else None,
        "nm_prestador": nome,
        "custo": float(custo) if custo is not None else None,
    }

@app.get("/kpi/faixa/custo")
def custo_por_faixa(competencia: str = Query(..., description="YYYY-MM")):
    try:
        dt = datetime.strptime(competencia + "-01", "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(400, "Formato de competência inválido (use YYYY-MM)")

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
    return {"competencia": competencia, "faixas": rows.to_dict(orient="records")}
