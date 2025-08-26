from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime
from pathlib import Path
import duckdb
import pandas as pd

# ======================================================================
# CONFIG
# ======================================================================
DB_PATH = str((Path(__file__).parent / "data" / "operadora.duckdb").resolve())

app = FastAPI(title="Operadora KPIs", version="0.7.0")

# ======================================================================
# CONEXÃO + UTILITÁRIOS
# ======================================================================
def get_con():
    try:
        con = duckdb.connect(DB_PATH, read_only=False)
        return con
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha ao abrir DuckDB: {e}")

def table_exists(con, name: str) -> bool:
    sql = "SELECT COUNT(*) FROM information_schema.tables WHERE lower(table_name)=lower(?)"
    return con.execute(sql, [name]).fetchone()[0] > 0

def get_cols(con, table: str) -> List[str]:
    if not table_exists(con, table):
        raise HTTPException(status_code=400, detail=f"Tabela '{table}' não existe no DuckDB.")
    rows = con.execute(f"PRAGMA table_info('{table}')").fetchall()
    return [r[1] for r in rows]

def find_col(con, table: str, candidates: List[str]) -> Optional[str]:
    cols = set(c.lower() for c in get_cols(con, table))
    for cand in candidates:
        if cand and cand.lower() in cols:
            return cand
    return None

def safe_json(data: Any) -> JSONResponse:
    return JSONResponse(content=data)

# ======================================================================
# EXPRESSÕES ROBUSTAS PARA DATAS E NÚMEROS
# ======================================================================
DATE_CANDIDATES = ["%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y%m%d", "%Y/%m/%d"]

def month_expr(col_sql: str) -> str:
    tries = [f"TRY_STRPTIME({col_sql}, '{fmt}')" for fmt in DATE_CANDIDATES]
    tries.append(f"CAST({col_sql} AS DATE)")
    coalesced = "COALESCE(" + ", ".join(tries) + ")"
    return f"strftime('%Y-%m', {coalesced})"

def numeric_expr(col_sql: str) -> str:
    sanitized = (
        f"REPLACE(REPLACE(REPLACE(REPLACE({col_sql}, 'R$', ''), ' ', ''), '.', ''), ',', '.')"
    )
    return (
        "COALESCE("
        f"TRY_CAST({col_sql} AS DOUBLE), "
        f"TRY_CAST({sanitized} AS DOUBLE), "
        "0)"
    )

# ======================================================================
# CANDIDATOS DE COLUNAS
# ======================================================================
CAND_DATA_CONTA = [
    "dt_competencia","competencia","mes_competencia","dt_emissao",
    "dt_pagamento","dt_apresentacao","data","dt_liberacao"
]
CAND_VALOR_CONTA = [
    "vl_pago","vl_liberado","vl_aprovado","vl_total","vl_custo",
    "valor_pago","valor_liberado","valor_aprovado","valor_total","valor"
]
CAND_DATA_MENS = [
    "dt_competencia","competencia","mes_competencia","dt_emissao",
    "dt_pagamento","data","dt_referencia","dt_vencimento"
]
CAND_VALOR_MENS = [
    "vl_faturado","vl_liquido","vl_receita","vl_total",
    "valor_faturado","valor_liquido","valor_total","valor","receita"
]
CAND_PRODUTO = [
    "produto","ds_produto","cd_produto","nome_produto","plano","ds_plano","produto_comercial"
]
CAND_ID_BENEF = ["id_beneficiario","id_benef","id_pessoa","id_cliente"]
CAND_DT_NASC = ["dt_nascimento","nascimento","data_nascimento"]
CAND_ID_PREST_AUT = ["id_prestador"]  # em autorizacao
CAND_ID_PREST_CONTA = ["id_prestador_pagamento","id_prestador","id_prestador_envio"]  # em conta
CAND_NM_PREST = ["nm_prestador","ds_prestador","nome","razao_social"]

# possíveis colunas/status p/ "ativos"
CAND_STATUS_BENEF = [
    "ds_situacao","situacao","status","st_beneficiario","fl_ativo","in_ativo","ativo"
]

# cidade/UF
CAND_CIDADE = ["cidade","nm_cidade","ds_cidade","municipio","cidade_beneficiario"]
CAND_UF = ["uf","sg_uf","ds_uf","estado"]

# ======================================================================
# FUNÇÕES DE DESCOBERTA (SINISTRALIDADE)
# ======================================================================
def get_competencia_and_val_exprs(con) -> Tuple[str,str,str,str]:
    if not table_exists(con, "conta") or not table_exists(con, "mensalidade"):
        raise HTTPException(status_code=400, detail="Requer tabelas 'conta' e 'mensalidade'.")

    data_conta = find_col(con, "conta", CAND_DATA_CONTA)
    valor_conta = find_col(con, "conta", CAND_VALOR_CONTA)
    if not data_conta or not valor_conta:
        raise HTTPException(status_code=400, detail="Não encontrei DATA/VALOR em 'conta' (ex.: dt_competencia, vl_liberado).")

    data_mens = find_col(con, "mensalidade", CAND_DATA_MENS)
    valor_mens = find_col(con, "mensalidade", CAND_VALOR_MENS)
    if not data_mens or not valor_mens:
        raise HTTPException(status_code=400, detail="Não encontrei DATA/VALOR em 'mensalidade' (ex.: dt_competencia, vl_faturado).")

    mes_conta = month_expr(f"conta.{data_conta}")
    custo_expr = numeric_expr(f"conta.{valor_conta}")
    mes_mens = month_expr(f"mensalidade.{data_mens}")
    receita_expr = numeric_expr(f"mensalidade.{valor_mens}")
    return mes_conta, custo_expr, mes_mens, receita_expr

def get_produto_cols(con) -> Dict[str, Optional[str]]:
    out = {"conta_prod": None, "mens_prod": None, "benef_prod": None}
    if table_exists(con, "conta"):
        out["conta_prod"] = find_col(con, "conta", CAND_PRODUTO)
    if table_exists(con, "mensalidade"):
        out["mens_prod"] = find_col(con, "mensalidade", CAND_PRODUTO)
    if table_exists(con, "beneficiario"):
        out["benef_prod"] = find_col(con, "beneficiario", CAND_PRODUTO)
    return out

def get_benef_keys(con) -> Dict[str, Optional[str]]:
    return {
        "conta_benef": find_col(con, "conta", CAND_ID_BENEF) if table_exists(con,"conta") else None,
        "mens_benef": find_col(con, "mensalidade", CAND_ID_BENEF) if table_exists(con,"mensalidade") else None,
        "benef_key": find_col(con, "beneficiario", CAND_ID_BENEF) if table_exists(con,"beneficiario") else None,
    }

def get_dt_nasc(con) -> Optional[str]:
    if table_exists(con, "beneficiario"):
        return find_col(con, "beneficiario", CAND_DT_NASC)
    return None

def get_status_col(con) -> Optional[str]:
    if not table_exists(con, "beneficiario"):
        return None
    return find_col(con, "beneficiario", CAND_STATUS_BENEF)

def status_ativo_clause(col: str) -> str:
    """
    Tenta cobrir variações comuns de 'ativo'.
    """
    return (
        "("
        f" upper({col}) LIKE '%ATIV%' "
        f" OR {col} IN ('1','S','SIM','s','sim','true','TRUE','t','T') "
        ")"
    )

# ======================================================================
# META/DIAGNÓSTICO
# ======================================================================
@app.get("/health")
def health():
    con = get_con()
    tables = ["beneficiario", "conta", "mensalidade", "prestador", "autorizacao"]
    info = {}
    for t in tables:
        if table_exists(con, t):
            try:
                n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            except Exception:
                n = None
            info[t] = {"exists": True, "rows": n}
        else:
            info[t] = {"exists": False, "rows": None}
    return {"ok": True, "db": DB_PATH, "tables": info}

@app.get("/meta/schema")
def meta_schema():
    con = get_con()
    out = {}
    for t in ["beneficiario", "conta", "mensalidade", "prestador", "autorizacao"]:
        if table_exists(con, t):
            out[t] = get_cols(con, t)
    return out

@app.get("/meta/sample")
def meta_sample(table: str = Query(...), limit: int = 5):
    con = get_con()
    if not table_exists(con, table):
        raise HTTPException(status_code=400, detail=f"Tabela '{table}' não existe.")
    df = con.execute(f"SELECT * FROM {table} LIMIT {limit}").df()
    return {"table": table, "limit": limit, "rows": df.to_dict(orient="records")}

@app.get("/meta/meses")
def meta_meses(table: str = Query(...), col: str = Query(...), limit: int = Query(60, ge=1, le=240)):
    con = get_con()
    if not table_exists(con, table):
        raise HTTPException(status_code=400, detail=f"Tabela '{table}' não existe.")
    cols = get_cols(con, table)
    if col not in cols:
        raise HTTPException(status_code=400, detail=f"Coluna '{col}' não existe em '{table}'.")
    expr = month_expr(f"{table}.{col}")
    sql = f"""
        SELECT DISTINCT {expr} AS mes
        FROM {table}
        WHERE {expr} IS NOT NULL
        ORDER BY mes DESC
        LIMIT ?
    """
    rows = con.execute(sql, [limit]).fetchall()
    return {"table": table, "col": col, "meses": [r[0] for r in rows]}

# ======================================================================
# FILTROS DE BENEFICIÁRIO (usados em utilização)
# ======================================================================
def add_benef_filters(con, table_alias: str, filtros: Dict[str, Optional[str]]) -> (List[str], List[Any]):
    wheres, binds = [], []
    if filtros.get("uf"):
        col = find_col(con, "beneficiario", CAND_UF)
        if col:
            wheres.append(f"upper({table_alias}.{col}) = upper(?)")
            binds.append(filtros["uf"])
    if filtros.get("cidade"):
        col = find_col(con, "beneficiario", CAND_CIDADE)
        if col:
            wheres.append(f"upper({table_alias}.{col}) = upper(?)")
            binds.append(filtros["cidade"])
    if filtros.get("sexo"):
        col = find_col(con, "beneficiario", ["sexo","ds_sexo","cd_sexo"])
        if col:
            wheres.append(f"upper({table_alias}.{col}) = upper(?)")
            binds.append(filtros["sexo"])
    if filtros.get("faixa"):
        faixa = filtros["faixa"].strip()
        col_nasc = find_col(con, "beneficiario", CAND_DT_NASC)
        if col_nasc:
            if faixa.endswith("+"):
                idade_min = int(faixa[:-1])
                wheres.append(f"date_diff('year', CAST({table_alias}.{col_nasc} AS DATE), CURRENT_DATE) >= ?")
                binds.append(idade_min)
            else:
                a, b = faixa.split("-")
                idade_min, idade_max = int(a), int(b)
                wheres.append(f"date_diff('year', CAST({table_alias}.{col_nasc} AS DATE), CURRENT_DATE) BETWEEN ? AND ?")
                binds += [idade_min, idade_max]
    return wheres, binds

# ======================================================================
# KPI: UTILIZAÇÃO (existentes)
# ======================================================================
@app.get("/kpi/utilizacao/resumo")
def kpi_utilizacao_resumo(
    competencia: str = Query(..., description="YYYY-MM"),
    produto: Optional[str] = Query(None),
    uf: Optional[str] = Query(None),
    cidade: Optional[str] = Query(None),
    sexo: Optional[str] = Query(None),
    faixa: Optional[str] = Query(None),
):
    con = get_con()
    if not table_exists(con, "autorizacao"):
        raise HTTPException(status_code=400, detail="Tabela 'autorizacao' não existe.")
    if not table_exists(con, "beneficiario"):
        raise HTTPException(status_code=400, detail="Tabela 'beneficiario' não existe.")

    id_benef_aut = find_col(con, "autorizacao", ["id_beneficiario"])
    dt_aut = find_col(con, "autorizacao", ["dt_autorizacao"])
    if not id_benef_aut or not dt_aut:
        raise HTTPException(status_code=400, detail="Faltam colunas em 'autorizacao' (id_beneficiario/dt_autorizacao).")
    id_benef_ben = find_col(con, "beneficiario", CAND_ID_BENEF) or "id_beneficiario"

    filtros = {"uf": uf, "cidade": cidade, "sexo": sexo, "faixa": faixa}
    where_b, binds_b = add_benef_filters(con, "b", filtros)

    # Produto via conta (opcional)
    produto_where, produto_binds, join_conta = [], [], ""
    if produto and table_exists(con, "conta"):
        col_prod = find_col(con, "conta", CAND_PRODUTO)
        col_benef_conta = find_col(con, "conta", CAND_ID_BENEF)
        if col_prod and col_benef_conta:
            join_conta = f" LEFT JOIN conta c ON c.{col_benef_conta} = b.{id_benef_ben} "
            produto_where.append(f"upper(c.{col_prod}) = upper(?)")
            produto_binds.append(produto)

    mes_expr = month_expr(f"a.{dt_aut}")
    wheres = [f"{mes_expr} = ?"]
    binds: List[Any] = [competencia]
    if where_b:
        wheres += where_b; binds += binds_b
    if produto_where:
        wheres += produto_where; binds += produto_binds
    where_sql = " AND ".join(wheres) if wheres else "1=1"

    sql_util = f"""
        SELECT COUNT(DISTINCT a.{id_benef_aut}) AS utilizados
        FROM autorizacao a
        JOIN beneficiario b ON b.{id_benef_ben} = a.{id_benef_aut}
        {join_conta}
        WHERE {where_sql}
    """

    # base (sem mês)
    where_base = []
    binds_base: List[Any] = []
    if where_b:
        where_base += where_b; binds_base += binds_b
    if produto_where:
        where_base += produto_where; binds_base += produto_binds
    where_base_sql = " AND ".join(where_base) if where_base else "1=1"
    join_conta_base = join_conta

    sql_base = f"""
        SELECT COUNT(DISTINCT b.{id_benef_ben}) AS base
        FROM beneficiario b
        {join_conta_base}
        WHERE {where_base_sql}
    """

    col_qt = find_col(con, "autorizacao", ["qt_autorizada"])
    if col_qt:
        sql_aut = f"""
            SELECT COALESCE(SUM(CAST(a.{col_qt} AS DOUBLE)), 0) AS autorizacoes
            FROM autorizacao a
            JOIN beneficiario b ON b.{id_benef_ben} = a.{id_benef_aut}
            {join_conta}
            WHERE {where_sql}
        """
    else:
        sql_aut = f"""
            SELECT COUNT(*) AS autorizacoes
            FROM autorizacao a
            JOIN beneficiario b ON b.{id_benef_ben} = a.{id_benef_aut}
            {join_conta}
            WHERE {where_sql}
        """

    try:
        utilizados = int(con.execute(sql_util, binds).fetchone()[0])
        base = int(con.execute(sql_base, binds_base).fetchone()[0])
        autorizacoes = float(con.execute(sql_aut, binds).fetchone()[0])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao calcular resumo: {e}")

    return {
        "competencia": competencia,
        "beneficiarios_base": base,
        "beneficiarios_utilizados": utilizados,
        "autorizacoes": autorizacoes,
        "filtros_aplicados": {k: v for k, v in {"produto": produto, "uf": uf, "cidade": cidade, "sexo": sexo, "faixa": faixa}.items() if v},
    }

@app.get("/kpi/utilizacao/evolucao")
def kpi_utilizacao_evolucao(
    desde: str = Query(..., description="YYYY-MM"),
    ate: str = Query(..., description="YYYY-MM"),
):
    con = get_con()
    if not table_exists(con, "autorizacao"):
        raise HTTPException(status_code=400, detail="Tabela 'autorizacao' não existe no banco.")
    id_benef = find_col(con, "autorizacao", ["id_beneficiario"])
    dt_aut = find_col(con, "autorizacao", ["dt_autorizacao"])
    if not id_benef or not dt_aut:
        raise HTTPException(status_code=400, detail="Faltam colunas em 'autorizacao' (id_beneficiario/dt_autorizacao).")

    def parse_m(s): return datetime.strptime(s, "%Y-%m")
    cur = parse_m(desde); end = parse_m(ate)
    meses = []
    while cur <= end:
        meses.append(cur.strftime("%Y-%m"))
        if cur.month == 12: cur = cur.replace(year=cur.year + 1, month=1)
        else: cur = cur.replace(month=cur.month + 1)

    mes_expr = month_expr(f"a.{dt_aut}")
    out = []
    for m in meses:
        n = con.execute(
            f"SELECT COUNT(DISTINCT {id_benef}) FROM autorizacao a WHERE {mes_expr} = ?",
            [m],
        ).fetchone()[0]
        out.append({"competencia": m, "beneficiarios_utilizados": int(n)})
    return {"desde": desde, "ate": ate, "evolucao": out}

# ======================================================================
# KPI: PRESTADOR (existente + novo)
# ======================================================================
@app.get("/kpi/prestador/top")
def kpi_prestador_top(competencia: str = Query(..., description="YYYY-MM"), limite: int = 10):
    con = get_con()
    if not table_exists(con, "autorizacao"):
        raise HTTPException(status_code=400, detail="Tabela 'autorizacao' não existe.")
    id_prest = find_col(con, "autorizacao", CAND_ID_PREST_AUT)
    dt_aut = find_col(con, "autorizacao", ["dt_autorizacao"])
    if not id_prest or not dt_aut:
        raise HTTPException(status_code=400, detail="Colunas 'id_prestador' e/ou 'dt_autorizacao' ausentes em 'autorizacao'.")
    col_qt = find_col(con, "autorizacao", ["qt_autorizada"])
    agg = f"COALESCE(SUM(CAST(a.{col_qt} AS DOUBLE)),0)" if col_qt else "COUNT(*)"

    join_prest = ""; nome_col = None
    if table_exists(con, "prestador"):
        id_prest_tab = find_col(con, "prestador", ["id_prestador","id_prest"])
        nome_col = find_col(con, "prestador", CAND_NM_PREST)
        if id_prest_tab:
            join_prest = f" LEFT JOIN prestador p ON p.{id_prest_tab} = a.{id_prest} "

    mes_expr = month_expr(f"a.{dt_aut}")
    sql = f"""
        SELECT a.{id_prest} AS id_prestador,
               {agg} AS score
               {', p.' + nome_col + ' AS nome' if join_prest and nome_col else ''}
        FROM autorizacao a
        {join_prest}
        WHERE {mes_expr} = ?
        GROUP BY 1 {', 3' if join_prest and nome_col else ''}
        ORDER BY 2 DESC
        LIMIT ?
    """
    rows = con.execute(sql, [competencia, limite]).fetchall()
    out = []
    for r in rows:
        if join_prest and nome_col:
            out.append({"id_prestador": r[0], "nome": r[2], "score": float(r[1])})
        else:
            out.append({"id_prestador": r[0], "score": float(r[1])})
    return {"competencia": competencia, "top": out}

@app.get("/kpi/prestador/impacto")
def kpi_prestador_impacto(competencia: str = Query(..., description="YYYY-MM"), top: int = 10):
    """
    Impacto = custo do prestador no mês. (Receita é global do plano; opcionalmente retornamos sinistralidade relativa = custo / receita_total_mensal)
    """
    con = get_con()
    if not table_exists(con, "conta"):
        raise HTTPException(status_code=400, detail="Tabela 'conta' não existe.")
    mes_conta, custo_expr, mes_mens, receita_expr = get_competencia_and_val_exprs(con)

    id_prest_conta = find_col(con, "conta", CAND_ID_PREST_CONTA)
    if not id_prest_conta:
        raise HTTPException(status_code=400, detail="Não encontrei coluna de prestador em 'conta' (ex.: id_prestador_pagamento).")

    # receita total do mês (para referência/sinistralidade relativa)
    receita_total = con.execute(
        f"SELECT COALESCE(SUM({receita_expr}),0) FROM mensalidade WHERE {mes_mens} = ?",
        [competencia],
    ).fetchone()[0] or 0

    # nome do prestador (opcional)
    join_prest = ""; nome_col = None; id_prest_tab = None
    if table_exists(con, "prestador"):
        id_prest_tab = find_col(con, "prestador", ["id_prestador","id_prest"])
        nome_col = find_col(con, "prestador", CAND_NM_PREST)
        if id_prest_tab:
            join_prest = f" LEFT JOIN prestador p ON p.{id_prest_tab} = conta.{id_prest_conta} "

    sql = f"""
        SELECT conta.{id_prest_conta} AS id_prestador,
               SUM({custo_expr}) AS custo
               {', p.' + nome_col + ' AS nome' if join_prest and nome_col else ''}
        FROM conta
        {join_prest}
        WHERE {mes_conta} = ?
        GROUP BY 1 {', 3' if join_prest and nome_col else ''}
        ORDER BY 2 DESC
        LIMIT ?
    """
    rows = con.execute(sql, [competencia, top]).fetchall()
    out = []
    for r in rows:
        if join_prest and nome_col:
            out.append({
                "id_prestador": r[0],
                "nome": r[2],
                "custo": float(r[1]),
                "sinistralidade_relativa": (float(r[1])/float(receita_total)) if receita_total != 0 else None
            })
        else:
            out.append({
                "id_prestador": r[0],
                "custo": float(r[1]),
                "sinistralidade_relativa": (float(r[1])/float(receita_total)) if receita_total != 0 else None
            })
    return {"competencia": competencia, "receita_total_mes": float(receita_total), "top": out}

# ======================================================================
# KPI: SINISTRALIDADE (existentes)
# ======================================================================
@app.get("/kpi/sinistralidade/ultima")
def kpi_sin_ultima():
    con = get_con()
    mes_conta, custo_expr, mes_mens, receita_expr = get_competencia_and_val_exprs(con)
    sql = f"""
        WITH custos AS (
            SELECT {mes_conta} AS mes, SUM({custo_expr}) AS custo
            FROM conta
            WHERE {mes_conta} IS NOT NULL
            GROUP BY 1
        ),
        receitas AS (
            SELECT {mes_mens} AS mes, SUM({receita_expr}) AS receita
            FROM mensalidade
            WHERE {mes_mens} IS NOT NULL
            GROUP BY 1
        )
        SELECT COALESCE(custos.mes, receitas.mes) AS mes,
               COALESCE(custos.custo, 0) AS custo,
               COALESCE(receitas.receita, 0) AS receita
        FROM custos
        FULL OUTER JOIN receitas USING (mes)
        WHERE mes IS NOT NULL
        ORDER BY mes DESC
        LIMIT 1
    """
    row = con.execute(sql).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Sem dados para calcular sinistralidade.")
    mes, custo, receita = row[0], float(row[1]), float(row[2])
    sin = (custo / receita) if receita != 0 else None
    return {"competencia": mes, "custo": custo, "receita": receita, "sinistralidade": sin,
            "observacao": None if receita != 0 else "Receita igual a 0 nesta competência; sinistralidade indefinida."}

@app.get("/kpi/sinistralidade/media")
def kpi_sin_media(meses: int = Query(6, ge=1, le=36)):
    con = get_con()
    mes_conta, custo_expr, mes_mens, receita_expr = get_competencia_and_val_exprs(con)
    series_sql = f"""
        WITH custos AS (
            SELECT {mes_conta} AS mes, SUM({custo_expr}) AS custo
            FROM conta
            WHERE {mes_conta} IS NOT NULL
            GROUP BY 1
        ),
        receitas AS (
            SELECT {mes_mens} AS mes, SUM({receita_expr}) AS receita
            FROM mensalidade
            WHERE {mes_mens} IS NOT NULL
            GROUP BY 1
        ),
        joined AS (
            SELECT COALESCE(c.mes, r.mes) AS mes,
                   COALESCE(c.custo, 0) AS custo,
                   COALESCE(r.receita, 0) AS receita
            FROM custos c
            FULL OUTER JOIN receitas r USING (mes)
            WHERE COALESCE(c.mes, r.mes) IS NOT NULL
        )
        SELECT mes, custo, receita
        FROM joined
        ORDER BY mes DESC
        LIMIT ?
    """
    rows = con.execute(series_sql, [meses]).fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail="Sem dados para calcular a média de sinistralidade.")
    serie = []
    total_custo = 0.0; total_receita = 0.0
    for mes, custo, receita in rows:
        custo = float(custo); receita = float(receita)
        total_custo += custo; total_receita += receita
        sin = (custo / receita) if receita != 0 else None
        serie.append({"competencia": mes, "custo": custo, "receita": receita, "sinistralidade": sin})
    media_agregada = (total_custo / total_receita) if total_receita != 0 else None
    return {"meses_considerados": len(serie), "media_sinistralidade_agregada": media_agregada,
            "custo_total": total_custo, "receita_total": total_receita, "serie": list(reversed(serie))}

@app.get("/kpi/sinistralidade/diferenca")
def kpi_sin_diferenca(competencia: str = Query(..., description="YYYY-MM")):
    con = get_con()
    mes_conta, custo_expr, mes_mens, receita_expr = get_competencia_and_val_exprs(con)
    sql = f"""
        WITH c AS (
            SELECT SUM({custo_expr}) AS custo FROM conta WHERE {mes_conta} = ?
        ),
        r AS (
            SELECT SUM({receita_expr}) AS receita FROM mensalidade WHERE {mes_mens} = ?
        )
        SELECT c.custo, r.receita FROM c, r
    """
    custo, receita = con.execute(sql, [competencia, competencia]).fetchone()
    custo = float(custo or 0); receita = float(receita or 0)
    return {"competencia": competencia, "custo": custo, "receita": receita, "diferenca": receita - custo,
            "sinistralidade": (custo/receita) if receita != 0 else None}

@app.get("/kpi/sinistralidade/tendencia_trimestre")
def kpi_sin_tendencia_trimestre(fim: str = Query(..., description="YYYY-MM (mês final)")):
    con = get_con()
    mes_conta, custo_expr, mes_mens, receita_expr = get_competencia_and_val_exprs(con)
    base = datetime.strptime(fim, "%Y-%m")
    meses = []
    for i in (2,1,0):
        y = base.year
        mo = base.month - i
        while mo <= 0:
            y -= 1; mo += 12
        meses.append(f"{y:04d}-{mo:02d}")

    out = []
    for m in meses:
        row = con.execute(
            f"""
            WITH c AS (SELECT SUM({custo_expr}) AS custo FROM conta WHERE {mes_conta} = ?),
                 r AS (SELECT SUM({receita_expr}) AS receita FROM mensalidade WHERE {mes_mens} = ?)
            SELECT c.custo, r.receita FROM c, r
            """, [m, m]
        ).fetchone()
        custo = float((row[0] or 0)); receita = float((row[1] or 0))
        sin = (custo/receita) if receita != 0 else None
        out.append({"competencia": m, "custo": custo, "receita": receita, "sinistralidade": sin})

    vals = [x["sinistralidade"] for x in out if x["sinistralidade"] is not None]
    tendencia = "indefinida"
    if len(vals) >= 2:
        ini, fimv = vals[0], vals[-1]
        if ini == 0 and fimv == 0:
            tendencia = "estabilidade"
        elif ini == 0 and fimv > 0:
            tendencia = "alta"
        else:
            var = (fimv - ini) / ini if ini != 0 else float("inf")
            if var > 0.05: tendencia = "alta"
            elif var < -0.05: tendencia = "queda"
            else: tendencia = "estabilidade"

    return {"serie": out, "tendencia": tendencia, "regra": "compara 1º vs último; variação > 5% = alta/queda"}

@app.get("/kpi/sinistralidade/por_produto")
def kpi_sin_por_produto(competencia: str = Query(..., description="YYYY-MM"), top: int = 10):
    con = get_con()
    mes_conta, custo_expr, mes_mens, receita_expr = get_competencia_and_val_exprs(con)
    prods = get_produto_cols(con)
    keys = get_benef_keys(con)

    prod_conta = prods["conta_prod"]; prod_mens = prods["mens_prod"]; prod_benef = prods["benef_prod"]
    if not (prod_conta or prod_mens or prod_benef):
        raise HTTPException(status_code=400, detail="Não encontrei coluna de produto em conta/mensalidade/beneficiario.")

    if prod_conta:
        sql_custo = f"""
            SELECT conta.{prod_conta} AS produto, SUM({custo_expr}) AS custo
            FROM conta
            WHERE {mes_conta} = ?
            GROUP BY 1
        """
        custos = con.execute(sql_custo, [competencia]).fetchall()
    elif prod_benef and keys["conta_benef"] and keys["benef_key"]:
        sql_custo = f"""
            SELECT b.{prod_benef} AS produto, SUM({custo_expr}) AS custo
            FROM conta
            JOIN beneficiario b ON b.{keys['benef_key']} = conta.{keys['conta_benef']}
            WHERE {mes_conta} = ?
            GROUP BY 1
        """
        custos = con.execute(sql_custo, [competencia]).fetchall()
    else:
        raise HTTPException(status_code=400, detail="Impossível obter custos por produto (faltam colunas).")

    if prod_mens:
        sql_rec = f"""
            SELECT mensalidade.{prod_mens} AS produto, SUM({receita_expr}) AS receita
            FROM mensalidade
            WHERE {mes_mens} = ?
            GROUP BY 1
        """
        receitas = con.execute(sql_rec, [competencia]).fetchall()
    elif prod_benef and keys["mens_benef"] and keys["benef_key"]:
        sql_rec = f"""
            SELECT b.{prod_benef} AS produto, SUM({receita_expr}) AS receita
            FROM mensalidade
            JOIN beneficiario b ON b.{keys['benef_key']} = mensalidade.{keys['mens_benef']}
            WHERE {mes_mens} = ?
            GROUP BY 1
        """
        receitas = con.execute(sql_rec, [competencia]).fetchall()
    else:
        rec_total = con.execute(f"SELECT SUM({receita_expr}) FROM mensalidade WHERE {mes_mens} = ?", [competencia]).fetchone()[0] or 0
        out = [{"produto": r[0], "custo": float(r[1]), "receita": None, "sinistralidade": None} for r in custos]
        out.sort(key=lambda x: x["custo"], reverse=True)
        return {"competencia": competencia, "observacao": "Não há coluna de produto na mensalidade/beneficiário para ratear receita por produto.",
                "receita_total": float(rec_total), "itens": out[:top]}

    mapa_c = {str(r[0]): float(r[1] or 0) for r in custos}
    mapa_r = {str(r[0]): float(r[1] or 0) for r in receitas}
    todos = set(mapa_c) | set(mapa_r)
    itens = []
    for p in todos:
        custo = mapa_c.get(p, 0.0); receita = mapa_r.get(p, 0.0)
        sin = (custo/receita) if receita != 0 else None
        itens.append({"produto": p, "custo": custo, "receita": receita, "sinistralidade": sin})

    itens.sort(key=lambda x: (x["sinistralidade"] if x["sinistralidade"] is not None else -1), reverse=True)
    return {"competencia": competencia, "itens": itens[:top]}

def parse_bins(bins: str) -> List[Tuple[int, Optional[int], str]]:
    out = []
    for part in bins.split(","):
        part = part.strip()
        if not part: continue
        if part.endswith("+"):
            n = int(part[:-1]); out.append((n, None, part))
        else:
            a,b = part.split("-"); out.append((int(a), int(b), part))
    return out

@app.get("/kpi/sinistralidade/por_faixa")
def kpi_sin_por_faixa(competencia: str = Query(..., description="YYYY-MM"),
                      bins: str = Query("0-18,19-59,60+")):
    con = get_con()
    mes_conta, custo_expr, mes_mens, receita_expr = get_competencia_and_val_exprs(con)
    keys = get_benef_keys(con)
    dt_nasc = get_dt_nasc(con)
    if not (keys["conta_benef"] and keys["mens_benef"] and keys["benef_key"] and dt_nasc):
        raise HTTPException(status_code=400, detail="Para faixas etárias, é necessário id_benef em conta/mensalidade/beneficiario e dt_nascimento em beneficiario.")

    ref = datetime.strptime(competencia, "%Y-%m")
    ref_date = f"DATE '{ref.year:04d}-{ref.month:02d}-01'"
    idade_expr = f"DATE_DIFF('year', CAST(b.{dt_nasc} AS DATE), {ref_date})"

    faixas = parse_bins(bins)
    sql_custo = f"""
        SELECT
          CASE
            {" ".join([f"WHEN {idade_expr} >= {a} AND {idade_expr} <= {b} THEN '{label}'" for a,b,label in faixas if b is not None])}
            {" ".join([f"WHEN {idade_expr} >= {a} THEN '{label}'" for a,b,label in faixas if b is None])}
            ELSE 'OUTROS'
          END AS faixa,
          SUM({custo_expr}) AS custo
        FROM conta
        JOIN beneficiario b ON b.{keys['benef_key']} = conta.{keys['conta_benef']}
        WHERE {mes_conta} = ?
        GROUP BY 1
    """
    custos = con.execute(sql_custo, [competencia]).fetchall()

    sql_rec = f"""
        SELECT
          CASE
            {" ".join([f"WHEN {idade_expr} >= {a} AND {idade_expr} <= {b} THEN '{label}'" for a,b,label in faixas if b is not None])}
            {" ".join([f"WHEN {idade_expr} >= {a} THEN '{label}'" for a,b,label in faixas if b is None])}
            ELSE 'OUTROS'
          END AS faixa,
          SUM({receita_expr}) AS receita
        FROM mensalidade
        JOIN beneficiario b ON b.{keys['benef_key']} = mensalidade.{keys['mens_benef']}
        WHERE {mes_mens} = ?
        GROUP BY 1
    """
    receitas = con.execute(sql_rec, [competencia]).fetchall()

    mapa_c = {str(r[0]): float(r[1] or 0) for r in custos}
    mapa_r = {str(r[0]): float(r[1] or 0) for r in receitas}
    todos = set(mapa_c) | set(mapa_r)
    itens = []
    for fx in sorted(todos):
        custo = mapa_c.get(fx, 0.0); receita = mapa_r.get(fx, 0.0)
        sin = (custo/receita) if receita != 0 else None
        itens.append({"faixa": fx, "custo": custo, "receita": receita, "sinistralidade": sin})
    order = [lbl for _,_,lbl in faixas] + ["OUTROS"]
    itens.sort(key=lambda x: order.index(x["faixa"]) if x["faixa"] in order else 999)
    return {"competencia": competencia, "bins": [lbl for _,_,lbl in faixas], "itens": itens}

# === NOVOS: por cidade e só ativos ===================================
@app.get("/kpi/sinistralidade/por_cidade")
def kpi_sin_por_cidade(competencia: str = Query(..., description="YYYY-MM"), top: int = 10):
    con = get_con()
    if not table_exists(con, "beneficiario"):
        raise HTTPException(status_code=400, detail="Tabela 'beneficiario' não existe.")
    mes_conta, custo_expr, mes_mens, receita_expr = get_competencia_and_val_exprs(con)
    keys = get_benef_keys(con)
    col_cidade = find_col(con, "beneficiario", CAND_CIDADE)
    if not (keys["conta_benef"] and keys["mens_benef"] and keys["benef_key"] and col_cidade):
        raise HTTPException(status_code=400, detail="Necessário id_benef em conta/mensalidade/beneficiario e coluna de cidade em beneficiario.")

    sql_custo = f"""
        SELECT upper(b.{col_cidade}) AS cidade, SUM({custo_expr}) AS custo
        FROM conta
        JOIN beneficiario b ON b.{keys['benef_key']} = conta.{keys['conta_benef']}
        WHERE {mes_conta} = ?
        GROUP BY 1
    """
    custos = {r[0] or "SEM_CIDADE": float(r[1] or 0) for r in con.execute(sql_custo, [competencia]).fetchall()}

    sql_rec = f"""
        SELECT upper(b.{col_cidade}) AS cidade, SUM({receita_expr}) AS receita
        FROM mensalidade
        JOIN beneficiario b ON b.{keys['benef_key']} = mensalidade.{keys['mens_benef']}
        WHERE {mes_mens} = ?
        GROUP BY 1
    """
    receitas = {r[0] or "SEM_CIDADE": float(r[1] or 0) for r in con.execute(sql_rec, [competencia]).fetchall()}

    cidades = set(custos) | set(receitas)
    itens = []
    for c in cidades:
        custo = custos.get(c, 0.0); receita = receitas.get(c, 0.0)
        sin = (custo/receita) if receita != 0 else None
        itens.append({"cidade": c, "custo": custo, "receita": receita, "sinistralidade": sin})
    itens.sort(key=lambda x: (x["sinistralidade"] if x["sinistralidade"] is not None else -1), reverse=True)
    return {"competencia": competencia, "itens": itens[:top]}

@app.get("/kpi/sinistralidade/ativos")
def kpi_sin_ativos(competencia: str = Query(..., description="YYYY-MM")):
    """
    Sinistralidade considerando apenas beneficiários marcados como 'ativos'.
    Detecta automaticamente a coluna de status; se não encontrar, retorna 400.
    """
    con = get_con()
    mes_conta, custo_expr, mes_mens, receita_expr = get_competencia_and_val_exprs(con)
    keys = get_benef_keys(con)
    status_col = get_status_col(con)
    if not (keys["conta_benef"] and keys["mens_benef"] and keys["benef_key"] and status_col):
        raise HTTPException(status_code=400, detail="Não encontrei coluna de status em 'beneficiario' ou chaves para join.")

    ativo_clause = status_ativo_clause(f"b.{status_col}")

    sql_custo = f"""
        SELECT COALESCE(SUM({custo_expr}),0)
        FROM conta
        JOIN beneficiario b ON b.{keys['benef_key']} = conta.{keys['conta_benef']}
        WHERE {mes_conta} = ?
          AND {ativo_clause}
    """
    custo = float(con.execute(sql_custo, [competencia]).fetchone()[0] or 0)

    sql_rec = f"""
        SELECT COALESCE(SUM({receita_expr}),0)
        FROM mensalidade
        JOIN beneficiario b ON b.{keys['benef_key']} = mensalidade.{keys['mens_benef']}
        WHERE {mes_mens} = ?
          AND {ativo_clause}
    """
    receita = float(con.execute(sql_rec, [competencia]).fetchone()[0] or 0)

    sin = (custo/receita) if receita != 0 else None
    return {"competencia": competencia, "custo_ativos": custo, "receita_ativos": receita, "sinistralidade_ativos": sin}

# ======================================================================
# PLACEHOLDER
# ======================================================================
@app.get("/kpi/sinistralidade/nota")
def kpi_sin_nota():
    return {"detail": "Endpoints adicionais (especialidade, por sexo/UF, etc.) podem ser habilitados após validarmos as colunas específicas."}
