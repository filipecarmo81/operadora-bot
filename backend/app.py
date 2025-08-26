from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from typing import Optional, List, Dict, Any
from datetime import datetime
from pathlib import Path
import duckdb
import pandas as pd

# ======================================================================
# CONFIG
# ======================================================================
# Caminho do banco relativo a este arquivo (funciona local e no Render)
DB_PATH = str((Path(__file__).parent / "data" / "operadora.duckdb").resolve())

app = FastAPI(title="Operadora KPIs", version="0.4.0")

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
        if cand.lower() in cols:
            return cand
    return None

def require_cols(con, table: str, needed: List[str]) -> Dict[str, str]:
    found = {}
    for cand in needed:
        col = find_col(con, table, [cand])
        if not col:
            raise HTTPException(status_code=400, detail=f"Coluna '{cand}' não encontrada em '{table}'.")
        found[cand] = col
    return found

def safe_json(data: Any) -> JSONResponse:
    return JSONResponse(content=data)

# ======================================================================
# EXPRESSÕES ROBUSTAS PARA DATAS E NÚMEROS
# ======================================================================
DATE_CANDIDATES = [
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%d-%m-%Y",
    "%Y%m%d",
    "%Y/%m/%d",
]

def month_expr(col_sql: str) -> str:
    """
    Extrai 'YYYY-MM' de uma coluna de data/strings.
    Tenta múltiplos formatos e por fim faz CAST para DATE.
    """
    tries = [f"TRY_STRPTIME({col_sql}, '{fmt}')" for fmt in DATE_CANDIDATES]
    tries.append(f"CAST({col_sql} AS DATE)")
    coalesced = "COALESCE(" + ", ".join(tries) + ")"
    return f"strftime('%Y-%m', {coalesced})"

def numeric_expr(col_sql: str) -> str:
    """
    Converte valores monetários em DOUBLE de forma robusta.
    - Tenta CAST direto
    - Depois remove 'R$', separador de milhar e troca vírgula por ponto
    - Garante COALESCE para 0 quando não conseguir converter
    """
    # Remove 'R$' e espaços; remove pontos (milhar); troca vírgula por ponto
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
def meta_sample(table: str = Query(..., description="Nome da tabela"), limit: int = 5):
    con = get_con()
    if not table_exists(con, table):
        raise HTTPException(status_code=400, detail=f"Tabela '{table}' não existe.")
    df = con.execute(f"SELECT * FROM {table} LIMIT {limit}").df()
    return {"table": table, "limit": limit, "rows": df.to_dict(orient="records")}

@app.get("/meta/meses")
def meta_meses(
    table: str = Query(..., description="Tabela com coluna de data"),
    col: str = Query(..., description="Nome da coluna de data"),
    limit: int = Query(60, ge=1, le=240),
):
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
# FILTROS DE BENEFICIÁRIO
# ======================================================================
def add_benef_filters(con, table_alias: str, filtros: Dict[str, Optional[str]]) -> (List[str], List[Any]):
    wheres, binds = [], []

    # UF
    if filtros.get("uf"):
        col = find_col(con, "beneficiario", ["uf", "sg_uf", "ds_uf", "estado"])
        if col:
            wheres.append(f"upper({table_alias}.{col}) = upper(?)")
            binds.append(filtros["uf"])

    # Cidade
    if filtros.get("cidade"):
        col = find_col(con, "beneficiario", ["cidade", "nm_cidade", "ds_cidade"])
        if col:
            wheres.append(f"upper({table_alias}.{col}) = upper(?)")
            binds.append(filtros["cidade"])

    # Sexo
    if filtros.get("sexo"):
        col = find_col(con, "beneficiario", ["sexo", "ds_sexo", "cd_sexo"])
        if col:
            wheres.append(f"upper({table_alias}.{col}) = upper(?)")
            binds.append(filtros["sexo"])

    # Faixa etária (0-18, 19-59, 60+)
    if filtros.get("faixa"):
        faixa = filtros["faixa"].strip()
        col_nasc = find_col(con, "beneficiario", ["dt_nascimento", "nascimento", "data_nascimento"])
        if col_nasc:
            if faixa.endswith("+"):
                try:
                    idade_min = int(faixa[:-1])
                    wheres.append(f"date_diff('year', CAST({table_alias}.{col_nasc} AS DATE), CURRENT_DATE) >= ?")
                    binds.append(idade_min)
                except:
                    pass
            else:
                try:
                    a, b = faixa.split("-")
                    idade_min, idade_max = int(a), int(b)
                    wheres.append(f"date_diff('year', CAST({table_alias}.{col_nasc} AS DATE), CURRENT_DATE) BETWEEN ? AND ?")
                    binds += [idade_min, idade_max]
                except:
                    pass

    return wheres, binds

# ======================================================================
# KPI: UTILIZAÇÃO
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
        raise HTTPException(status_code=400, detail="Tabela 'autorizacao' não existe no banco.")
    if not table_exists(con, "beneficiario"):
        raise HTTPException(status_code=400, detail="Tabela 'beneficiario' não existe no banco.")

    id_benef_aut = find_col(con, "autorizacao", ["id_beneficiario"])
    dt_aut = find_col(con, "autorizacao", ["dt_autorizacao"])
    if not id_benef_aut or not dt_aut:
        raise HTTPException(status_code=400, detail="Faltam colunas em 'autorizacao' (id_beneficiario/dt_autorizacao).")

    id_benef_ben = find_col(con, "beneficiario", ["id_beneficiario", "id_benef"])
    if not id_benef_ben:
        raise HTTPException(status_code=400, detail="Falta coluna de chave em 'beneficiario' (id_beneficiario).")

    filtros = {"uf": uf, "cidade": cidade, "sexo": sexo, "faixa": faixa}
    where_b, binds_b = add_benef_filters(con, "b", filtros)

    # Produto via conta (opcional)
    produto_where, produto_binds, join_conta = [], [], ""
    if produto and table_exists(con, "conta"):
        col_prod = find_col(con, "conta", ["produto", "ds_produto", "cd_produto", "nome_produto"])
        col_benef_conta = find_col(con, "conta", ["id_beneficiario", "id_benef"])
        if col_prod and col_benef_conta:
            join_conta = f" LEFT JOIN conta c ON c.{col_benef_conta} = b.{id_benef_ben} "
            produto_where.append(f"upper(c.{col_prod}) = upper(?)")
            produto_binds.append(produto)

    mes_expr = month_expr(f"a.{dt_aut}")
    wheres = [f"{mes_expr} = ?"]
    binds: List[Any] = [competencia]

    if where_b:
        wheres += where_b
        binds += binds_b
    if produto_where:
        wheres += produto_where
        binds += produto_binds

    where_sql = " AND ".join(wheres) if wheres else "1=1"

    # utilizados
    sql_util = f"""
        SELECT COUNT(DISTINCT a.{id_benef_aut}) AS utilizados
        FROM autorizacao a
        JOIN beneficiario b ON b.{id_benef_ben} = a.{id_benef_aut}
        {join_conta}
        WHERE {where_sql}
    """

    # base
    where_base = []
    binds_base: List[Any] = []
    if where_b:
        where_base += where_b
        binds_base += binds_b
    if produto_where:
        where_base += produto_where
        binds_base += produto_binds
    where_base_sql = " AND ".join(where_base) if where_base else "1=1"

    join_conta_base = ""
    if join_conta:
        col_benef_conta = find_col(con, "conta", ["id_beneficiario", "id_benef"])
        join_conta_base = f" LEFT JOIN conta c ON c.{col_benef_conta} = b.{id_benef_ben} "

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

    # gera lista de meses
    def parse_m(s): return datetime.strptime(s, "%Y-%m")
    cur = parse_m(desde)
    end = parse_m(ate)
    meses = []
    while cur <= end:
        meses.append(cur.strftime("%Y-%m"))
        if cur.month == 12:
            cur = cur.replace(year=cur.year + 1, month=1)
        else:
            cur = cur.replace(month=cur.month + 1)

    mes_expr = month_expr(f"a.{dt_aut}")
    out = []
    for m in meses:
        try:
            n = con.execute(
                f"SELECT COUNT(DISTINCT {id_benef}) FROM autorizacao a WHERE {mes_expr} = ?",
                [m],
            ).fetchone()[0]
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Erro na competência {m}: {e}")
        out.append({"competencia": m, "beneficiarios_utilizados": int(n)})

    return {"desde": desde, "ate": ate, "evolucao": out}

# ======================================================================
# KPI: PRESTADOR (TOP)
# ======================================================================
@app.get("/kpi/prestador/top")
def kpi_prestador_top(competencia: str = Query(..., description="YYYY-MM"), limite: int = 10):
    con = get_con()
    if not table_exists(con, "autorizacao"):
        raise HTTPException(status_code=400, detail="Tabela 'autorizacao' não existe.")

    id_prest = find_col(con, "autorizacao", ["id_prestador"])
    dt_aut = find_col(con, "autorizacao", ["dt_autorizacao"])
    if not id_prest or not dt_aut:
        raise HTTPException(status_code=400, detail="Colunas 'id_prestador' e/ou 'dt_autorizacao' ausentes em 'autorizacao'.")

    col_qt = find_col(con, "autorizacao", ["qt_autorizada"])
    agg = f"COALESCE(SUM(CAST(a.{col_qt} AS DOUBLE)),0)" if col_qt else "COUNT(*)"

    join_prest = ""
    nome_col = None
    if table_exists(con, "prestador"):
        id_prest_tab = find_col(con, "prestador", ["id_prestador", "id_prest"])
        nome_col = find_col(con, "prestador", ["nm_prestador", "ds_prestador", "nome", "razao_social"])
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

# ======================================================================
# KPI: SINISTRALIDADE
# ======================================================================

# Candidatos para colunas (flexível a variações de nomes)
CAND_DATA_CONTA = [
    "dt_competencia", "competencia", "mes_competencia", "dt_emissao",
    "dt_pagamento", "dt_apresentacao", "data", "dt_liberacao"
]
CAND_VALOR_CONTA = [
    "vl_pago", "vl_liberado", "vl_aprovado", "vl_total", "vl_custo",
    "valor_pago", "valor_liberado", "valor_aprovado", "valor_total", "valor"
]
CAND_DATA_MENS = [
    "dt_competencia", "competencia", "mes_competencia", "dt_emissao",
    "dt_pagamento", "data", "dt_referencia", "dt_vencimento"
]
CAND_VALOR_MENS = [
    "vl_faturado", "vl_liquido", "vl_receita", "vl_total",
    "valor_faturado", "valor_liquido", "valor_total", "valor", "receita"
]

def get_competencia_and_val_exprs(con):
    """
    Descobre (ou falha com 400) as colunas de competência e valor em conta/mensalidade
    e retorna expressões (mes_conta, custo_expr, mes_mensalidade, receita_expr) prontas para usar.
    """
    if not table_exists(con, "conta") or not table_exists(con, "mensalidade"):
        raise HTTPException(status_code=400, detail="Requer tabelas 'conta' e 'mensalidade'.")

    # Conta (custo)
    data_conta = find_col(con, "conta", CAND_DATA_CONTA)
    if not data_conta:
        raise HTTPException(status_code=400, detail="Não encontrei coluna de DATA em 'conta' (ex.: dt_competencia).")
    valor_conta = find_col(con, "conta", CAND_VALOR_CONTA)
    if not valor_conta:
        raise HTTPException(status_code=400, detail="Não encontrei coluna de VALOR em 'conta' (ex.: vl_liberado/vl_pago).")

    mes_conta = month_expr(f"conta.{data_conta}")
    custo_expr = numeric_expr(f"conta.{valor_conta}")

    # Mensalidade (receita)
    data_mens = find_col(con, "mensalidade", CAND_DATA_MENS)
    if not data_mens:
        raise HTTPException(status_code=400, detail="Não encontrei coluna de DATA em 'mensalidade' (ex.: dt_competencia).")
    valor_mens = find_col(con, "mensalidade", CAND_VALOR_MENS)
    if not valor_mens:
        raise HTTPException(status_code=400, detail="Não encontrei coluna de VALOR em 'mensalidade' (ex.: vl_faturado/vl_liquido).")

    mes_mens = month_expr(f"mensalidade.{data_mens}")
    receita_expr = numeric_expr(f"mensalidade.{valor_mens}")

    return mes_conta, custo_expr, mes_mens, receita_expr

@app.get("/kpi/sinistralidade/ultima")
def kpi_sin_ultima():
    """
    Sinistralidade da competência mais recente disponível (full outer join de custos x receitas).
    sinistralidade = custo / receita  (se receita == 0, retorna null e avisa)
    """
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
        raise HTTPException(status_code=404, detail="Não há dados suficientes para calcular sinistralidade.")
    mes, custo, receita = row[0], float(row[1]), float(row[2])
    sin = (custo / receita) if receita != 0 else None
    return {
        "competencia": mes,
        "custo": custo,
        "receita": receita,
        "sinistralidade": sin,
        "observacao": None if receita != 0 else "Receita igual a 0 nesta competência; sinistralidade indefinida."
    }

@app.get("/kpi/sinistralidade/media")
def kpi_sin_media(meses: int = Query(6, ge=1, le=36)):
    """
    Média de sinistralidade nos últimos N meses.
    -> Usa a forma agregada (sum(custos) / sum(receitas)) no período (mais estável).
    Retorna também a lista mês a mês.
    """
    con = get_con()
    mes_conta, custo_expr, mes_mens, receita_expr = get_competencia_and_val_exprs(con)

    # Traz série mensal de custos e receitas, une, ordena desc e limita aos N mais recentes
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

    # Calcula série e agregados
    serie = []
    total_custo = 0.0
    total_receita = 0.0
    for mes, custo, receita in rows:
        custo = float(custo)
        receita = float(receita)
        total_custo += custo
        total_receita += receita
        sin = (custo / receita) if receita != 0 else None
        serie.append({"competencia": mes, "custo": custo, "receita": receita, "sinistralidade": sin})

    media_agregada = (total_custo / total_receita) if total_receita != 0 else None

    return {
        "meses_considerados": len(serie),
        "media_sinistralidade_agregada": media_agregada,
        "custo_total": total_custo,
        "receita_total": total_receita,
        "serie": list(reversed(serie))  # crescente no retorno (do mais antigo para o mais recente)
    }

# ======================================================================
# PLACEHOLDERS QUE SERÃO EXPANDIDOS
# ======================================================================
@app.get("/kpi/sinistralidade/nota")
def kpi_sin_nota():
    return {"detail": "Endpoints adicionais (por produto, tendência, por faixa etária) podem ser habilitados após validarmos as colunas de produto e faixas."}
