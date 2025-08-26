from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from typing import Optional, List, Dict, Any
from datetime import datetime
from pathlib import Path
import duckdb
import pandas as pd

# RESOLVE O CAMINHO DO BANCO RELATIVO A ESTE ARQUIVO
DB_PATH = str((Path(__file__).parent / "data" / "operadora.duckdb").resolve())

app = FastAPI(title="Operadora KPIs", version="0.3.0")

# ---------------- Conexão ----------------
def get_con():
    try:
        con = duckdb.connect(DB_PATH, read_only=False)
        return con
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha ao abrir DuckDB: {e}")

# ---------------- Utilitários de esquema/erros ----------------
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

# ---------------- Datas robustas ----------------
DATE_CANDIDATES = [
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%d-%m-%Y",
    "%Y%m%d",
    "%Y/%m/%d",
]

def month_expr(col_sql: str) -> str:
    """
    Retorna uma expressão SQL que extrai 'YYYY-MM' de uma coluna string/data,
    tentando múltiplos formatos com TRY_STRPTIME, e por último tenta CAST.
    """
    tries = [f"TRY_STRPTIME({col_sql}, '{fmt}')" for fmt in DATE_CANDIDATES]
    tries.append(f"CAST({col_sql} AS DATE)")
    coalesced = "COALESCE(" + ", ".join(tries) + ")"
    return f"strftime('%Y-%m', {coalesced})"

# ---------------- Diagnóstico ----------------
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
    """
    Lista os meses (YYYY-MM) encontrados na coluna informada, em ordem descrescente.
    Use: /meta/meses?table=autorizacao&col=dt_autorizacao
    """
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

# ---------------- Filtros de beneficiário ----------------
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

# ---------------- KPIs: Utilização ----------------
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

# ---------------- Prestador (top) ----------------
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

# ---------------- Sinistralidade (placeholders) ----------------
@app.get("/kpi/sinistralidade/ultima")
def kpi_sin_ultima():
    con = get_con()
    if not table_exists(con, "conta") or not table_exists(con, "mensalidade"):
        raise HTTPException(status_code=400, detail="Requer 'conta' e 'mensalidade' para sinistralidade.")
    return {"detail": "Cálculo de sinistralidade será habilitado assim que confirmarmos as colunas de valores."}

@app.get("/kpi/sinistralidade/media")
def kpi_sin_media(meses: int = Query(6, ge=1, le=36)):
    con = get_con()
    if not table_exists(con, "conta") or not table_exists(con, "mensalidade"):
        raise HTTPException(status_code=400, detail="Requer 'conta' e 'mensalidade' para sinistralidade.")
    return {"detail": "Cálculo de sinistralidade em construção — precisamos confirmar nomes das colunas de valores."}
