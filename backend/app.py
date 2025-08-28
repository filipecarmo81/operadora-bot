# backend/app.py
import os
from typing import List, Optional, Dict, Tuple
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import duckdb

# Caminho do banco
HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.getenv("DB_PATH", os.path.join(HERE, "data", "operadora.duckdb"))

app = FastAPI(title="Operadora KPIs", version="0.2.1")

# CORS liberado (frontend ou testes via navegador/curl/Postman)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=False,
)

# -----------------------
# Helpers de conexão/SQL
# -----------------------
def con_ro() -> duckdb.DuckDBPyConnection:
    try:
        return duckdb.connect(DB_PATH, read_only=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha ao abrir DuckDB: {e}")

def table_columns(c: duckdb.DuckDBPyConnection, table: str) -> List[str]:
    rows = c.execute(f"PRAGMA table_info('{table}')").fetchall()
    # PRAGMA table_info retorna: (cid, name, type, notnull, dflt_value, pk)
    return [r[1] for r in rows]

def pick_col(c: duckdb.DuckDBPyConnection, table: str, candidates: List[str]) -> str:
    cols = set(table_columns(c, table))
    for col in candidates:
        if col in cols:
            return col
    raise HTTPException(
        status_code=400,
        detail=f"Não encontrei {candidates} em '{table}'. Colunas disponíveis: {sorted(cols)}",
    )

def latest_competencia(c: duckdb.DuckDBPyConnection) -> str:
    # procura colunas de competência em conta e mensalidade
    conta_mes_col = pick_col(c, "conta", ["dt_competencia", "dt_mes_competencia"])
    mensal_mes_col = pick_col(c, "mensalidade", ["dt_competencia", "dt_mes_competencia"])
    (ult,) = c.execute(
        f"""
        SELECT MAX(mes)::VARCHAR
        FROM (
          SELECT {conta_mes_col} AS mes FROM conta
          UNION
          SELECT {mensal_mes_col} AS mes FROM mensalidade
        )
        """
    ).fetchone()
    if not ult:
        raise HTTPException(status_code=404, detail="Não há competências em conta/mensalidade.")
    return ult

def sum_sinistro_e_premio(c: duckdb.DuckDBPyConnection, competencia: str) -> Tuple[float, float]:
    conta_mes_col = pick_col(c, "conta", ["dt_competencia", "dt_mes_competencia"])
    mensal_mes_col = pick_col(c, "mensalidade", ["dt_competencia", "dt_mes_competencia"])
    vl_lib = pick_col(c, "conta", ["vl_liberado"])
    vl_pre = pick_col(c, "mensalidade", ["vl_premio"])

    (sinistro,) = c.execute(
        f"SELECT COALESCE(SUM({vl_lib}),0) FROM conta WHERE {conta_mes_col} = ?",
        [competencia],
    ).fetchone()
    (premio,) = c.execute(
        f"SELECT COALESCE(SUM({vl_pre}),0) FROM mensalidade WHERE {mensal_mes_col} = ?",
        [competencia],
    ).fetchone()
    return float(sinistro or 0.0), float(premio or 0.0)

def prestador_nome_col(c: duckdb.DuckDBPyConnection) -> str:
    return pick_col(c, "prestador", ["nome", "nm_prestador", "razao_social", "ds_prestador"])

def autorizacao_data_col(c: duckdb.DuckDBPyConnection) -> str:
    # para filtrar por mês
    return pick_col(c, "autorizacao", ["dt_autorizacao", "dt_entrada"])

def month_filter_sql(col: str) -> str:
    # Filtro por competência 'YYYY-MM'
    # Usamos strftime para ser independente do tipo (DATE/TIMESTAMP/STRING)
    return f"strftime({col}, '%Y-%m') = ?"

# -------------
# Endpoints
# -------------

@app.get("/")
def root():
    # NÃO consulta o DB aqui para não quebrar a raiz se o schema mudar.
    return {
        "ok": True,
        "message": "API do Operadora Bot. Use /docs para testar.",
        "db": DB_PATH,
        "endpoints": [
            "/health",
            "/kpi/sinistralidade/ultima",
            "/kpi/sinistralidade/competencia?competencia=YYYY-MM",
            "/kpi/prestador/top?competencia=YYYY-MM&limite=10",
            "/kpi/prestador/impacto?competencia=YYYY-MM&top=10",
            "/kpi/utilizacao/resumo?competencia=YYYY-MM",
        ],
    }

@app.get("/health")
def health():
    with con_ro() as c:
        # Apenas verifica acesso e contagem simples
        try:
            (n_conta,) = c.execute("SELECT COUNT(*) FROM conta").fetchone()
        except Exception:
            n_conta = None
        try:
            (n_mens,), = [(c.execute("SELECT COUNT(*) FROM mensalidade").fetchone())]
        except Exception:
            n_mens = None
        return {"ok": True, "db": DB_PATH, "conta_rows": n_conta, "mensalidade_rows": n_mens}

@app.get("/kpi/sinistralidade/ultima")
def sinistralidade_ultima():
    with con_ro() as c:
        comp = latest_competencia(c)
        sinistro, premio = sum_sinistro_e_premio(c, comp)
        sin = (sinistro / premio) if premio else 0.0
        return {
            "competencia": comp,
            "sinistro": sinistro,
            "receita": premio,
            "sinistralidade": sin,
        }

@app.get("/kpi/sinistralidade/competencia")
def sinistralidade_competencia(
    competencia: str = Query(..., pattern=r"^\d{4}-\d{2}$", description="YYYY-MM")
):
    with con_ro() as c:
        sinistro, premio = sum_sinistro_e_premio(c, competencia)
        sin = (sinistro / premio) if premio else 0.0
        return {
            "competencia": competencia,
            "sinistro": sinistro,
            "receita": premio,
            "sinistralidade": sin,
        }

@app.get("/kpi/prestador/top")
def prestador_top(
    competencia: str = Query(..., pattern=r"^\d{4}-\d{2}$", description="YYYY-MM"),
    limite: int = Query(10, ge=1, le=100),
):
    with con_ro() as c:
        conta_mes_col = pick_col(c, "conta", ["dt_competencia", "dt_mes_competencia"])
        vl_lib = pick_col(c, "conta", ["vl_liberado"])
        nm_col = prestador_nome_col(c)

        rows = c.execute(
            f"""
            SELECT c.id_prestador,
                   COALESCE(p.{nm_col}, CAST(c.id_prestador AS VARCHAR)) AS nome,
                   SUM(c.{vl_lib}) AS score
            FROM conta c
            LEFT JOIN prestador p USING (id_prestador)
            WHERE {conta_mes_col} = ?
            GROUP BY 1,2
            ORDER BY score DESC
            LIMIT ?
            """,
            [competencia, limite],
        ).fetchall()

        return {
            "competencia": competencia,
            "top": [{"id_prestador": r[0], "nome": r[1], "score": float(r[2] or 0.0)} for r in rows],
        }

@app.get("/kpi/prestador/impacto")
def prestador_impacto(
    competencia: str = Query(..., pattern=r"^\d{4}-\d{2}$", description="YYYY-MM"),
    top: int = Query(10, ge=1, le=100),
):
    with con_ro() as c:
        conta_mes_col = pick_col(c, "conta", ["dt_competencia", "dt_mes_competencia"])
        vl_lib = pick_col(c, "conta", ["vl_liberado"])
        nm_col = prestador_nome_col(c)

        (total_mes,) = c.execute(
            f"SELECT COALESCE(SUM({vl_lib}),0) FROM conta WHERE {conta_mes_col} = ?",
            [competencia],
        ).fetchone()

        rows = c.execute(
            f"""
            SELECT c.id_prestador,
                   COALESCE(p.{nm_col}, CAST(c.id_prestador AS VARCHAR)) AS nome,
                   SUM(c.{vl_lib}) AS custo
            FROM conta c
            LEFT JOIN prestador p USING (id_prestador)
            WHERE {conta_mes_col} = ?
            GROUP BY 1,2
            ORDER BY custo DESC
            LIMIT ?
            """,
            [competencia, top],
        ).fetchall()

        dados = []
        for r in rows:
            custo = float(r[2] or 0.0)
            perc = (custo / total_mes) if total_mes else 0.0
            dados.append({"id_prestador": r[0], "nome": r[1], "custo": custo, "participacao": perc})

        return {"competencia": competencia, "total_mes": float(total_mes or 0.0), "dados": dados}

@app.get("/kpi/utilizacao/resumo")
def utilizacao_resumo(
    competencia: str = Query(..., pattern=r"^\d{4}-\d{2}$", description="YYYY-MM"),
    produto: Optional[str] = Query(None),
    uf: Optional[str] = Query(None),
    cidade: Optional[str] = Query(None),
    sexo: Optional[str] = Query(None),
    faixa: Optional[str] = Query(None),
):
    with con_ro() as c:
        # Base total
        (base_total,) = c.execute("SELECT COUNT(*) FROM beneficiario").fetchone()

        # Coluna de data em autorizacao
        dt_aut = autorizacao_data_col(c)

        # Monta filtros opcionais se (e somente se) existirem as colunas
        filtros_sql = [month_filter_sql(dt_aut)]
        params: List[object] = [competencia]

        def tem_col(tab: str, col: str) -> bool:
            return col in set(table_columns(c, tab))

        if produto and tem_col("beneficiario", "produto"):
            filtros_sql.append("b.produto = ?")
            params.append(produto)

        if uf and tem_col("beneficiario", "uf"):
            filtros_sql.append("b.uf = ?")
            params.append(uf)

        if cidade and tem_col("beneficiario", "cidade"):
            filtros_sql.append("b.cidade = ?")
            params.append(cidade)

        if sexo and tem_col("beneficiario", "sexo"):
            filtros_sql.append("b.sexo = ?")
            params.append(sexo)

        if faixa and tem_col("beneficiario", "faixa"):
            filtros_sql.append("b.faixa = ?")
            params.append(faixa)

        where_clause = " AND ".join(filtros_sql)

        # Distintos que utilizaram no mês (pelo id_beneficiario de autorizacao)
        (utilizados,) = c.execute(
            f"""
            SELECT COUNT(DISTINCT a.id_beneficiario)
            FROM autorizacao a
            LEFT JOIN beneficiario b USING (id_beneficiario)
            WHERE {where_clause}
            """,
            params,
        ).fetchone()

        # Quantidade de autorizações no mês (com os mesmos filtros)
        (qt_autorizacoes,) = c.execute(
            f"""
            SELECT COUNT(*)
            FROM autorizacao a
            LEFT JOIN beneficiario b USING (id_beneficiario)
            WHERE {where_clause}
            """,
            params,
        ).fetchone()

        filtros_aplicados: Dict[str, str] = {}
        if produto: filtros_aplicados["produto"] = produto
        if uf: filtros_aplicados["uf"] = uf
        if cidade: filtros_aplicados["cidade"] = cidade
        if sexo: filtros_aplicados["sexo"] = sexo
        if faixa: filtros_aplicados["faixa"] = faixa

        return {
            "competencia": competencia,
            "beneficiarios_base": int(base_total or 0),
            "beneficiarios_utilizados": int(utilizados or 0),
            "autorizacoes": int(qt_autorizacoes or 0),
            "filtros_aplicados": filtros_aplicados,
        }
