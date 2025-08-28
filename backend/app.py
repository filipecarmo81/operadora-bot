# backend/app.py
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional, Dict, Any
import os
import duckdb

APP_TITLE = "Operadora KPIs"
APP_VERSION = "0.3.0"

# Caminho do banco (build gera em backend/data/operadora.duckdb)
DB_CANDIDATES = [
    "/opt/render/project/src/backend/data/operadora.duckdb",   # Render
    "backend/data/operadora.duckdb",                           # local (repo)
    "data/operadora.duckdb",                                   # fallback
]

def db_path() -> str:
    for p in DB_CANDIDATES:
        if os.path.exists(p):
            return p
    # ainda assim devolve o primeiro para a mensagem de health
    return DB_CANDIDATES[0]

def con_ro():
    # read_only=True evita lock em produção
    return duckdb.connect(db_path(), read_only=True)

def show_tables(c) -> List[str]:
    # mais robusto que PRAGMA em ambientes diferentes
    rows = c.execute("SELECT table_name FROM duckdb_information_schema.tables WHERE database_name=current_database() ORDER BY 1").fetchall()
    return [r[0] for r in rows]

def table_cols(c, table: str) -> List[str]:
    rows = c.execute(f"PRAGMA table_info('{table}')").fetchall()
    # esquema: [cid, name, type, notnull, dflt, pk]
    return [r[1] for r in rows]

def first_present(cols: List[str], candidates: List[str]) -> Optional[str]:
    for name in candidates:
        if name in cols:
            return name
    return None

def month_sql(col: str) -> str:
    """
    Normaliza qualquer coluna de data para 'YYYY-MM'.
    Suporta 'YYYY-MM', 'YYYY-MM-DD', 'DD/MM/YYYY', 'YYYYMMDD' e DATE nativo.
    """
    return (
        "strftime(COALESCE("
        f"try_strptime({col}, '%Y-%m-%d'),"
        f"try_strptime({col}, '%Y-%m'),"
        f"try_strptime({col}, '%d/%m/%Y'),"
        f"try_strptime({col}, '%Y%m%d'),"
        f"CAST({col} AS DATE)"
        "), '%Y-%m')"
    )

def ensure_cols_or_400(c, table: str, needed: Dict[str, List[str]]) -> Dict[str, str]:
    """
    Garante que as colunas (data/valor) existam na tabela.
    Retorna o mapeamento real { logical_name: real_col }.
    Levanta 400 com mensagem amigável caso não ache.
    """
    cols = set(table_cols(c, table))
    resolved = {}
    for logical, candidates in needed.items():
        real = first_present(list(cols), candidates)
        if not real:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Não encontrei {logical.upper()} em '{table}'. "
                    f"Tente uma destas colunas: {candidates}. "
                    f"Colunas disponíveis: {sorted(cols)}"
                ),
            )
        resolved[logical] = real
    return resolved

app = FastAPI(title=APP_TITLE, version=APP_VERSION)

# --------- CORS (resolve 'Failed to fetch' no front) ----------
ALLOWED_ORIGINS = [
    "https://operadora-bot.onrender.com",    # backend (para testes diretos)
    "https://operadora-bot-1.onrender.com",  # seu frontend Render
    "http://localhost:5173",                 # dev Vite
    "http://127.0.0.1:5173",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------- Raiz & Health ---------------------------
@app.get("/")
def root():
    with con_ro() as c:
        return {
            "ok": True,
            "message": "API do Operadora Bot. Use /docs para testar.",
            "db": db_path(),
            "tables": show_tables(c),
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

@app.get("/health")
def health():
    with con_ro() as c:
        return {
            "ok": True,
            "db": db_path(),
            "tables": show_tables(c),
        }

@app.get("/debug/cols")
def debug_cols(table: Optional[str] = None):
    with con_ro() as c:
        if table:
            return {table: table_cols(c, table)}
        return {t: table_cols(c, t) for t in show_tables(c)}

# ----------------- SINISTRALIDADE -----------------------------
def sinistralidade_do_mes(c, competencia: str) -> Dict[str, Any]:
    """
    Sinistralidade = sum(conta.vl_liberado) / sum(mensalidade.vl_premio) no mês (YYYY-MM)
    """
    # conta: data + vl_liberado
    conta_map = ensure_cols_or_400(
        c,
        "conta",
        {
            "data": ["dt_mes_competencia", "dt_competencia", "competencia", "dt_apresentada", "dt_conta"],
            "valor": ["vl_liberado"],
        },
    )
    mens_map = ensure_cols_or_400(
        c,
        "mensalidade",
        {
            "data": ["dt_competencia", "dt_mes_competencia", "competencia"],
            "valor": ["vl_premio"],
        },
    )

    mes_conta = month_sql(conta_map["data"])
    mes_mens  = month_sql(mens_map["data"])

    sinistro = c.execute(
        f"SELECT COALESCE(SUM({conta_map['valor']}), 0)::DOUBLE "
        f"FROM conta WHERE {mes_conta} = ?", [competencia]
    ).fetchone()[0]

    receita = c.execute(
        f"SELECT COALESCE(SUM({mens_map['valor']}), 0)::DOUBLE "
        f"FROM mensalidade WHERE {mes_mens} = ?", [competencia]
    ).fetchone()[0]

    sinistralidade = (sinistro / receita) if receita else 0.0

    return {
        "competencia": competencia,
        "sinistro": sinistro,
        "receita": receita,
        "sinistralidade": sinistralidade,
    }

def competencia_ultima(c) -> str:
    # maior mês existente em conta ou mensalidade
    conta_map = ensure_cols_or_400(
        c, "conta", {"data": ["dt_mes_competencia", "dt_competencia", "competencia", "dt_apresentada", "dt_conta"]}
    )
    mens_map = ensure_cols_or_400(
        c, "mensalidade", {"data": ["dt_competencia", "dt_mes_competencia", "competencia"]}
    )
    mes_conta = month_sql(conta_map["data"])
    mes_mens  = month_sql(mens_map["data"])
    row = c.execute(
        f"""
        WITH meses AS (
          SELECT {mes_conta} AS mes FROM conta
          UNION
          SELECT {mes_mens}  AS mes FROM mensalidade
        )
        SELECT MAX(mes) FROM meses
        """
    ).fetchone()
    if not row or not row[0]:
        raise HTTPException(status_code=400, detail="Não encontrei meses em 'conta' ou 'mensalidade'.")
    return row[0]

@app.get("/kpi/sinistralidade/competencia")
def sinistralidade_competencia(competencia: str = Query(..., pattern=r"^\d{4}-\d{2}$")):
    with con_ro() as c:
        return sinistralidade_do_mes(c, competencia)

@app.get("/kpi/sinistralidade/ultima")
def sinistralidade_ultima():
    with con_ro() as c:
        comp = competencia_ultima(c)
        return sinistralidade_do_mes(c, comp)

@app.get("/kpi/sinistralidade/media")
def sinistralidade_media(ultimos: int = 6):
    with con_ro() as c:
        # pega últimos N meses disponíveis (união conta/mensalidade)
        conta_map = ensure_cols_or_400(
            c, "conta", {"data": ["dt_mes_competencia", "dt_competencia", "competencia", "dt_apresentada", "dt_conta"]}
        )
        mens_map = ensure_cols_or_400(
            c, "mensalidade", {"data": ["dt_competencia", "dt_mes_competencia", "competencia"]}
        )
        mes_conta = month_sql(conta_map["data"])
        mes_mens  = month_sql(mens_map["data"])

        meses = [r[0] for r in c.execute(
            f"""
            WITH meses AS (
              SELECT DISTINCT {mes_conta} AS mes FROM conta
              UNION
              SELECT DISTINCT {mes_mens}  AS mes FROM mensalidade
            )
            SELECT mes FROM meses WHERE mes IS NOT NULL ORDER BY mes DESC LIMIT ?
            """, [ultimos]
        ).fetchall()]

        if not meses:
            raise HTTPException(status_code=400, detail="Não encontrei meses para calcular a média.")

        pontos = [sinistralidade_do_mes(c, m) for m in meses]
        media = sum(p["sinistralidade"] for p in pontos) / len(pontos)
        return {"meses": meses, "media": media, "observacoes": len(meses)}

# ----------------- PRESTADOR (impacto / top) ------------------
@app.get("/kpi/prestador/impacto")
def prestador_impacto(competencia: str = Query(..., pattern=r"^\d{4}-\d{2}$"), top: int = 10):
    with con_ro() as c:
        conta_map = ensure_cols_or_400(
            c,
            "conta",
            {
                "data": ["dt_mes_competencia", "dt_competencia", "competencia", "dt_apresentada", "dt_conta"],
                "valor": ["vl_liberado"],
                # tentamos várias chaves de prestador
                "prest": ["id_prestador_pagamento", "id_prestador_envio", "id_prestador"],
            },
        )
        prest_cols = table_cols(c, "prestador")
        nm_col = first_present(prest_cols, ["nm_prestador", "nome", "nm_razao", "ds_prestador"]) or "nm_prestador"
        # caso não exista, criaremos um nome genérico no SELECT

        mes_conta = month_sql(conta_map["data"])
        sql = f"""
            SELECT 
              c.{conta_map['prest']} AS id_prestador,
              COALESCE(p.{nm_col}, 'Prestador ' || CAST(c.{conta_map['prest']} AS VARCHAR)) AS nome,
              SUM(c.{conta_map['valor']})::DOUBLE AS score
            FROM conta c
            LEFT JOIN prestador p ON p.id_prestador = c.{conta_map['prest']}
            WHERE {mes_conta} = ?
            GROUP BY 1,2
            ORDER BY score DESC
            LIMIT ?
        """
        rows = c.execute(sql, [competencia, top]).fetchall()
        return {"competencia": competencia, "top": [{"id_prestador": r[0], "nome": r[1], "score": r[2]} for r in rows]}

@app.get("/kpi/prestador/top")
def prestador_top(competencia: str = Query(..., pattern=r"^\d{4}-\d{2}$"), limite: int = 10):
    # alias para compatibilidade
    return prestador_impacto(competencia, limite)

# ----------------- UTILIZAÇÃO (resumo simples) ----------------
@app.get("/kpi/utilizacao/resumo")
def utilizacao_resumo(
    competencia: str = Query(..., pattern=r"^\d{4}-\d{2}$"),
    produto: Optional[str] = None,
    uf: Optional[str] = None,
    cidade: Optional[str] = None,
    sexo: Optional[str] = None,
    faixa: Optional[str] = None,
):
    """
    Resumo minimalista para já funcionar:
      - beneficiarios_base: total em 'beneficiario'
      - beneficiarios_utilizados: distintos em 'autorizacao' no mês
      - autorizacoes: total em 'autorizacao' no mês

    (Filtros opcionais estão reservados para a próxima iteração,
     porque dependem do mapeamento exato de colunas no seu CSV.)
    """
    with con_ro() as c:
        # Coluna de data na autorizacao
        aut_map = ensure_cols_or_400(
            c,
            "autorizacao",
            {"data": ["dt_autorizacao", "dt_entrada", "dt_solicitacao", "dt_guia"]},
        )
        mes_aut = month_sql(aut_map["data"])

        beneficiarios_base = c.execute("SELECT COUNT(*)::BIGINT FROM beneficiario").fetchone()[0]
        beneficiarios_utilizados = c.execute(
            f"SELECT COUNT(DISTINCT id_beneficiario)::BIGINT FROM autorizacao WHERE {mes_aut} = ?",
            [competencia],
        ).fetchone()[0]
        autorizacoes = c.execute(
            f"SELECT COUNT(*)::BIGINT FROM autorizacao WHERE {mes_aut} = ?",
            [competencia],
        ).fetchone()[0]

        return {
            "competencia": competencia,
            "beneficiarios_base": int(beneficiarios_base),
            "beneficiarios_utilizados": int(beneficiarios_utilizados),
            "autorizacoes": int(autorizacoes),
            "filtros_aplicados": {},  # preenchermos quando ativarmos filtros
        }
