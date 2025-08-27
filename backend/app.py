# app.py
# FastAPI + DuckDB (Operadora KPIs)

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict, Optional, Tuple
import duckdb
import re

# -----------------------------------------------------------------------------
# Configurações básicas
# -----------------------------------------------------------------------------

app = FastAPI(title="Operadora KPIs", version="0.2.0")

# CORS amplo: como não usamos cookies no fetch, allow_credentials=False permite "*"
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=False,
)

# >>> Atenção ao caminho do banco no Render <<<
DB_PATH = "backend/data/operadora.duckdb"  # não use 'backend/backend/...'

# -----------------------------------------------------------------------------
# Utilitários
# -----------------------------------------------------------------------------

def open_con(read_only: bool = True):
    try:
        return duckdb.connect(DB_PATH, read_only=read_only)
    except Exception as e:
        raise HTTPException(500, f"Falha ao abrir DuckDB: {e}")

def get_cols(con, table: str) -> List[str]:
    try:
        rows = con.execute(f"PRAGMA table_info('{table}')").fetchall()
        return [r[1].lower() for r in rows]  # nome da coluna em [1]
    except Exception:
        return []

def find_col(con, table: str, candidates: List[str], obrigatoria: bool = True) -> Optional[str]:
    cols = set(get_cols(con, table))
    for c in candidates:
        if c.lower() in cols:
            return c
    if obrigatoria:
        raise HTTPException(
            400,
            f"Não encontrei nenhuma das colunas {candidates} em '{table}'. "
            f"Colunas disponíveis: {sorted(cols)}"
        )
    return None

def month_bounds_str(competencia: str) -> Tuple[str, str]:
    """
    Retorna (ini, fim) como strings 'YYYY-MM-01' e 'YYYY-MM-01 + 1 month'
    """
    if not re.fullmatch(r"\d{4}-\d{2}", competencia or ""):
        raise HTTPException(400, "competencia deve estar no formato YYYY-MM")
    ini = f"{competencia}-01"
    fim = f"{competencia}-01"
    return ini, fim  # usaremos + INTERVAL 1 MONTH no SQL

def sanitize_like(s: str) -> str:
    # evita quebrar o LIKE
    return (s or "").replace("'", "''")

def parse_faixas(s: Optional[str]) -> List[Tuple[Optional[int], Optional[int]]]:
    """
    Converte '0-18, 19-59, 60+' em [(0,18), (19,59), (60,None)]
    """
    if not s:
        return []
    parts = [p.strip() for p in s.split(",")]
    out: List[Tuple[Optional[int], Optional[int]]] = []
    for p in parts:
        if not p:
            continue
        m = re.match(r"^(\d+)\s*-\s*(\d+)$", p)
        if m:
            out.append((int(m.group(1)), int(m.group(2))))
            continue
        m = re.match(r"^(\d+)\s*\+$", p)
        if m:
            out.append((int(m.group(1)), None))
            continue
        m = re.match(r"^(\d+)\s*$", p)
        if m:
            a = int(m.group(1))
            out.append((a, a))
            continue
    return out

# -----------------------------------------------------------------------------
# HEALTH
# -----------------------------------------------------------------------------

@app.get("/health")
def health():
    con = open_con()
    try:
        tables = [r[0] for r in con.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema='main'
            ORDER BY table_name
        """).fetchall()]
        info: Dict[str, int] = {}
        for t in tables:
            try:
                n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            except Exception:
                n = -1
            info[t] = int(n)
        return {"status": "ok", "tabelas": info}
    finally:
        con.close()

# -----------------------------------------------------------------------------
# SINISTRALIDADE
# -----------------------------------------------------------------------------

@app.get("/kpi/sinistralidade/ultima")
def kpi_sin_ultima():
    con = open_con()
    try:
        # Tabelas/colunas necessárias
        col_dt_comp_m = find_col(con, "mensalidade", ["dt_competencia"])
        col_receita   = find_col(con, "mensalidade", ["vl_premio", "vl_sca", "vl_receita"])

        # maior competência existente em mensalidade
        comp = con.execute(f"""
            SELECT strftime('%Y-%m', max({col_dt_comp_m})) FROM mensalidade
        """).fetchone()[0]
        if not comp:
            return {"competencia": None, "sinistro": 0.0, "receita": 0.0, "sinistralidade": 0.0}

        ini, _ = month_bounds_str(comp)

        # Localiza colunas em 'conta'
        col_dt_conta = find_col(con, "conta", ["dt_conta_item", "dt_mes_competencia", "dt_competencia"])
        # >>> Se a regra de negócio usar 'vl_apresentado', troque o nome abaixo:
        col_vl_conta = find_col(con, "conta", ["vl_liberado", "vl_apresentado"])

        data = con.execute(f"""
            WITH bounds AS (
                SELECT DATE '{ini}' AS ini, DATE '{ini}' + INTERVAL 1 MONTH AS fim
            ),
            s AS (
                SELECT COALESCE(SUM({col_vl_conta}),0) AS sinistro
                FROM conta, bounds
                WHERE {col_dt_conta} >= ini AND {col_dt_conta} < fim
            ),
            r AS (
                SELECT COALESCE(SUM({col_receita}),0) AS receita
                FROM mensalidade, bounds
                WHERE {col_dt_comp_m} >= ini AND {col_dt_comp_m} < fim
            )
            SELECT s.sinistro, r.receita FROM s, r
        """).fetchone()

        sinistro = float(data[0] or 0)
        receita  = float(data[1] or 0)
        idx = sinistro / receita if receita else 0.0
        return {"competencia": comp, "sinistro": sinistro, "receita": receita, "sinistralidade": idx}
    finally:
        con.close()

@app.get("/kpi/sinistralidade/media")
def kpi_sin_media(janela_meses: int = Query(12, ge=1, le=60)):
    con = open_con()
    try:
        col_dt_comp_m = find_col(con, "mensalidade", ["dt_competencia"])
        col_receita   = find_col(con, "mensalidade", ["vl_premio", "vl_sca", "vl_receita"])

        # fim = maior dt_competencia
        fim = con.execute(f"SELECT max({col_dt_comp_m}) FROM mensalidade").fetchone()[0]
        if not fim:
            return {"competencia": None, "sinistro": 0.0, "receita": 0.0, "sinistralidade": 0.0}

        col_dt_conta = find_col(con, "conta", ["dt_conta_item", "dt_mes_competencia", "dt_competencia"])
        col_vl_conta = find_col(con, "conta", ["vl_liberado", "vl_apresentado"])

        row = con.execute(f"""
            WITH win AS (
                SELECT DATE '{fim}' + INTERVAL 1 DAY - date_part('day','{fim}')::INT * INTERVAL 1 DAY AS fim_m,
                       (DATE '{fim}' + INTERVAL 1 DAY - date_part('day','{fim}')::INT * INTERVAL 1 DAY) - INTERVAL {janela_meses} MONTH AS ini_m
            ),
            s AS (
                SELECT COALESCE(SUM({col_vl_conta}),0) AS sinistro
                FROM conta, win
                WHERE {col_dt_conta} >= ini_m AND {col_dt_conta} < fim_m
            ),
            r AS (
                SELECT COALESCE(SUM({col_receita}),0) AS receita
                FROM mensalidade, win
                WHERE {col_dt_comp_m} >= ini_m AND {col_dt_comp_m} < fim_m
            )
            SELECT strftime('%Y-%m', DATE '{fim}'), s.sinistro, r.receita
            FROM s, r
        """).fetchone()

        comp     = row[0]
        sinistro = float(row[1] or 0)
        receita  = float(row[2] or 0)
        idx = sinistro / receita if receita else 0.0
        return {"competencia": comp, "sinistro": sinistro, "receita": receita, "sinistralidade": idx}
    finally:
        con.close()

# -----------------------------------------------------------------------------
# TOP PRESTADOR (impacto)
# -----------------------------------------------------------------------------

@app.get("/kpi/prestador/top")
def kpi_prestador_top(competencia: str, limite: int = Query(10, ge=1, le=100)):
    con = open_con()
    try:
        ini, _ = month_bounds_str(competencia)

        # colunas em conta
        col_dt_conta = find_col(con, "conta", ["dt_conta_item", "dt_mes_competencia", "dt_competencia"])
        col_vl_conta = find_col(con, "conta", ["vl_liberado", "vl_apresentado"])
        col_id_prest_conta = find_col(con, "conta", ["id_prestador", "id_prestador_pagamento", "id_prestador_envio"])

        # colunas em prestador
        col_id_prest = find_col(con, "prestador", ["id_prestador"])
        col_nm_prest = find_col(con, "prestador", ["nm_prestador", "nome", "ds_prestador", "razao_social"])

        rows = con.execute(f"""
            WITH bounds AS (SELECT DATE '{ini}' AS ini, DATE '{ini}' + INTERVAL 1 MONTH AS fim)
            SELECT
                c.{col_id_prest_conta} AS id_prestador,
                p.{col_nm_prest}       AS nome,
                COALESCE(SUM(c.{col_vl_conta}),0) AS score
            FROM conta c
            JOIN bounds b ON 1=1
            LEFT JOIN prestador p
              ON p.{col_id_prest} = c.{col_id_prest_conta}
            WHERE c.{col_dt_conta} >= b.ini AND c.{col_dt_conta} < b.fim
            GROUP BY 1,2
            ORDER BY score DESC NULLS LAST
            LIMIT {limite}
        """).fetchall()

        data = [{"id_prestador": int(r[0]) if r[0] is not None else None,
                 "nome": r[1],
                 "score": float(r[2] or 0)} for r in rows]
        return {"competencia": competencia, "top": data}
    finally:
        con.close()

# -----------------------------------------------------------------------------
# UTILIZAÇÃO — RESUMO COM FILTROS
# -----------------------------------------------------------------------------

@app.get("/kpi/utilizacao/resumo")
def kpi_utilizacao_resumo(
    competencia: str,
    produto: Optional[str] = None,   # cd_item ou ds_item (conta)
    uf: Optional[str] = None,        # prestador.uf / sg_uf
    cidade: Optional[str] = None,    # prestador.cidade / nm_municipio
    sexo: Optional[str] = None,      # beneficiario.sexo / ds_sexo / sg_sexo
    faixa: Optional[str] = None      # "0-18, 19-59, 60+"
):
    con = open_con()
    try:
        ini, _ = month_bounds_str(competencia)

        # --- beneficiário (base e atributos) ---
        col_id_benef = find_col(con, "beneficiario", ["id_beneficiario"])
        col_dt_nasc  = find_col(con, "beneficiario", ["dt_nascimento"], obrigatoria=False)
        col_sexo     = find_col(con, "beneficiario", ["sexo", "ds_sexo", "sg_sexo"], obrigatoria=False)

        base_total = con.execute(f"SELECT COUNT(*) FROM beneficiario").fetchone()[0] or 0

        # --- autorizacao (evento de utilização) ---
        col_id_benef_aut = find_col(con, "autorizacao", ["id_beneficiario"])
        col_dt_aut       = find_col(con, "autorizacao", ["dt_autorizacao", "dt_competencia"])
        col_cd_item      = find_col(con, "autorizacao", ["cd_item"], obrigatoria=False)
        col_ds_item      = find_col(con, "autorizacao", ["ds_item"], obrigatoria=False)

        # --- prestador (filtros geográficos) ---
        col_id_prest_aut = find_col(con, "autorizacao", ["id_prestador"], obrigatoria=False)
        col_id_prest     = find_col(con, "prestador", ["id_prestador"], obrigatoria=False)
        col_uf           = find_col(con, "prestador", ["uf", "sg_uf"], obrigatoria=False)
        col_cidade       = find_col(con, "prestador", ["cidade", "nm_municipio"], obrigatoria=False)

        filtros_sql = ["a.{dt} >= b.ini AND a.{dt} < b.fim".format(dt=col_dt_aut)]
        joins = []
        params = []

        # Produto (código ou nome contém)
        if produto and (col_cd_item or col_ds_item):
            like = f"%{sanitize_like(produto)}%"
            sub = []
            if col_cd_item:
                sub.append(f"a.{col_cd_item} LIKE '{like}'")
            if col_ds_item:
                sub.append(f"a.{col_ds_item} LIKE '{like}'")
            if sub:
                filtros_sql.append("(" + " OR ".join(sub) + ")")

        # Join com prestador se necessário e se existir mapeamento
        if (uf or cidade) and col_id_prest_aut and col_id_prest and (col_uf or col_cidade):
            joins.append(f"LEFT JOIN prestador p ON p.{col_id_prest} = a.{col_id_prest_aut}")
            if uf and col_uf:
                ufs = [u.strip().upper() for u in uf.split(",") if u.strip()]
                if ufs:
                    lista = ",".join([f"'{sanitize_like(x)}'" for x in ufs])
                    filtros_sql.append(f"UPPER(p.{col_uf}) IN ({lista})")
            if cidade and col_cidade:
                # match via LIKE em alguma(s) cidades
                cids = [c.strip() for c in cidade.split(",") if c.strip()]
                if cids:
                    esc = " OR ".join([f"UPPER(p.{col_cidade}) LIKE UPPER('%{sanitize_like(c)}%')" for c in cids])
                    filtros_sql.append("(" + esc + ")")

        # Join com beneficiario para sexo/idade
        precisa_benef = (sexo is not None) or (faixa is not None)
        if precisa_benef:
            joins.append(f"LEFT JOIN beneficiario b ON b.{col_id_benef} = a.{col_id_benef_aut}")
            if sexo and col_sexo:
                filtros_sql.append(f"UPPER(b.{col_sexo}) = UPPER('{sanitize_like(sexo)}')")
            if faixa and col_dt_nasc:
                faixas = parse_faixas(faixa)
                if faixas:
                    # idade ao 1º dia do mês
                    conds = []
                    for (mi, ma) in faixas:
                        if mi is not None and ma is not None:
                            conds.append(
                                f"(date_diff('year', b.{col_dt_nasc}, DATE '{ini}') BETWEEN {mi} AND {ma})"
                            )
                        elif mi is not None and ma is None:
                            conds.append(
                                f"(date_diff('year', b.{col_dt_nasc}, DATE '{ini}') >= {mi})"
                            )
                    if conds:
                        filtros_sql.append("(" + " OR ".join(conds) + ")")

        where = " AND ".join(filtros_sql)
        join_sql = "\n".join(joins)

        rows = con.execute(f"""
            WITH bounds AS (SELECT DATE '{ini}' AS ini, DATE '{ini}' + INTERVAL 1 MONTH AS fim)
            SELECT
                COUNT(*)                                     AS autorizacoes,
                COUNT(DISTINCT a.{col_id_benef_aut})         AS beneficiarios_utilizados
            FROM autorizacao a
            JOIN bounds b ON 1=1
            {join_sql}
            WHERE {where}
        """).fetchone()

        autoriz = int(rows[0] or 0)
        util    = int(rows[1] or 0)

        return {
            "competencia": competencia,
            "beneficiarios_base": int(base_total),
            "beneficiarios_utilizados": int(util),
            "autorizacoes": int(autoriz),
            "filtros_aplicados": {
                k: v for k, v in dict(
                    produto=produto, uf=uf, cidade=cidade, sexo=sexo, faixa=faixa
                ).items() if v
            }
        }
    finally:
        con.close()

# -----------------------------------------------------------------------------
# UTILIZAÇÃO — EVOLUÇÃO (série mensal)
# -----------------------------------------------------------------------------

@app.get("/kpi/utilizacao/evolucao")
def kpi_utilizacao_evolucao(
    meses: int = Query(12, ge=1, le=60),
    produto: Optional[str] = None
):
    con = open_con()
    try:
        col_dt_aut       = find_col(con, "autorizacao", ["dt_autorizacao", "dt_competencia"])
        col_cd_item      = find_col(con, "autorizacao", ["cd_item"], obrigatoria=False)
        col_ds_item      = find_col(con, "autorizacao", ["ds_item"], obrigatoria=False)

        filtro_prod = ""
        if produto and (col_cd_item or col_ds_item):
            like = f"%{sanitize_like(produto)}%"
            sub = []
            if col_cd_item:
                sub.append(f"{col_cd_item} LIKE '{like}'")
            if col_ds_item:
                sub.append(f"{col_ds_item} LIKE '{like}'")
            if sub:
                filtro_prod = " AND (" + " OR ".join(sub) + ")"

        rows = con.execute(f"""
            WITH maxd AS (SELECT max({col_dt_aut}) AS mx FROM autorizacao),
            rng AS (
                SELECT generate_series(
                    DATE_TRUNC('month', mx) - INTERVAL {meses-1} MONTH,
                    DATE_TRUNC('month', mx),
                    INTERVAL 1 MONTH
                ) AS m
                FROM maxd
            )
            SELECT
                strftime('%Y-%m', r.m) AS competencia,
                COUNT(a.*) AS autorizacoes
            FROM rng r
            LEFT JOIN autorizacao a
              ON DATE_TRUNC('month', a.{col_dt_aut}) = r.m
             {filtro_prod}
            GROUP BY 1
            ORDER BY 1
        """).fetchall()

        dados = [{"competencia": r[0], "autorizacoes": int(r[1] or 0)} for r in rows]
        return {"meses": meses, "serie": dados}
    finally:
        con.close()
