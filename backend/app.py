# app.py
import os
import json
from datetime import datetime, date
from typing import List, Optional, Dict, Any

import duckdb
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, Response

# ------------------------------------------------------------------------------
# Config de DB: tenta encontrar o arquivo em caminhos comuns
# ------------------------------------------------------------------------------
CANDIDATE_DB_PATHS = [
    "/opt/render/project/src/backend/data/operadora.duckdb",
    "backend/data/operadora.duckdb",
    "data/operadora.duckdb",
    "./operadora.duckdb",
]

def resolve_db_path() -> str:
    for p in CANDIDATE_DB_PATHS:
        if os.path.exists(p):
            return p
    # Se não achar, devolve o primeiro (para mensagem de erro coerente)
    return CANDIDATE_DB_PATHS[0]

DB_PATH = resolve_db_path()

# ------------------------------------------------------------------------------
# FastAPI
# ------------------------------------------------------------------------------
app = FastAPI(title="Operadora KPIs", version="0.2.0")

# CORS liberado (ajuste origins se quiser restringir)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------------------------------------------------------------------
# Utilitários de DB/metadata
# ------------------------------------------------------------------------------
def open_con() -> duckdb.DuckDBPyConnection:
    try:
        return duckdb.connect(DB_PATH, read_only=True)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Falha ao abrir DuckDB: {e}"
        )

def get_tables(con) -> List[str]:
    rows = con.execute("SHOW TABLES").fetchall()
    return [r[0] for r in rows]

def get_cols(con, table: str) -> List[str]:
    try:
        rows = con.execute(f"PRAGMA table_info('{table}')").fetchall()
    except Exception:
        rows = []
    # PRAGMA table_info -> (cid, name, type, notnull, dflt_value, pk)
    return [r[1] for r in rows]

def find_col(con, table: str, candidates: List[str]) -> str:
    """
    Procura uma coluna que exista em 'table' dentre um conjunto de nomes
    candidatos (variações comuns). Lança 400 se não achar.
    """
    cols = set(get_cols(con, table))
    for c in candidates:
        if c in cols:
            return c
    # tenta equivalências simples (lowercase)
    cols_lower = {c.lower(): c for c in cols}
    for c in candidates:
        if c.lower() in cols_lower:
            return cols_lower[c.lower()]
    raise HTTPException(
        status_code=400,
        detail=f"Não encontrei {', '.join(candidates)} em '{table}'. "
               f"Colunas disponíveis: {sorted(list(cols))}"
    )

def parse_comp_yyyy_mm(comp: str) -> date:
    try:
        return datetime.strptime(comp, "%Y-%m").date().replace(day=1)
    except Exception:
        raise HTTPException(
            status_code=400,
            detail="Parâmetro 'competencia' deve estar no formato YYYY-MM."
        )

# ------------------------------------------------------------------------------
# Rotas utilitárias
# ------------------------------------------------------------------------------
@app.get("/")
def root():
    return RedirectResponse(url="/docs")

@app.get("/favicon.ico")
def favicon():
    # evita 404 no favicon nos logs
    return Response(status_code=204)

# ------------------------------------------------------------------------------
# /health
# ------------------------------------------------------------------------------
@app.get("/health")
def health():
    con = open_con()
    try:
        data = {}
        for t in get_tables(con):
            try:
                cnt = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            except Exception:
                cnt = None
            data[t] = cnt
        return {
            "db_path": DB_PATH,
            "tables": data
        }
    finally:
        con.close()

# ------------------------------------------------------------------------------
# SINISTRALIDADE: última e média
# ------------------------------------------------------------------------------
@app.get("/kpi/sinistralidade/ultima")
def kpi_sin_ultima():
    con = open_con()
    try:
        # mensalidade
        col_dt_comp_m = find_col(con, "mensalidade", ["dt_competencia", "dt_mes_competencia"])
        col_receita   = find_col(con, "mensalidade", ["vl_premio", "vl_sca", "vl_receita"])

        # conta (sinistro)
        col_dt_conta = find_col(con, "conta", ["dt_conta_item", "dt_mes_competencia", "dt_competencia"])
        # ajuste a ordem se sua regra de negócio considerar 'apresentado'
        col_vl_conta = find_col(con, "conta", ["vl_liberado", "vl_apresentado"])

        row = con.execute(f"""
            WITH m AS (
                SELECT date_trunc('month', max({col_dt_comp_m})) AS m
                FROM mensalidade
            )
            SELECT
                strftime('%Y-%m', m.m) AS competencia,
                (SELECT COALESCE(SUM({col_vl_conta}),0)
                   FROM conta
                  WHERE date_trunc('month', {col_dt_conta}) = m.m) AS sinistro,
                (SELECT COALESCE(SUM({col_receita}),0)
                   FROM mensalidade
                  WHERE date_trunc('month', {col_dt_comp_m}) = m.m) AS receita
            FROM m
        """).fetchone()

        if not row or not row[0]:
            return {"competencia": None, "sinistro": 0.0, "receita": 0.0, "sinistralidade": 0.0}

        comp     = row[0]
        sinistro = float(row[1] or 0)
        receita  = float(row[2] or 0)
        idx = sinistro / receita if receita else 0.0
        return {"competencia": comp, "sinistro": sinistro, "receita": receita, "sinistralidade": idx}
    finally:
        con.close()

@app.get("/kpi/sinistralidade/media")
def kpi_sin_media(meses: int = 6):
    if meses <= 0 or meses > 60:
        raise HTTPException(status_code=400, detail="Param 'meses' deve estar entre 1 e 60.")
    con = open_con()
    try:
        col_dt_comp_m = find_col(con, "mensalidade", ["dt_competencia", "dt_mes_competencia"])
        col_receita   = find_col(con, "mensalidade", ["vl_premio", "vl_sca", "vl_receita"])
        col_dt_conta  = find_col(con, "conta", ["dt_conta_item", "dt_mes_competencia", "dt_competencia"])
        col_vl_conta  = find_col(con, "conta", ["vl_liberado", "vl_apresentado"])

        rows = con.execute(f"""
            WITH meses AS (
                SELECT DISTINCT date_trunc('month', {col_dt_comp_m}) AS m
                FROM mensalidade
            ), ult AS (
                SELECT m FROM meses
                ORDER BY m DESC
                LIMIT {meses}
            )
            SELECT
                strftime('%Y-%m', u.m) AS competencia,
                (SELECT COALESCE(SUM({col_vl_conta}),0) FROM conta
                  WHERE date_trunc('month', {col_dt_conta}) = u.m) AS sinistro,
                (SELECT COALESCE(SUM({col_receita}),0) FROM mensalidade
                  WHERE date_trunc('month', {col_dt_comp_m}) = u.m) AS receita
            FROM ult u
            ORDER BY 1
        """).fetchall()

        serie = []
        num = 0
        den = 0
        for comp, sin, rec in rows:
            sin = float(sin or 0)
            rec = float(rec or 0)
            serie.append({"competencia": comp, "sinistro": sin, "receita": rec,
                          "sinistralidade": (sin / rec if rec else 0.0)})
            num += sin
            den += rec
        media = (num / den) if den else 0.0
        return {"meses": meses, "media": media, "series": serie}
    finally:
        con.close()

# ------------------------------------------------------------------------------
# PRESTADOR: impacto/top
# ------------------------------------------------------------------------------
@app.get("/kpi/prestador/top")
def kpi_prestador_top(competencia: str, limite: int = 10):
    if limite <= 0 or limite > 1000:
        raise HTTPException(status_code=400, detail="Param 'limite' inválido.")
    comp_dt = parse_comp_yyyy_mm(competencia)

    con = open_con()
    try:
        # conta / prestador
        col_dt_conta = find_col(con, "conta", ["dt_conta_item", "dt_mes_competencia", "dt_competencia"])
        col_vl       = find_col(con, "conta", ["vl_liberado", "vl_apresentado"])
        col_id_p1    = None
        try:
            col_id_p1 = find_col(con, "conta", ["id_prestador_pagamento", "id_prestador_envio"])
        except HTTPException:
            # tenta outra
            col_id_p1 = find_col(con, "conta", ["id_prestador_envio"])

        col_id_prest = find_col(con, "prestador", ["id_prestador"])
        col_nm       = find_col(con, "prestador", ["nm_prestador", "nome", "ds_prestador"])

        rows = con.execute(f"""
            WITH sel AS (
              SELECT {col_id_p1} AS id_prestador, COALESCE({col_vl},0) AS valor
              FROM conta
              WHERE date_trunc('month', {col_dt_conta}) = date '{comp_dt}'
            )
            SELECT s.id_prestador,
                   p.{col_nm} AS nome,
                   SUM(s.valor) AS score
            FROM sel s
            LEFT JOIN prestador p ON p.{col_id_prest} = s.id_prestador
            GROUP BY 1,2
            ORDER BY 3 DESC NULLS LAST
            LIMIT {limite}
        """).fetchall()

        top = [{"id_prestador": r[0], "nome": r[1], "score": float(r[2] or 0)} for r in rows]
        return {"competencia": competencia, "top": top}
    finally:
        con.close()

# Alias compatível com versão antiga
@app.get("/kpi/prestador/impacto")
def kpi_prestador_impacto(competencia: str, top: int = 10):
    return kpi_prestador_top(competencia=competencia, limite=top)

# ------------------------------------------------------------------------------
# FAIXA/CUSTO no mês
# ------------------------------------------------------------------------------
@app.get("/kpi/faixa/custo")
def kpi_faixa_custo(competencia: str,
                    faixas: str = Query("0-18,19-59,60+",
                                        description="Lista ex.: '0-18,19-59,60+'")):
    comp_dt = parse_comp_yyyy_mm(competencia)
    grupos = [f.strip() for f in faixas.split(",") if f.strip()]
    if not grupos:
        raise HTTPException(status_code=400, detail="Informe pelo menos uma faixa.")

    con = open_con()
    try:
        # conta
        col_dt_conta = find_col(con, "conta", ["dt_conta_item", "dt_mes_competencia", "dt_competencia"])
        col_vl       = find_col(con, "conta", ["vl_liberado", "vl_apresentado"])
        col_id_ben_c = find_col(con, "conta", ["id_beneficiario", "id_benef"])

        # beneficiario
        col_id_ben_b = find_col(con, "beneficiario", ["id_beneficiario", "id_benef"])
        col_dt_nasc  = find_col(con, "beneficiario", ["dt_nascimento"])

        # monta CASE de faixas por idade
        def faixa_case() -> str:
            parts = []
            for g in grupos:
                if "+" in g:
                    base = int(g.replace("+", "").strip())
                    parts.append(f"WHEN idade >= {base} THEN '{g}'")
                else:
                    a,b = g.split("-")
                    parts.append(f"WHEN idade BETWEEN {int(a)} AND {int(b)} THEN '{g}'")
            return "CASE " + " ".join(parts) + " ELSE 'outras' END"

        # idade no mês de competência (aproximação por ano/mes)
        query = f"""
            WITH base AS (
              SELECT c.{col_id_ben_c} AS id_beneficiario,
                     COALESCE(c.{col_vl},0) AS vl,
                     b.{col_dt_nasc} AS dt_nasc
              FROM conta c
              LEFT JOIN beneficiario b ON b.{col_id_ben_b} = c.{col_id_ben_c}
              WHERE date_trunc('month', c.{col_dt_conta}) = date '{comp_dt}'
            ),
            idades AS (
              SELECT id_beneficiario,
                     vl,
                     -- idade aproximada no primeiro dia do mês
                     CAST((strftime('%Y', date '{comp_dt}')::INT - strftime('%Y', dt_nasc)::INT) AS INT) AS idade
              FROM base
            )
            SELECT {faixa_case()} AS faixa,
                   SUM(vl) AS custo
            FROM idades
            GROUP BY 1
        """
        rows = con.execute(query).fetchall()
        out = {r[0]: float(r[1] or 0) for r in rows}
        # garante todas as faixas pedidas no retorno
        for g in grupos:
            out.setdefault(g, 0.0)
        return {"competencia": competencia, "custo_por_faixa": out}
    finally:
        con.close()

# ------------------------------------------------------------------------------
# UTILIZAÇÃO: resumo e evolução
# ------------------------------------------------------------------------------
def _construir_filtros_autorizacao(con,
                                   produto: Optional[str],
                                   uf: Optional[str],
                                   cidade: Optional[str],
                                   sexo: Optional[str],
                                   faixa: Optional[str]) -> Dict[str, Any]:
    """
    Constrói pedaços de WHERE/JOINS para filtros opcionais sobre a tabela 'autorizacao',
    possivelmente juntando com 'prestador' (uf/cidade) e 'beneficiario' (sexo/idade).
    """
    join_prestador = False
    join_benef     = False
    where_clauses  = []
    params         = {}

    # produto: tenta por código ou nome do item
    if produto:
        col_cd_item = None
        col_ds_item = None
        try:
            col_cd_item = find_col(con, "autorizacao", ["cd_item", "codigo_item"])
        except HTTPException:
            pass
        try:
            col_ds_item = find_col(con, "autorizacao", ["ds_item", "descricao_item"])
        except HTTPException:
            pass

        produto = produto.strip()
        if col_cd_item and produto.isdigit():
            where_clauses.append(f"a.{col_cd_item} = :prod_cod")
            params["prod_cod"] = int(produto)
        elif col_ds_item:
            where_clauses.append(f"lower(a.{col_ds_item}) LIKE :prod_nome")
            params["prod_nome"] = f"%{produto.lower()}%"

    # uf / cidade via prestador
    if uf or cidade:
        join_prestador = True
        col_id_prest_a = find_col(con, "autorizacao", ["id_prestador", "id_prestador_pagamento", "id_prestador_envio"])
        col_id_prest_p = find_col(con, "prestador", ["id_prestador"])
        col_uf         = find_col(con, "prestador", ["uf", "sg_uf"])
        col_cidade     = find_col(con, "prestador", ["cidade", "nm_cidade", "ds_cidade"])
        if uf:
            where_clauses.append(f"upper(p.{col_uf}) IN ({','.join([':uf'+str(i) for i,_ in enumerate(uf.split(','))])})")
            for i, val in enumerate(uf.split(',')):
                params['uf'+str(i)] = val.strip().upper()
        if cidade:
            # múltiplas cidades possíveis
            toks = [c.strip().lower() for c in cidade.split(',') if c.strip()]
            if toks:
                ors = [f"lower(p.{col_cidade}) LIKE :cid{i}" for i,_ in enumerate(toks)]
                where_clauses.append("(" + " OR ".join(ors) + ")")
                for i, val in enumerate(toks):
                    params['cid'+str(i)] = f"%{val}%"

    # sexo / faixa etária via beneficiario
    if sexo or faixa:
        join_benef = True
        find_col(con, "autorizacao", ["id_beneficiario"])  # valida presença
        find_col(con, "beneficiario", ["id_beneficiario"]) # valida presença
        col_sexo   = None
        try:
            col_sexo = find_col(con, "beneficiario", ["sexo", "ds_sexo"])
        except HTTPException:
            pass

        if sexo and col_sexo:
            where_clauses.append(f"upper(b.{col_sexo}) = :sexo")
            params["sexo"] = sexo.strip().upper()

        if faixa:
            # aceita "0-18,19-59,60+" e aplica como um OU (pertencer a qualquer)
            faixas = [f.strip() for f in faixa.split(",") if f.strip()]
            if faixas:
                col_dt_nasc = find_col(con, "beneficiario", ["dt_nascimento"])
                case_parts = []
                for g in faixas:
                    if "+" in g:
                        base = int(g.replace("+","").strip())
                        case_parts.append(f"CASE WHEN idade >= {base} THEN 1 ELSE 0 END")
                    else:
                        a,b = g.split("-")
                        case_parts.append(f"CASE WHEN idade BETWEEN {int(a)} AND {int(b)} THEN 1 ELSE 0 END")
                # idade aproximada no mês corrente (usaremos current_date; é suficiente para filtro)
                # Se quiser por competência, mude para uma data específica
                where_clauses.append(f"""
                   (
                     CASE
                       WHEN b.{col_dt_nasc} IS NULL THEN 0
                       ELSE (
                         {" + ".join(case_parts)}
                       )
                     END
                   ) >= 1
                """)

    return {
        "join_prestador": join_prestador,
        "join_benef": join_benef,
        "where": where_clauses,
        "params": params,
    }

@app.get("/kpi/utilizacao/resumo")
def kpi_utilizacao_resumo(
    competencia: str,
    produto: Optional[str] = None,
    uf: Optional[str] = None,
    cidade: Optional[str] = None,
    sexo: Optional[str] = None,
    faixa: Optional[str] = None
):
    comp_dt = parse_comp_yyyy_mm(competencia)
    con = open_con()
    try:
        # beneficiario base (todos)
        base_count = con.execute("SELECT COUNT(*) FROM beneficiario").fetchone()[0]

        # autorizacao mapeamento datas/ids
        col_dt_aut = find_col(con, "autorizacao", ["dt_autorizacao", "dt_entrada", "dt_envio"])
        col_id_ben = find_col(con, "autorizacao", ["id_beneficiario"])
        col_id_aut = find_col(con, "autorizacao", ["id_autorizacao"])

        # filtros opcionais
        F = _construir_filtros_autorizacao(con, produto, uf, cidade, sexo, faixa)
        join_p = ""
        join_b = ""
        if F["join_prestador"]:
            col_id_p_a = find_col(con, "autorizacao", ["id_prestador", "id_prestador_pagamento", "id_prestador_envio"])
            col_id_p_p = find_col(con, "prestador", ["id_prestador"])
            join_p = f"LEFT JOIN prestador p ON p.{col_id_p_p} = a.{col_id_p_a}"
        if F["join_benef"]:
            col_id_b_b = find_col(con, "beneficiario", ["id_beneficiario"])
            join_b = f"LEFT JOIN beneficiario b ON b.{col_id_b_b} = a.{col_id_ben}"

        where = [f"date_trunc('month', a.{col_dt_aut}) = date '{comp_dt}'"] + F["where"]
        wh = " AND ".join(where) if where else "1=1"

        # beneficiários que utilizaram no mês (distintos)
        q_ben = f"""
            SELECT COUNT(DISTINCT a.{col_id_ben})
            FROM autorizacao a
            {join_p}
            {join_b}
            WHERE {wh}
        """
        ben_util = con.execute(q_ben, F["params"]).fetchone()[0]

        # total de autorizações no mês (com filtros)
        q_aut = f"""
            SELECT COUNT(a.{col_id_aut})
            FROM autorizacao a
            {join_p}
            {join_b}
            WHERE {wh}
        """
        aut_total = con.execute(q_aut, F["params"]).fetchone()[0]

        return {
            "competencia": competencia,
            "beneficiarios_base": int(base_count or 0),
            "beneficiarios_utilizados": int(ben_util or 0),
            "autorizacoes": int(aut_total or 0),
            "filtros_aplicados": {
                k: v for k, v in {
                    "produto": produto,
                    "uf": uf,
                    "cidade": cidade,
                    "sexo": sexo,
                    "faixa": faixa,
                }.items() if v
            }
        }
    finally:
        con.close()

@app.get("/kpi/utilizacao/evolucao")
def kpi_utilizacao_evolucao(
    meses: int = 6,
    produto: Optional[str] = None,
    uf: Optional[str] = None,
    cidade: Optional[str] = None,
    sexo: Optional[str] = None,
    faixa: Optional[str] = None
):
    if meses <= 0 or meses > 60:
        raise HTTPException(status_code=400, detail="Param 'meses' deve estar entre 1 e 60.")

    con = open_con()
    try:
        col_dt_aut = find_col(con, "autorizacao", ["dt_autorizacao", "dt_entrada", "dt_envio"])
        col_id_ben = find_col(con, "autorizacao", ["id_beneficiario"])
        col_id_aut = find_col(con, "autorizacao", ["id_autorizacao"])

        # meses disponíveis
        meses_rows = con.execute(f"""
            SELECT DISTINCT date_trunc('month', {col_dt_aut}) AS m
            FROM autorizacao
            WHERE {col_dt_aut} IS NOT NULL
            ORDER BY m DESC
            LIMIT {meses}
        """).fetchall()
        meses_list = [r[0] for r in meses_rows][::-1]  # asc

        F = _construir_filtros_autorizacao(con, produto, uf, cidade, sexo, faixa)

        join_p = ""
        join_b = ""
        if F["join_prestador"]:
            col_id_p_a = find_col(con, "autorizacao", ["id_prestador", "id_prestador_pagamento", "id_prestador_envio"])
            col_id_p_p = find_col(con, "prestador", ["id_prestador"])
            join_p = f"LEFT JOIN prestador p ON p.{col_id_p_p} = a.{col_id_p_a}"
        if F["join_benef"]:
            col_id_b_b = find_col(con, "beneficiario", ["id_beneficiario"])
            join_b = f"LEFT JOIN beneficiario b ON b.{col_id_b_b} = a.{col_id_ben}"

        series = []
        for m in meses_list:
            where = [f"date_trunc('month', a.{col_dt_aut}) = '{m}'"] + F["where"]
            wh = " AND ".join(where)

            q1 = f"""
                SELECT COUNT(DISTINCT a.{col_id_ben})
                FROM autorizacao a
                {join_p}
                {join_b}
                WHERE {wh}
            """
            q2 = f"""
                SELECT COUNT(a.{col_id_aut})
                FROM autorizacao a
                {join_p}
                {join_b}
                WHERE {wh}
            """
            ben_util = con.execute(q1, F["params"]).fetchone()[0]
            aut_total = con.execute(q2, F["params"]).fetchone()[0]
            series.append({
                "competencia": con.execute("SELECT strftime('%Y-%m', ?)", [m]).fetchone()[0],
                "beneficiarios_utilizados": int(ben_util or 0),
                "autorizacoes": int(aut_total or 0),
            })

        return {
            "meses": len(series),
            "series": series,
            "filtros_aplicados": {
                k: v for k, v in {
                    "produto": produto,
                    "uf": uf,
                    "cidade": cidade,
                    "sexo": sexo,
                    "faixa": faixa,
                }.items() if v
            }
        }
    finally:
        con.close()
