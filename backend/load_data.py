"""
Cria/atualiza o banco DuckDB a partir dos CSVs em backend/data/.
Normaliza tipos, cria tabelas base e materializa KPIs.
"""
import duckdb, pandas as pd, os, pathlib

DATA_DIR = pathlib.Path(__file__).parent / "data"
DB_PATH = DATA_DIR / "operadora.duckdb"

def read_csv(name):
    p = DATA_DIR / f"{name}.csv"
    if not p.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {p}")
    # tenta latin1 e vírgula como separador (amostras do Filipe)
    try:
        df = pd.read_csv(p, sep=",", quotechar='"', encoding="latin1", engine="python")
    except Exception:
        df = pd.read_csv(p, engine="python")
    return df

def normalize_types(con):
    # Beneficiário
    con.execute("""
        CREATE OR REPLACE TABLE beneficiario AS
        SELECT 
            CAST(id_beneficiario AS BIGINT) AS id_beneficiario,
            TRY_CAST(dt_nascimento AS DATE) AS dt_nascimento
        FROM beneficiario_raw
    """)

    # Prestador
    con.execute("""
        CREATE OR REPLACE TABLE prestador AS
        SELECT 
            CAST(id_prestador AS BIGINT) AS id_prestador,
            CAST(nm_prestador AS VARCHAR) AS nm_prestador
        FROM prestador_raw
    """)

    # Mensalidade
    con.execute("""
        CREATE OR REPLACE TABLE mensalidade AS
        SELECT 
            TRY_CAST(dt_competencia AS DATE) AS dt_competencia,
            TRY_CAST(REPLACE(CAST(vl_premio AS VARCHAR), ',', '.') AS DOUBLE) AS vl_premio
        FROM mensalidade_raw
        WHERE dt_competencia IS NOT NULL
    """)

    # Conta
    con.execute("""
        CREATE OR REPLACE TABLE conta AS
        SELECT 
            TRY_CAST(dt_mes_competencia AS DATE) AS dt_mes_competencia,
            CAST(id_beneficiario AS BIGINT) AS id_beneficiario,
            CAST(id_prestador_pagamento AS BIGINT) AS id_prestador_pagamento,
            TRY_CAST(REPLACE(CAST(vl_liberado AS VARCHAR), ',', '.') AS DOUBLE) AS vl_liberado
        FROM conta_raw
        WHERE dt_mes_competencia IS NOT NULL
    """)

def materialize_kpis(con):
    con.execute("""
        CREATE OR REPLACE TABLE kpi_sinistralidade_mensal AS
        WITH custo AS (
          SELECT dt_mes_competencia AS competencia, SUM(vl_liberado) AS custo
          FROM conta
          GROUP BY 1
        ),
        receita AS (
          SELECT dt_competencia AS competencia, SUM(vl_premio) AS receita
          FROM mensalidade
          GROUP BY 1
        )
        SELECT c.competencia,
               COALESCE(r.receita, 0) AS receita_vl_premio,
               COALESCE(c.custo, 0) AS custo_vl_liberado,
               CASE WHEN COALESCE(r.receita,0) = 0 THEN NULL ELSE c.custo / r.receita END AS sinistralidade
        FROM custo c
        FULL OUTER JOIN receita r USING (competencia)
        WHERE competencia IS NOT NULL
        ORDER BY competencia
    """)

def main():
    con = duckdb.connect(str(DB_PATH))
    # Carrega CSVs crus
    for name in ["beneficiario","prestador","mensalidade","conta"]:
        df = read_csv(name)
        con.execute(f"CREATE OR REPLACE TABLE {name}_raw AS SELECT * FROM df")

    # Normaliza tipos e cria tabelas base
    normalize_types(con)
    # Materializa KPIs
    materialize_kpis(con)
    con.close()
    print("DuckDB atualizado em", DB_PATH)

if __name__ == "__main__":
    main()
