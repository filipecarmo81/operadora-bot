# backend/load_data.py
from pathlib import Path
import duckdb

DATA_DIR = Path(__file__).parent / "data"
DB_PATH = DATA_DIR / "operadora.duckdb"

TABLES = {
    "beneficiario": "beneficiario.csv",
    "conta": "conta.csv",
    "mensalidade": "mensalidade.csv",
    "prestador": "prestador.csv",
    "autorizacao": "autorizacao.csv",  # <- garante carregar autorizacao
}

def main():
    if not DATA_DIR.exists():
        raise SystemExit(f"[ERRO] Pasta de dados não encontrada: {DATA_DIR}")

    con = duckdb.connect(str(DB_PATH))

    for table, fname in TABLES.items():
        csv_path = DATA_DIR / fname
        if not csv_path.exists():
            print(f"[WARN] CSV ausente: {csv_path} — tabela '{table}' NÃO será atualizada.")
            continue

        print(f"[LOAD] {table:<13} <- {csv_path.name}")
        # read_csv_auto detecta delimitador, tipos, etc.
        con.execute(
            """
            CREATE OR REPLACE TABLE %s AS
            SELECT * FROM read_csv_auto(?, HEADER=TRUE, SAMPLE_SIZE=-1);
            """
            % table,
            [str(csv_path)],
        )
        n = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"[OK]   {table:<13}: {n} linhas")

    con.close()
    print(f"DuckDB atualizado em {DB_PATH}")

if __name__ == "__main__":
    main()
