from pathlib import Path
import sys
import duckdb
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"
DB_PATH = DATA_DIR / "operadora.duckdb"

def load_with_duckdb(con: duckdb.DuckDBPyConnection, table: str, csv_path: Path) -> bool:
    """
    Tenta carregar usando DuckDB read_csv_auto (rápido).
    Retorna True se deu certo, False se deu erro de unicode (ou erro qualquer).
    """
    try:
        con.execute(f"DROP TABLE IF EXISTS {table}")
        # Sem ENCODING — DuckDB 1.0.0 não suporta. Mantemos HEADER e SAMPLE_SIZE.
        con.execute(
            f"""
            CREATE TABLE {table} AS
            SELECT * FROM read_csv_auto(?, HEADER=TRUE, SAMPLE_SIZE=-1, IGNORE_ERRORS=FALSE);
            """,
            [str(csv_path)],
        )
        (cnt,) = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        print(f"[OK]   {table:<12}: {cnt} linhas (duckdb)")
        return True
    except Exception as e:
        print(f"[INFO] DuckDB falhou para {table} ({csv_path.name}): {type(e).__name__}: {e}")
        return False

def load_with_pandas(con: duckdb.DuckDBPyConnection, table: str, csv_path: Path) -> None:
    """
    Fallback usando pandas.read_csv com tentativas de encoding.
    """
    tried = []
    for enc in ["utf-8", "latin1", "cp1252"]:
        try:
            df = pd.read_csv(csv_path, encoding=enc, engine="python")
            con.execute(f"DROP TABLE IF EXISTS {table}")
            con.register("tmp_df", df)
            con.execute(f"CREATE TABLE {table} AS SELECT * FROM tmp_df")
            con.unregister("tmp_df")
            (cnt,) = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            print(f"[OK]   {table:<12}: {cnt} linhas (pandas, encoding={enc})")
            return
        except Exception as e:
            tried.append(f"{enc} -> {type(e).__name__}: {e}")
    # Último recurso: substituir bytes inválidos
    try:
        df = pd.read_csv(csv_path, encoding="latin1", engine="python", errors="replace")
        con.execute(f"DROP TABLE IF EXISTS {table}")
        con.register("tmp_df", df)
        con.execute(f"CREATE TABLE {table} AS SELECT * FROM tmp_df")
        con.unregister("tmp_df")
        (cnt,) = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        print(f"[OK*]  {table:<12}: {cnt} linhas (pandas, encoding=latin1, errors=replace)")
    except Exception as e:
        print(f"[ERRO] {table}: falha geral no fallback pandas: {type(e).__name__}: {e}")
        print("[TRACE] Tentativas:", *tried, sep="\n  - ")
        raise

def load_table(con: duckdb.DuckDBPyConnection, table: str, filename: str) -> None:
    csv_path = DATA_DIR / filename
    if not csv_path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {csv_path}")
    print(f"[LOAD] {table:<12} <- {filename}")

    # 1) Tenta DuckDB direto (rápido)
    ok = load_with_duckdb(con, table, csv_path)
    if ok:
        return

    # 2) Fallback Pandas (encodings alternativos)
    load_with_pandas(con, table, csv_path)

def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute("PRAGMA threads=4;")

    # Carrega todas as tabelas necessárias
    load_table(con, "beneficiario", "beneficiario.csv")
    load_table(con, "conta",        "conta.csv")
    load_table(con, "mensalidade",  "mensalidade.csv")
    load_table(con, "prestador",    "prestador.csv")
    load_table(con, "autorizacao",  "autorizacao.csv")

    con.close()
    print(f"[DONE] DuckDB atualizado em {DB_PATH}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(e, file=sys.stderr)
        sys.exit(1)
