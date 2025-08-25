# backend/load_data.py
from pathlib import Path
import duckdb
import sys

ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"
DB_PATH = DATA_DIR / "operadora.duckdb"

def load_csv_with_encodings(con, table: str, csv_path: Path) -> None:
    """
    Tenta carregar o CSV em UTF-8; se der erro de encoding, tenta LATIN1/CP1252.
    Como último recurso, usa IGNORE_ERRORS=TRUE em LATIN1.
    """
    encoders = ["utf8", "latin1", "windows-1252"]
    last_err = None

    for enc in encoders:
        try:
            con.execute(f"DROP TABLE IF EXISTS {table}")
            con.execute(
                """
                CREATE TABLE {table} AS
                SELECT * FROM read_csv_auto(?, HEADER=TRUE, ENCODING=?, IGNORE_ERRORS=FALSE);
                """.format(table=table),
                [str(csv_path), enc],
            )
            (cnt,) = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            print(f"[OK]   {table:<12}: {cnt} linhas (encoding={enc})")
            return
        except Exception as e:
            last_err = e

    # Último recurso: ignora linhas problemáticas em LATIN1
    try:
        con.execute(f"DROP TABLE IF EXISTS {table}")
        con.execute(
            """
            CREATE TABLE {table} AS
            SELECT * FROM read_csv_auto(?, HEADER=TRUE, ENCODING='latin1', IGNORE_ERRORS=TRUE);
            """.format(table=table),
            [str(csv_path)],
        )
        (cnt,) = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        print(f"[OK*] {table:<12}: {cnt} linhas (encoding=latin1, IGNORE_ERRORS=TRUE)")
    except Exception as e:
        print(f"[ERRO] {table}: {type(e).__name__}: {e}")
        if last_err:
            print(f"[TRACE] Último erro sem IGNORE_ERRORS: {last_err}")
        raise

def load_table(con, table: str, filename: str) -> None:
    csv_path = DATA_DIR / filename
    if not csv_path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {csv_path}")
    print(f"[LOAD] {table:<12} <- {filename}")
    load_csv_with_encodings(con, table, csv_path)

def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Conecta/cria o banco
    con = duckdb.connect(str(DB_PATH))
    # Opcional: paralelismo
    con.execute("PRAGMA threads=4;")

    # Carrega cada tabela
    load_table(con, "beneficiario", "beneficiario.csv")
    load_table(con, "conta",        "conta.csv")
    load_table(con, "mensalidade",  "mensalidade.csv")
    load_table(con, "prestador",    "prestador.csv")
    load_table(con, "autorizacao",  "autorizacao.csv")

    # Dica: crie índices simples quando fizer sentido para acelerar consultas
    # (DuckDB usa vectorized execution e costuma ser rápido mesmo sem índice)
    # Exemplo:
    # con.execute("CREATE INDEX IF NOT EXISTS idx_benef_cpf ON beneficiario(id_beneficiario)")

    con.close()
    print(f"[DONE] DuckDB atualizado em {DB_PATH}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(e, file=sys.stderr)
        sys.exit(1)
