import os
from contextlib import contextmanager
from pathlib import Path
import psycopg2
from dotenv import load_dotenv

_SQL_DIR = Path(__file__).parent / "sql"

load_dotenv()

_DATABASE_URL = os.environ["DATABASE_URL"]

@contextmanager
def get_conn():
    conn = psycopg2.connect(_DATABASE_URL)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def init_db() -> None:
    order = (_SQL_DIR / "order.txt").read_text().splitlines()
    print("Initializing database with SQL files in the following order: %s", order)
    sql_files = [_SQL_DIR / name for name in order if name.strip()]
    with get_conn() as conn:
        with conn.cursor() as cur:
            for sql_file in sql_files:
                cur.execute(sql_file.read_text())
                print(f"Executed {sql_file.name}")
                conn.commit()
            with open("SeedDataInsert.sql") as f:
                cur.execute(f.read())
                print(f"Executed SeedDataInsert.sql")
                conn.commit()
