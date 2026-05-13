import os
import subprocess
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

BACKUP_DIR = Path(os.environ.get("BACKUP_DIR"))

def backup() -> None:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    db_name = os.environ["POSTGRES_DB"]
    out_file = BACKUP_DIR / f"{db_name}_{timestamp}.bak"
    subprocess.run(["pg_dump", os.environ["DATABASE_URL"], "-f", str(out_file)], check=True)
    print(f"Backup saved to {out_file}")


if __name__ == "__main__":
    backup()
