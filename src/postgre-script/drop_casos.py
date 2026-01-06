import os
from pathlib import Path

from sqlalchemy import create_engine, text
from dotenv import load_dotenv  # precisa já estar instalado na venv

# --- Carrega .env / sinan.env / config.env (igual ao ingest) ---
here = Path(__file__).parent
candidates = [here / ".env", here / "sinan.env", here / "config.env"]

loaded_any = False
for c in candidates:
    if c.exists():
        load_dotenv(dotenv_path=c, override=True)
        print(f"[ENV] carregado: {c}")
        loaded_any = True
        break

if not loaded_any:
    print("[DROP_CASOS] Nenhum arquivo .env/sinan.env/config.env encontrado; usando variáveis já definidas.")


def env(name: str, default: str | None = None) -> str | None:
    return os.environ.get(name, default)


# --- Monta a URL igual ao ingest (sem forçar host.docker.internal) ---
user = env("PGUSER", "postgres")
password = env("PGPASSWORD", "")
host = env("PGHOST", "localhost")
port = env("PGPORT", "5432")
db = env("PGDATABASE", "datasus")

url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
safe_url = f"postgresql+psycopg2://{user}:*****@{host}:{port}/{db}"
print("[DROP_CASOS] Conectando ao Postgres com URL:", safe_url, flush=True)

engine = create_engine(url, pool_pre_ping=True)

with engine.begin() as con:
    # CASCADE para derrubar as views/materialized views que dependem de sinan.casos
    con.execute(text('DROP TABLE IF EXISTS "sinan"."casos" CASCADE'))
    print("[DROP_CASOS] Tabela sinan.casos dropada com CASCADE (views dependentes também).")
