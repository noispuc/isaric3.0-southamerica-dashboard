# patch_staging_sinan.py
import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Carrega o mesmo .env que o sinan_postgre_local.py
here = Path(__file__).parent
env_path = here / "sinan.env"
if env_path.exists():
    load_dotenv(env_path)
    print(f"[PATCH] .env carregado: {env_path}")
else:
    print(f"[PATCH][WARN] sinan.env não encontrado em {env_path}, usando variáveis de ambiente atuais.")

# Monta a URL do Postgres
user = os.getenv("PGUSER", "postgres")
password = os.getenv("PGPASSWORD", "")
host = os.getenv("PGHOST", "localhost")
port = os.getenv("PGPORT", "5432")
db = os.getenv("PGDATABASE", "datasus")

url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
safe_url = f"postgresql+psycopg2://{user}:*****@{host}:{port}/{db}"
print("[PATCH] Conectando ao Postgres com URL:", safe_url)

engine = create_engine(url)

alter_sql = """
ALTER TABLE "sinan"."staging_solicitacao"
    ADD COLUMN IF NOT EXISTS cs_raca integer,
    ADD COLUMN IF NOT EXISTS cs_escol_n integer,
    ADD COLUMN IF NOT EXISTS cs_gestant integer,
    ADD COLUMN IF NOT EXISTS diabetes integer,
    ADD COLUMN IF NOT EXISTS hematolog integer,
    ADD COLUMN IF NOT EXISTS hepatopat integer,
    ADD COLUMN IF NOT EXISTS renal integer,
    ADD COLUMN IF NOT EXISTS hipertensa integer,
    ADD COLUMN IF NOT EXISTS acido_pept integer,
    ADD COLUMN IF NOT EXISTS auto_imune integer;
"""

with engine.begin() as con:
    con.execute(text(alter_sql))

print("[PATCH] Colunas extras criadas/garantidas em sinan.staging_solicitacao.")
