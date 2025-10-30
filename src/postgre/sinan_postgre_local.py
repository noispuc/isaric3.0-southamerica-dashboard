
import os, tempfile, itertools
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL  
from dbfread import DBF
import datasus_dbc


ARQ_DBC = r""
DOENCA = "dengue"
ANO    = 2022

SAMPLE_ROWS = 10_000  # Carrega 10000 linhas

# Engine via URL.create 
url = URL.create(
    "postgresql+psycopg2",
    username="postgres",
    password="benech",   
    host="localhost",
    port=5432,
    database="datasus",
)
engine = create_engine(url)


print(f"Lendo: {ARQ_DBC}  ({DOENCA}, {ANO}) | SAMPLE_ROWS={SAMPLE_ROWS}")

# DBC -> DBF -> DataFrame
with tempfile.TemporaryDirectory() as tmp:
    dbf_path = os.path.join(tmp, os.path.splitext(os.path.basename(ARQ_DBC))[0] + ".dbf")
    datasus_dbc.decompress(ARQ_DBC, dbf_path)

    table = DBF(dbf_path, encoding="latin-1")
    if SAMPLE_ROWS is None:
        df = pd.DataFrame(iter(table))
    else:
        rows = list(itertools.islice(table, SAMPLE_ROWS))
        df = pd.DataFrame(rows)

# metadados
df.columns = df.columns.str.lower()
df["doenca"] = DOENCA
df["ano"] = ANO

print("Shape do DF (teste):", df.shape)
print(df.head(3))

# grava no Postgres
df.to_sql(
    "solicitacao",
    engine,
    if_exists="append",
    index=False,
    chunksize=10_000,
    method="multi",
)
print("OK: Inserido no PostgreSQL (tabela: solicitacao)")

# conferência
with engine.begin() as con:
    total = con.execute(text("SELECT COUNT(*) FROM solicitacao")).scalar()
print(f"Total de linhas na tabela: {total}")