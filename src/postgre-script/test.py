import os
import psycopg2

conn = psycopg2.connect(
    host=os.getenv("PGHOST", "localhost"),
    port=os.getenv("PGPORT", 5432),
    user=os.getenv("PGUSER", "postgres"),
    password=os.getenv("PGPASSWORD"),
    dbname=os.getenv("PGDATABASE", "datasus"),
)
print("Conectou!", conn.get_dsn_parameters())
conn.close()