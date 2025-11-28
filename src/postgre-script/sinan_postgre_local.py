import os
import sys
import tempfile
import itertools
import warnings
from typing import Any, Dict, List

# -----------------------------
# .env loader (aceita .env, sinan.env, config.env ao lado do script)
# -----------------------------
try:
    from dotenv import load_dotenv  # type: ignore
    from pathlib import Path
    _here = Path(__file__).parent
    _env_candidates = [
        _here / ".env",
        _here / "sinan.env",
        _here / "config.env",
    ]
    _loaded_any = False
    for _cand in _env_candidates:
        if _cand.exists():
            load_dotenv(dotenv_path=_cand, override=True)
            print(f"[ENV] carregado: {_cand}")
            _loaded_any = True
            break
    if not _loaded_any:
        print(f"[WARN] nenhum arquivo .env encontrado nestes caminhos: {', '.join(str(p) for p in _env_candidates)}")
except Exception as _e:
    print(f"[WARN] python-dotenv não disponível ou falhou ao carregar .env: {_e}")

import pandas as pd
import yaml
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.types import String, Integer, Date, CHAR
from dbfread import DBF
import datasus_dbc


# -----------------------------
# Helpers
# -----------------------------
def env(name: str, default: str | None = None) -> str | None:
    return os.environ.get(name, default)


def load_yaml_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def build_engine() -> Any:
    """Build SQLAlchemy engine from either POSTGRES_DSN or PG* vars."""
    dsn = env("POSTGRES_DSN")
    if dsn:
        return create_engine(dsn)
    url = URL.create(
        "postgresql+psycopg2",
        username=env("PGUSER", "postgres"),
        password=env("PGPASSWORD", ""),
        host=env("PGHOST", "localhost"),
        port=int(env("PGPORT", "5432")),
        database=env("PGDATABASE", "postgres"),
    )
    return create_engine(url)


# -----------------------------
# Load env / config
# -----------------------------
CONFIG_PATH = _here / "config_sinan.yaml"
SCHEMA_TARGET = env("SCHEMA_TARGET", "sinan")

INPUT_DBC = env("INPUT_DBC")
DOENCA = env("DOENCA", "dengue")
ANO = int(env("ANO", "2022"))

# Ingest tuning
SAMPLE_ROWS = env("SAMPLE_ROWS")
SAMPLE_ROWS = None if (SAMPLE_ROWS is None or str(SAMPLE_ROWS).strip() == "") else int(SAMPLE_ROWS)
CHUNK_SIZE = int(env("CHUNK_SIZE", "50000"))

if not INPUT_DBC:
    print("ERRO: INPUT_DBC não encontrado nas variáveis de ambiente.")
    print(f"CWD: {os.getcwd()}")
    try:
        from pathlib import Path as _P
        here = _P(__file__).parent
        print("Arquivos .env tentados:")
        for p in [here/".env", here/"sinan.env", here/"config.env"]:
            print(f" - {p} | Existe? {p.exists()}")
    except Exception:
        pass
    sys.exit(1)

if not os.path.exists(INPUT_DBC):
    print(f"ERRO: Caminho INPUT_DBC não existe: {INPUT_DBC}")
    sys.exit(1)

cfg = load_yaml_config(CONFIG_PATH)
engine = build_engine()

project = cfg.get("project")
version = cfg.get("version")
print(f"Projeto: {project} | versão: {version}")

schema = cfg.get("database", {}).get("schema", SCHEMA_TARGET) or SCHEMA_TARGET
print(f"Schema alvo: {schema}")

# Ingest section
ingest_cfg = cfg.get("ingest", {})
source_encoding = ingest_cfg.get("source_encoding", "latin-1")
load_table = ingest_cfg.get("load_table", "staging_solicitacao")

# Table specs / views / grants / refresh
tables: List[Dict[str, Any]] = cfg.get("tables", [])
views: List[Dict[str, Any]] = cfg.get("views", [])
grants = cfg.get("grants", [])
refresh_cfg = cfg.get("refresh", {})


# -----------------------------
# DDL utilities
# -----------------------------
def ensure_schema_exists(schema_name: str) -> None:
    with engine.begin() as con:
        con.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"'))


def sql_identifier(name: str) -> str:
    return f'"{name}"'


def map_sqlalchemy_types(colspec: Dict[str, str]):
    """Map YAML types to SQLAlchemy types for pandas.to_sql dtype."""
    dtype_map = {}
    for col, typ in colspec.items():
        t = typ.strip().lower()
        if t.startswith("varchar"):
            try:
                length = int(t.split("(")[1].split(")")[0])
            except Exception:
                length = 255
            dtype_map[col] = String(length)
        elif t.startswith("char"):
            try:
                length = int(t.split("(")[1].split(")")[0])
            except Exception:
                length = 1
            dtype_map[col] = CHAR(length)
        elif t in ("integer", "int", "bigint", "smallint"):
            dtype_map[col] = Integer()
        elif t in ("date",):
            dtype_map[col] = Date()
        else:
            dtype_map[col] = String()
    return dtype_map


def create_staging_table(schema_name: str, table_cfg: Dict[str, Any]) -> None:
    name = table_cfg["name"]
    cols = table_cfg.get("columns", {})
    col_lines = []
    for col, typ in cols.items():
        col_lines.append(f"{sql_identifier(col)} {typ}")
    ddl = (
        f"CREATE TABLE IF NOT EXISTS {sql_identifier(schema_name)}.{sql_identifier(name)}\n("
        + ", ".join(col_lines) +
        "\n)"
    )
    with engine.begin() as con:
        con.execute(text(ddl))

    # Indexes
    for idx in table_cfg.get("indexes", []) or []:
        idx_cols = [sql_identifier(c) for c in idx]
        idx_name = f"idx_{name}_" + "_".join(idx)
        ddl_idx = (
            f"CREATE INDEX IF NOT EXISTS {sql_identifier(idx_name)} "
            f"ON {sql_identifier(schema_name)}.{sql_identifier(name)} ("
            + ", ".join(idx_cols) + ")"
        )
        with engine.begin() as con:
            con.execute(text(ddl_idx))


def get_column_type(schema_name: str, table_name: str, column_name: str) -> str | None:
    q = text("""
        SELECT data_type
        FROM information_schema.columns
        WHERE table_schema = :s AND table_name = :t AND column_name = :c
        LIMIT 1
    """)
    with engine.begin() as con:
        return con.execute(q, {"s": schema_name, "t": table_name, "c": column_name}).scalar()


# --- utils para views dependentes ---
def view_exists(schema_name: str, view_name: str) -> bool:
    q = text("""
        SELECT 1
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = :s
          AND c.relname = :v
          AND c.relkind IN ('v','m')
        LIMIT 1
    """)
    with engine.begin() as con:
        return bool(con.execute(q, {"s": schema_name, "v": view_name}).scalar())


def drop_config_views(schema_name: str, views_cfg: List[Dict[str, Any]]) -> None:
    """Dropa (se existirem) as views do YAML antes de migrar tipos da core."""
    with engine.begin() as con:
        for v in views_cfg:
            name = v["name"]
            is_mat = bool(v.get("materialized", False))
            if view_exists(schema_name, name):
                if is_mat:
                    con.execute(text(
                        f"DROP MATERIALIZED VIEW IF EXISTS {sql_identifier(schema_name)}.{sql_identifier(name)}"
                    ))
                else:
                    con.execute(text(
                        f"DROP VIEW IF EXISTS {sql_identifier(schema_name)}.{sql_identifier(name)}"
                    ))
                print(f"[DROP] {schema_name}.{name}")


def migrate_staging_types(schema_name: str, table_name: str) -> None:
    """
    Corrige tipos legados para casar com o YAML atual.
    - ano: se for DATE/TIMESTAMP, converte para INTEGER (extrai o ano).
    """
    cur = get_column_type(schema_name, table_name, "ano")
    if not cur:
        return
    cur_l = cur.lower()
    if cur_l.startswith("date") or cur_l.startswith("timestamp"):
        stmt = text(f"""
            ALTER TABLE {sql_identifier(schema_name)}.{sql_identifier(table_name)}
            ALTER COLUMN "ano" TYPE integer
            USING EXTRACT(YEAR FROM "ano")::int
        """)
        with engine.begin() as con:
            con.execute(stmt)
        print(f"[MIGRATE] Corrigido tipo de {schema_name}.{table_name}.ano -> INTEGER")


def upsert_core_table(schema_name: str, core_cfg: Dict[str, Any]) -> None:
    name = core_cfg["name"]
    create_as = core_cfg.get("create_as") or core_cfg.get("create_as_sql")
    if not create_as:
        return
    create_as_sql = create_as.replace("{{schema}}", schema_name)

    # Create table if not exists, then TRUNCATE + INSERT (idempotente)
    ddl = f"CREATE TABLE IF NOT EXISTS {sql_identifier(schema_name)}.{sql_identifier(name)} AS {create_as_sql} WITH NO DATA"
    with engine.begin() as con:
        con.execute(text(ddl))
        con.execute(text(f"TRUNCATE TABLE {sql_identifier(schema_name)}.{sql_identifier(name)}"))
        con.execute(text(f"INSERT INTO {sql_identifier(schema_name)}.{sql_identifier(name)} {create_as_sql}"))

    # Antes de migrar tipos, derruba as views do config (serão recriadas depois)
    drop_config_views(schema_name, views)

    # Agora sim, garante tipos esperados nos IDs (varchar(7)) para JOIN com aux.pop_municipios
    migrate_core_casos_types(schema_name, name)


def migrate_core_casos_types(schema_name: str, table_name: str) -> None:
    """Garante que colunas ID em 'casos' sejam varchar(7), compatíveis com aux.pop_municipios."""
    if table_name != "casos":
        return
    # id_municip
    cur_id = get_column_type(schema_name, table_name, "id_municip")
    if cur_id and not cur_id.lower().startswith("character varying"):
        with engine.begin() as con:
            con.execute(text(f"""
                ALTER TABLE {sql_identifier(schema_name)}.{sql_identifier(table_name)}
                ALTER COLUMN "id_municip" TYPE varchar(7)
                USING lpad("id_municip"::text, 7, '0')
            """))
        print(f"[MIGRATE] {schema_name}.{table_name}.id_municip -> varchar(7)")
    # id_mn_resi
    cur_idr = get_column_type(schema_name, table_name, "id_mn_resi")
    if cur_idr and not cur_idr.lower().startswith("character varying"):
        with engine.begin() as con:
            con.execute(text(f"""
                ALTER TABLE {sql_identifier(schema_name)}.{sql_identifier(table_name)}
                ALTER COLUMN "id_mn_resi" TYPE varchar(7)
                USING lpad("id_mn_resi"::text, 7, '0')
            """))
        print(f"[MIGRATE] {schema_name}.{table_name}.id_mn_resi -> varchar(7)")


# --- Índice único exigido para REFRESH CONCURRENTLY ---
def has_unique_index_without_predicate_on_mv(schema_name: str, mv_name: str) -> bool:
    q = text("""
        SELECT 1
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = :s
          AND c.relname = :m
          AND i.indisunique = true
          AND i.indpred IS NULL
        LIMIT 1
    """)
    with engine.begin() as con:
        return bool(con.execute(q, {"s": schema_name, "m": mv_name}).scalar())


def ensure_mv_unique_index(schema_name: str, mv_name: str, cols: list[str]) -> None:
    """Cria um índice ÚNICO (sem WHERE) nas colunas informadas, se ainda não existir."""
    if has_unique_index_without_predicate_on_mv(schema_name, mv_name):
        return
    idx_name = f"uq_{mv_name}_{'_'.join(cols)}"
    ddl = text(
        f"CREATE UNIQUE INDEX IF NOT EXISTS {sql_identifier(idx_name)} "
        f"ON {sql_identifier(schema_name)}.{sql_identifier(mv_name)} "
        f"({', '.join(cols)})"
    )
    with engine.begin() as con:
        con.execute(ddl)
    print(f"[IDX] Índice único criado em {schema_name}.{mv_name} ({', '.join(cols)})")


def create_or_replace_views(schema_name: str, views_cfg: List[Dict[str, Any]]) -> None:
    for v in views_cfg:
        name = v["name"]
        sql = v.get("sql", "").replace("{{schema}}", schema_name)
        is_materialized = bool(v.get("materialized", False))
        with engine.begin() as con:
            if is_materialized:
                # já cria a MV com dados para evitar o 1º refresh sem concurrently
                con.execute(text(
                    f"CREATE MATERIALIZED VIEW IF NOT EXISTS {sql_identifier(schema_name)}.{sql_identifier(name)} AS {sql} WITH DATA"
                ))
                # para nossa MV semanal, garantir índice único (semana,id_municip)
                if name == "vw_inc_mun_semanal":
                    # como acabamos de criar a MV, o índice é criado fora do mesmo bloco
                    pass
            else:
                con.execute(text(
                    f"CREATE OR REPLACE VIEW {sql_identifier(schema_name)}.{sql_identifier(name)} AS {sql}"
                ))
    # criar índices exigidos (fora do bloco acima para evitar problemas de transação)
    for v in views_cfg:
        if v.get("materialized", False) and v["name"] == "vw_inc_mun_semanal":
            ensure_mv_unique_index(schema_name, v["name"], ["semana", "id_municip"])


def apply_grants(schema_name: str, grants_cfg: List[Dict[str, Any]]) -> None:
    with engine.begin() as con:
        for g in grants_cfg:
            role = g.get("role")
            objs = g.get("on", [])
            privs = g.get("privileges", ["SELECT"])  # default
            if not role or not objs:
                continue
            for obj in objs:
                stmt = f"GRANT {', '.join(privs)} ON {sql_identifier(schema_name)}.{sql_identifier(obj)} TO \"{role}\""
                try:
                    con.execute(text(stmt))
                except Exception as e:
                    warnings.warn(f"Falha ao aplicar GRANT em {obj} para {role}: {e}")


def refresh_materialized(schema_name: str, refresh_cfg: Dict[str, Any]) -> None:
    """Refresca MVs do YAML; usa CONCURRENTLY apenas se já estiverem populadas e com índice único."""
    mats = (refresh_cfg or {}).get("materialized_views", [])
    with engine.begin() as con:
        for mv in mats:
            name = mv.get("name")
            on_load = mv.get("on_load", False)
            if not (name and on_load):
                continue

            exists = con.execute(text("""
                SELECT 1 FROM pg_matviews
                WHERE schemaname = :s AND matviewname = :m
            """), {"s": schema_name, "m": name}).scalar()
            if not exists:
                print(f"[SKIP] MV {schema_name}.{name} não existe; pulando refresh")
                continue

            ispop = bool(con.execute(text("""
                SELECT ispopulated
                FROM pg_matviews
                WHERE schemaname = :s AND matviewname = :m
            """), {"s": schema_name, "m": name}).scalar())

            can_concurrently = ispop and has_unique_index_without_predicate_on_mv(schema_name, name)

            stmt = (
                text(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {sql_identifier(schema_name)}.{sql_identifier(name)}")
                if can_concurrently else
                text(f"REFRESH MATERIALIZED VIEW {sql_identifier(schema_name)}.{sql_identifier(name)}")
            )
            con.execute(stmt)


# -----------------------------
# Aux table: aux.pop_municipios (for incidence/obitos per 100k)
# -----------------------------
def ensure_aux_pop_table() -> None:
    """
    Garante que exista o schema 'aux' e a tabela 'pop_municipios' com colunas mínimas.
    Se POP_MUN_CSV estiver definido e existir, carrega/atualiza os dados.
    Espera CSV com cabeçalhos: id_municip,pop
    """
    from pathlib import Path

    pop_schema = "aux"
    pop_table = "pop_municipios"

    with engine.begin() as con:
        con.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{pop_schema}"'))
        con.execute(text(
            f'''CREATE TABLE IF NOT EXISTS "{pop_schema}"."{pop_table}" (
                id_municip varchar(7) PRIMARY KEY,
                pop integer
            )'''
        ))

    csv_path = env("POP_MUN_CSV")
    if not csv_path:
        print("[POP] POP_MUN_CSV não definido; criando views que dependem de aux.pop_municipios com base em tabela (possivelmente vazia).")
        return

    p = Path(csv_path)
    if not p.exists():
        print(f"[POP] POP_MUN_CSV informado mas não existe: {p}")
        return

    # Carrega CSV e normaliza
    try:
        popdf = pd.read_csv(p)
    except Exception:
        popdf = pd.read_csv(p, sep=';')

    popdf.columns = popdf.columns.str.lower()
    # normaliza colunas esperadas
    if 'id_municip' not in popdf.columns:
        for cand in ['id_municipio', 'cod_mun', 'cod_municipio', 'municipio_id']:
            if cand in popdf.columns:
                popdf = popdf.rename(columns={cand: 'id_municip'})
                break
    if 'pop' not in popdf.columns:
        for cand in ['populacao', 'population']:
            if cand in popdf.columns:
                popdf = popdf.rename(columns={cand: 'pop'})
                break

    if 'id_municip' not in popdf.columns or 'pop' not in popdf.columns:
        print("[POP] CSV não contém colunas 'id_municip' e 'pop' — mantendo tabela como está.")
        return

    popdf['id_municip'] = (
        popdf['id_municip']
        .astype(str)
        .str.replace('.0','', regex=False)
        .str.replace('nan','', regex=False)
        .str.zfill(7)
    )
    popdf['pop'] = pd.to_numeric(popdf['pop'], errors='coerce').fillna(0).astype('Int64')

    # carrega substituindo (staging-like)
    with engine.begin() as con:
        con.execute(text(f'DELETE FROM "{pop_schema}"."{pop_table}"'))
    popdf.to_sql(
        name=pop_table,
        con=engine,
        schema=pop_schema,
        if_exists='append',
        index=False,
        chunksize=20000,
        method='multi',
    )
    print(f"[POP] Tabela {pop_schema}.{pop_table} carregada com {len(popdf)} linhas de {p}")


# -----------------------------
# Extract → Transform (light) → Load (staging)
# -----------------------------
print(f"Lendo: {INPUT_DBC}  ({DOENCA}, {ANO}) | SAMPLE_ROWS={SAMPLE_ROWS}")

with tempfile.TemporaryDirectory() as tmp:
    dbf_path = os.path.join(tmp, os.path.splitext(os.path.basename(INPUT_DBC))[0] + ".dbf")
    datasus_dbc.decompress(INPUT_DBC, dbf_path)

    table = DBF(dbf_path, encoding=source_encoding)
    if SAMPLE_ROWS is None:
        df = pd.DataFrame(iter(table))
    else:
        rows = list(itertools.islice(table, SAMPLE_ROWS))
        df = pd.DataFrame(rows)

# Normalize columns / minimal typing
df.columns = df.columns.str.lower()

# Ensure expected columns exist (fill if missing)
expected_cols = {
    "id_municip": None,
    "id_mn_resi": None,
    "sg_uf": None,
    "uf": None,
    "cs_sexo": None,
    "nu_idade_n": None,
    "classi_fin": None,
    "evolucao": None,
    "hospitaliz": None,
    "tp_not": None,
    "id_agravo": None,
    "dt_notific": None,
    "dt_sin_pri": None,

    # >>> NOVOS CAMPOS PARA TABLE 1 <<<

    "cs_raca": None,
    "cs_escol_n": None,
    "cs_gestant": None,

    "diabetes": None,
    "hematolog": None,
    "hepatopat": None,
    "renal": None,
    "hipertensa": None,
    "acido_pept": None,
    "auto_imune": None,
}

for c in expected_cols:
    if c not in df.columns:
        df[c] = pd.NA

# Coerce strings / ids with leading zeros
for pad_col in ["id_municip", "id_mn_resi"]:
    df[pad_col] = (
        df[pad_col]
        .astype(str)
        .str.replace(".0", "", regex=False)
        .mask(df[pad_col].isna(), "")
        .str.replace("nan", "", regex=False)
        .str.zfill(7)
    )

# Dates
for dcol in ["dt_notific", "dt_sin_pri"]:
    df[dcol] = pd.to_datetime(df[dcol], errors="coerce", dayfirst=True).dt.date

# Extra metadata
df["doenca"] = DOENCA
df["ano"] = int(ANO)

# Sanitização de tipos p/ colunas numéricas -> evita "" em INTEGER
_numeric_cols = [
    "uf", "nu_idade_n", "classi_fin", "evolucao",
    "hospitaliz", "tp_not", "ano",

    # novos numéricos:
    "cs_raca", "cs_escol_n", "cs_gestant",
    "diabetes", "hematolog", "hepatopat",
    "renal", "hipertensa", "acido_pept", "auto_imune",
]

for col in _numeric_cols:
    if col in df.columns:
        df[col] = (
            df[col]
            .replace({"": pd.NA, " ": pd.NA})
            .apply(lambda x: None if (pd.isna(x) or str(x).strip() == "") else x)
        )
        df[col] = pd.to_numeric(df[col], errors="coerce")

print("Shape do DF (amostra):", df.shape)
print(df.head(3))


# -----------------------------
# DDL: schema, staging, core, views, grants
# -----------------------------
ensure_schema_exists(schema)

# Create staging and indexes from config
staging_cfg = next((t for t in tables if t.get("name") == load_table), None)
if not staging_cfg:
    raise RuntimeError(f"Tabela de staging '{load_table}' não encontrada no config.")

create_staging_table(schema, staging_cfg)

# Corrige tipos legados (ex.: 'ano' que já exista como DATE/TIMESTAMP)
migrate_staging_types(schema, staging_cfg["name"])

# Load into staging
staging_cols = staging_cfg.get("columns", {})
pandas_dtype = map_sqlalchemy_types(staging_cols)

# Keep only staging columns order
df_to_load = df[[c for c in staging_cols.keys()]].copy()

print(f"Carregando dados em {schema}.{load_table} (chunksize={CHUNK_SIZE}) …")
df_to_load.to_sql(
    name=load_table,
    con=engine,
    schema=schema,
    if_exists="append",
    index=False,
    chunksize=CHUNK_SIZE,
    method="multi",
    dtype=pandas_dtype,
)
print("OK: Inserido no PostgreSQL (tabela de staging)")
# Conferência imediata da staging
try:
    with engine.begin() as con:
        total_stg_now = con.execute(text(
            f"SELECT COUNT(*) FROM {sql_identifier(schema)}.{sql_identifier(load_table)}"
        )).scalar()
        print(f"[CHECK] Linhas em {schema}.{load_table}: {total_stg_now}")
except Exception as e:
    print(f"[WARN] Falha ao consultar staging: {e}")

# Create/refresh core tables
for t in tables:
    if t.get("type") == "core":
        upsert_core_table(schema, t)
        print(f"Core atualizado: {schema}.{t['name']}")
        # Conferência imediata do core
        try:
            with engine.begin() as con:
                total_core_now = con.execute(text(
                    f"SELECT COUNT(*) FROM {sql_identifier(schema)}.{sql_identifier(t['name'])}"
                )).scalar()
                print(f"[CHECK] Linhas em {schema}.{t['name']}: {total_core_now}")
        except Exception as e:
            print(f"[WARN] Falha ao consultar core {t['name']}: {e}")

# Views (normal + materialized)
# Garante tabela de população antes, pois algumas views fazem JOIN em aux.pop_municipios
try:
    ensure_aux_pop_table()
    create_or_replace_views(schema, views)
except Exception as e:
    print(f"[WARN] Falha ao criar/atualizar views: {e}")

# Grants
apply_grants(schema, grants)

# Refresh materialized if on_load
refresh_materialized(schema, refresh_cfg)

# Conferência rápida
with engine.begin() as con:
    total_stg = con.execute(text(
        f"SELECT COUNT(*) FROM {sql_identifier(schema)}.{sql_identifier(load_table)}"
    )).scalar()
    print(f"Total de linhas na staging: {total_stg}")
    try:
        total_core = con.execute(text(
            f"SELECT COUNT(*) FROM {sql_identifier(schema)}.\"casos\""
        )).scalar()
        print(f"Total de linhas em {schema}.casos: {total_core}")
    except Exception:
        pass

print("Pipeline concluído com sucesso.")