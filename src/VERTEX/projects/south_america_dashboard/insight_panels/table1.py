import os
from typing import List, Tuple, Dict
from collections import OrderedDict

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

import vertex.IsaricDraw as idw

from dotenv import load_dotenv

# Carrega variáveis do arquivo .env (na raiz ou no diretório atual/pai)
load_dotenv()

# ----------------------------------------------------------------------
# Botão no menu do dashboard
# ----------------------------------------------------------------------
def define_button():
    return {"item": "Tables", "label": "Table 1"}


# ----------------------------------------------------------------------
# Conexão e carga da VIEW
# ----------------------------------------------------------------------
def _get_engine_from_env():
    """
    Usa apenas variáveis de ambiente para montar a URL de conexão.
    As variáveis devem estar definidas no .env (carregado via load_dotenv)
    ou diretamente no ambiente.
    """
    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD")
    host = os.getenv("PGHOST", "host.docker.internal")
    port = os.getenv("PGPORT", "5432")
    db = os.getenv("PGDATABASE")

    missing = [name for name, val in [
        ("PGUSER", user),
        ("PGPASSWORD", password),
        ("PGDATABASE", db),
    ] if not val]

    if missing:
        raise RuntimeError(
            "Variáveis de ambiente do banco não configuradas. "
            f"Faltando: {', '.join(missing)}. "
            "Defina-as no arquivo .env ou no ambiente antes de rodar o dashboard."
        )

    # se host for localhost/127.0.0.1 dentro do Docker, redireciona pra host.docker.internal
    if host in ("localhost", "127.0.0.1"):
        host = "host.docker.internal"

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    safe_url = f"postgresql+psycopg2://{user}:*****@{host}:{port}/{db}"
    print("[TABLE1] Conectando ao Postgres com URL:", safe_url, flush=True)

    return create_engine(url, pool_pre_ping=True)



def _load_sinan_view(year: int = 2024) -> pd.DataFrame:
    """
    Lê diretamente a VIEW sinan.vw_casos_tab12_base (já mapeada no SQL).
    """
    engine = _get_engine_from_env()

    sql = text(
        """
        SELECT *
        FROM sinan.vw_casos_tab12_base
        WHERE ano = :ano
          AND doenca = 'dengue'
        """
    )

    try:
        with engine.connect() as conn:
            print("[TABLE1] Lendo sinan.vw_casos_tab12_base…", flush=True)
            df = pd.read_sql(sql, conn, params={"ano": year})
        print("[TABLE1] Dados carregados da VIEW:", df.shape, flush=True)
        print("[TABLE1] Colunas:", list(df.columns), flush=True)
        return df
    except Exception as e:
        print("[TABLE1] ERRO ao conectar/buscar na VIEW:", repr(e), flush=True)
        raise


# ----------------------------------------------------------------------
# Helpers de formatação
# ----------------------------------------------------------------------
def _fmt_N(n: int) -> str:
    return f"{n:,}".replace(",", ".")


def _format_median_iqr(series: pd.Series) -> str:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return ""
    median = s.median()
    q1 = s.quantile(0.25)
    q3 = s.quantile(0.75)
    return f"{median:.1f} ({q1:.1f}, {q3:.1f})"


def _format_count_pct(series: pd.Series, value) -> str:
    s = series.dropna()
    denom = len(s)
    if denom == 0:
        return ""
    count = int((s == value).sum())
    pct = 100.0 * count / denom
    return f"{count:,d} ({pct:.1f}%)".replace(",", ".")


def _format_from_counts(count: int, denom: int) -> str:
    if denom <= 0:
        return ""
    pct = 100.0 * count / denom
    return f"{count:,d} ({pct:.1f}%)".replace(",", ".")


# ----------------------------------------------------------------------
# Construção da Tabela 1 (apenas cálculos e formatação)
# ----------------------------------------------------------------------
AGE_LABELS = [
    "0-4",
    "5-9",
    "10-14",
    "15-19",
    "20-29",
    "30-39",
    "40-49",
    "50-59",
    "60-69",
    "70-79",
    "80+",
]


def _split_by_severity(df: pd.DataFrame):
    """
    Separa em:
      - Todos
      - Dengue sem Sinais de Alarme (10)
      - Dengue com Sinais de Alarme (11)
      - Dengue Grave (12)
    usando classi_oms da VIEW (ou fallbacks se faltar).
    """
    if "classi_oms" in df.columns:
        cls_col = "classi_oms"
    elif "reclass_dengue_oms_v2" in df.columns:
        cls_col = "reclass_dengue_oms_v2"
    else:
        cls_col = "classi_fin"

    mask_any = df[cls_col].isin([10, 11, 12])
    df_any = df.loc[mask_any].copy()

    df_no = df_any.loc[df_any[cls_col] == 10]
    df_warn = df_any.loc[df_any[cls_col] == 11]
    df_sev = df_any.loc[df_any[cls_col] == 12]

    groups = OrderedDict(
        [
            ("Todos", df_any),
            ("Dengue sem Sinais de Alarme", df_no),
            ("Dengue com Sinais de Alarme", df_warn),
            ("Dengue Grave", df_sev),
        ]
    )

    n_all = len(df_any)
    n_no = len(df_no)
    n_warn = len(df_warn)
    n_sev = len(df_sev)

    print(
        f"[TABLE1] N total dengue (10/11/12): {n_all} | "
        f"sem sinais: {n_no} | com sinais: {n_warn} | grave: {n_sev}",
        flush=True,
    )

    return groups, n_all, n_no, n_warn, n_sev


def _build_table1(df: pd.DataFrame) -> Tuple[pd.DataFrame, int, int, int, int]:
    df = df.copy()

    groups, n_all, n_no, n_warn, n_sev = _split_by_severity(df)
    col_names = list(groups.keys())

    rows: List[Dict[str, str]] = []

    def add_row(label: str, values: Dict[str, str] | None = None):
        row = {"Características": label}
        for g in col_names:
            row[g] = "" if values is None else values.get(g, "")
        rows.append(row)

    # 1) Idade (Anos), mediana (IQR) — usa idade_anos da VIEW
    med_values = {g: _format_median_iqr(gdf.get("idade_anos", pd.Series(dtype=float)))
                  for g, gdf in groups.items()}
    add_row("Idade (Anos), mediana (IQR)", med_values)

    # 2) Faixas etárias, No. (%) — usa faixa_etaria (texto) da VIEW
    for faixa in AGE_LABELS:
        valores = {}
        for g, gdf in groups.items():
            if "faixa_etaria" in gdf.columns:
                valores[g] = _format_count_pct(gdf["faixa_etaria"], faixa)
            else:
                valores[g] = ""
        add_row(f"{faixa}, No. (%)", valores)

    # 3) Número de comorbidades — usa comorbidade_label da VIEW
    add_row("No. de comorbidades, No. (%)", None)
    com_order = [("0", "Nenhuma"), ("1", "1"), ("2", "2"), ("≥3", ">= 3")]
    for internal, label in com_order:
        valores = {}
        for g, gdf in groups.items():
            if "comorbidade_label" in gdf.columns:
                valores[g] = _format_count_pct(gdf["comorbidade_label"], internal)
            else:
                valores[g] = ""
        add_row(label, valores)

    # 4) Gênero Feminino, No. (%)
    if "sexo_label" in df.columns:
        sex_all = df["sexo_label"]
        n_valid_sex = int(sex_all.dropna().shape[0])
        pct_valid_sex = 100.0 * n_valid_sex / n_all if n_all > 0 else np.nan
        if n_valid_sex > 0:
            label_genero = (
                f"Gênero Feminino, No. (%) [n = {_fmt_N(n_valid_sex)}, "
                f"({pct_valid_sex:.0f}%)]"
            )
        else:
            label_genero = "Gênero Feminino, No. (%)"

        valores = {}
        for g, gdf in groups.items():
            ser = gdf["sexo_label"].dropna() if "sexo_label" in gdf.columns else pd.Series(dtype=object)
            denom = len(ser)
            count_fem = int((ser == "Feminino").sum())
            valores[g] = _format_from_counts(count_fem, denom)
        add_row(label_genero, valores)

    # 5) Escolaridade, No. (%) — usa escolaridade_nivel da VIEW
    if "escolaridade_nivel" in df.columns:
        esc_all = df["escolaridade_nivel"].dropna()
        n_valid_esc = int(esc_all.shape[0])
        pct_valid_esc = 100.0 * n_valid_esc / n_all if n_all > 0 else np.nan
        if n_valid_esc > 0:
            label_esc = (
                f"Escolaridade, No. (%) [n = {_fmt_N(n_valid_esc)}, "
                f"({pct_valid_esc:.0f}%)]"
            )
        else:
            label_esc = "Escolaridade, No. (%)"

        add_row(label_esc, None)

        esc_order = [
            "Analfabeto",
            "Ensino Fundamental Completo e Incompleto",
            "Ensino Médio Completo e Incompleto",
            "Ensino Superior Completo e Incompleto",
        ]
        for cat in esc_order:
            valores = {}
            for g, gdf in groups.items():
                if "escolaridade_nivel" in gdf.columns:
                    valores[g] = _format_count_pct(gdf["escolaridade_nivel"], cat)
                else:
                    valores[g] = ""
            add_row(cat, valores)

    # 6) Raça, No. (%) — usa raca_label da VIEW
    if "raca_label" in df.columns:
        race_all = df["raca_label"].dropna()
        n_valid_race = int(race_all.shape[0])
        pct_valid_race = 100.0 * n_valid_race / n_all if n_all > 0 else np.nan
        if n_valid_race > 0:
            label_race = (
                f"Raça, No. (%) [n = {_fmt_N(n_valid_race)}, "
                f"({pct_valid_race:.0f}%)]"
            )
        else:
            label_race = "Raça, No. (%)"

        add_row(label_race, None)

        race_order = ["Amarela", "Branca", "Indígena", "Parda", "Preta"]
        for cat in race_order:
            valores = {}
            for g, gdf in groups.items():
                if "raca_label" in gdf.columns:
                    valores[g] = _format_count_pct(gdf["raca_label"], cat)
                else:
                    valores[g] = ""
            add_row(cat, valores)

    table = pd.DataFrame(rows)
    table = table[["Características"] + col_names]
    print("[TABLE1] Tabela 1 montada no formato final:", table.shape, flush=True)

    return table, n_all, n_no, n_warn, n_sev


# ----------------------------------------------------------------------
# Função principal chamada pelo VERTEX
# ----------------------------------------------------------------------
def create_visuals(
    df_map,
    df_forms_dict,
    dictionary,
    quality_report,
    filepath,
    suffix,
    save_inputs,
):
    # 1) Carrega dados da VIEW (dengue, ano 2024)
    df_sinan = _load_sinan_view(year=2024)

    # 2) Monta a tabela já formatada (strings) + Ns
    disp, n_all, n_no, n_warn, n_sev = _build_table1(df_sinan)

    # 3) Ajusta cabeçalhos com N
    rename_map = {
        "Todos": f"Todos N = {_fmt_N(n_all)}",
        "Dengue sem Sinais de Alarme": f"Dengue sem Sinais de Alarme N = {_fmt_N(n_no)}",
        "Dengue com Sinais de Alarme": f"Dengue com Sinais de Alarme N = {_fmt_N(n_warn)}",
        "Dengue Grave": f"Dengue Grave N = {_fmt_N(n_sev)}",
    }
    disp = disp.rename(columns=rename_map)

    # 4) Cria visual
    table1 = idw.fig_table(
        disp,
        table_key="table1_sinan",
        suffix=suffix,
        filepath=filepath,
        save_inputs=save_inputs,
        graph_label="Tabela 1",
        graph_about=(
            "Características demográficas e clínicas dos casos de dengue "
            "segundo gravidade (SINAN, 2024)"
        ),
    )

    return [table1]