import os
from typing import List

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

import vertex.IsaricDraw as idw


def define_button():
    """
    Defines the button in the main dashboard menu.
    """
    button_item = "Tables"
    button_label = "Table 1"
    return {"item": button_item, "label": button_label}


# ----------------------------------------------------------------------
# Helpers de conexão / carga do SINAN
# ----------------------------------------------------------------------
def _get_engine_from_env():
    """
    Cria um engine SQLAlchemy usando as variáveis de ambiente do .env:

    PGUSER, PGPASSWORD, PGHOST, PGPORT, PGDATABASE
    """
    user = os.getenv("PGUSER", "postgres")
    password = os.getenv("PGPASSWORD", "benech")  # fallback

    host_env = os.getenv("PGHOST", "host.docker.internal")
    if host_env in ("localhost", "127.0.0.1"):
        host = "host.docker.internal"
    else:
        host = host_env

    port = os.getenv("PGPORT", "5432")
    db = os.getenv("PGDATABASE", "datasus")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"

    safe_url = f"postgresql+psycopg2://{user}:*****@{host}:{port}/{db}"
    print("[TABLE1] Conectando ao Postgres com URL:", safe_url, flush=True)

    return create_engine(url, pool_pre_ping=True)


def _load_sinan_cases(year: int = 2024) -> pd.DataFrame:
    """
    Carrega os casos do SINAN (schema sinan.casos) para um ano específico.
    """
    engine = _get_engine_from_env()

    sql = text(
        """
        SELECT *
        FROM sinan.casos
        WHERE ano = :ano
          AND doenca = 'dengue'
        """
    )

    try:
        with engine.connect() as conn:
            print("[TABLE1] Conexão com Postgres aberta com sucesso.", flush=True)
            df = pd.read_sql(sql, conn, params={"ano": year})
        print("[TABLE1] Dados do SINAN carregados:", df.shape, flush=True)
        print("[TABLE1] Colunas disponíveis:", list(df.columns), flush=True)
        print("[TABLE1] Distribuição de classi_fin:\n", df["classi_fin"].value_counts(), flush=True)
        return df

    except Exception as e:
        print("[TABLE1] ERRO ao conectar/buscar no Postgres:", repr(e), flush=True)
        raise


# ----------------------------------------------------------------------
# Helpers de tabela
# ----------------------------------------------------------------------
def _extract_age_years(idade_series: pd.Series) -> pd.Series:
    """
    Converte a coluna 'idade' no formato SINAN (4000 + idade em anos)
    para idade em anos.
    """
    s = pd.to_numeric(idade_series, errors="coerce")
    idade_anos = s - 4000
    idade_anos = idade_anos.where((idade_anos >= 0) & (idade_anos <= 120))
    return idade_anos


def _make_block_paper(
    name: str,
    s_all: pd.Series,
    s_no: pd.Series,
    s_warn: pd.Series,
    s_sev: pd.Series,
    order: List[str] | None = None,
    label_map: dict | None = None,
) -> pd.DataFrame:
    """
    Gera um bloco no formato:

    Characteristic | Category |
        All_n  | All_% |
        NoWarning_n | NoWarning_% |
        Warning_n | Warning_% |
        Severe_n | Severe_%

    s_all, s_no, s_warn, s_sev: séries com as categorias já prontas.
    """

    def _clean(series: pd.Series) -> pd.Series:
        return pd.Series(series).dropna()

    s_all = _clean(s_all)
    if s_all.empty:
        return pd.DataFrame(
            columns=[
                "Characteristic",
                "Category",
                "All_n",
                "All_%",
                "NoWarning_n",
                "NoWarning_%",
                "Warning_n",
                "Warning_%",
                "Severe_n",
                "Severe_%",
            ]
        )

    s_no = _clean(s_no)
    s_warn = _clean(s_warn)
    s_sev = _clean(s_sev)

    counts_all = s_all.value_counts()
    cats = list(counts_all.index)

    if order is not None:
        cats = [c for c in order if c in counts_all.index]

    counts_no = s_no.value_counts()
    counts_warn = s_warn.value_counts()
    counts_sev = s_sev.value_counts()

    total_all = counts_all.sum()
    total_no = counts_no.sum()
    total_warn = counts_warn.sum()
    total_sev = counts_sev.sum()

    rows = []
    for i, c in enumerate(cats):
        cat_label = c
        if label_map is not None:
            cat_label = label_map.get(c, c)

        n_all = int(counts_all.get(c, 0))
        n_no = int(counts_no.get(c, 0))
        n_warn = int(counts_warn.get(c, 0))
        n_sev = int(counts_sev.get(c, 0))

        row = {
            "Characteristic": name if i == 0 else "",
            "Category": cat_label,
            "All_n": n_all,
            "All_%": round((n_all / total_all * 100.0), 1) if total_all > 0 else np.nan,
            "NoWarning_n": n_no,
            "NoWarning_%": round((n_no / total_no * 100.0), 1) if total_no > 0 else np.nan,
            "Warning_n": n_warn,
            "Warning_%": round((n_warn / total_warn * 100.0), 1) if total_warn > 0 else np.nan,
            "Severe_n": n_sev,
            "Severe_%": round((n_sev / total_sev * 100.0), 1) if total_sev > 0 else np.nan,
        }
        rows.append(row)

    return pd.DataFrame(rows)


def _make_block_binary_single(
    name: str,
    s_all: pd.Series,
    s_no: pd.Series,
    s_warn: pd.Series,
    s_sev: pd.Series,
    yes_codes=(1,),
) -> pd.DataFrame:
    """
    Linha única para comorbidade do tipo "tem / não tem":

    Characteristic | Category (vazio) | All_n / All_% | ... | Severe_n / Severe_%
    """

    def _count_yes(series: pd.Series):
        s = pd.to_numeric(series, errors="coerce")
        if s is None:
            return 0, np.nan
        mask = s.isin(yes_codes)
        n_yes = int(mask.sum())
        denom = int(s.notna().sum())
        perc = round(n_yes / denom * 100.0, 1) if denom > 0 else np.nan
        return n_yes, perc

    n_all, p_all = _count_yes(s_all)
    n_no, p_no = _count_yes(s_no)
    n_warn, p_warn = _count_yes(s_warn)
    n_sev, p_sev = _count_yes(s_sev)

    row = {
        "Characteristic": name,
        "Category": "",
        "All_n": n_all,
        "All_%": p_all,
        "NoWarning_n": n_no,
        "NoWarning_%": p_no,
        "Warning_n": n_warn,
        "Warning_%": p_warn,
        "Severe_n": n_sev,
        "Severe_%": p_sev,
    }
    return pd.DataFrame([row])


# ----------------------------------------------------------------------
# Construção da Tabela 1
# ----------------------------------------------------------------------
def _build_table1(df: pd.DataFrame) -> pd.DataFrame:
    """
    Monta a Tabela 1 no formato próximo ao paper:

    - Age group (years)
    - Sex
    - Race/colour
    - Schooling
    - Comorbidities (diabetes, hipertensão, etc.)

    Colunas:
    Characteristic | Category |
        All_n | All_% |
        NoWarning_n | NoWarning_% |
        Warning_n | Warning_% |
        Severe_n | Severe_%
    """
    # Filtros de classificação OMS para dengue
    df = df.copy()

    # Considera só casos confirmados de dengue pelas classes 10, 11, 12
    mask_any = df["classi_fin"].isin([10, 11, 12])
    df_any = df[mask_any]

    df_no = df_any[df_any["classi_fin"] == 10]   # sem sinais de alarme
    df_warn = df_any[df_any["classi_fin"] == 11] # com sinais de alarme
    df_sev = df_any[df_any["classi_fin"] == 12]  # dengue grave

    n_all = len(df_any)
    n_no = len(df_no)
    n_warn = len(df_warn)
    n_sev = len(df_sev)

    print(
        f"[TABLE1] N total dengue (10/11/12): {n_all} | "
        f"sem sinais: {n_no} | com sinais: {n_warn} | grave: {n_sev}",
        flush=True,
    )

    blocks: List[pd.DataFrame] = []

    # -------- Age group (years) --------
    if "idade" in df_any.columns:
        print("[TABLE1] Usando coluna 'idade' (código SINAN: 4000 + anos).", flush=True)
        age_all = _extract_age_years(df_any["idade"])
        age_no = _extract_age_years(df_no["idade"])
        age_warn = _extract_age_years(df_warn["idade"])
        age_sev = _extract_age_years(df_sev["idade"])

        bins = [0, 5, 10, 15, 20, 30, 40, 50, 60, 70, np.inf]
        labels = [
            "0–4",
            "5–9",
            "10–14",
            "15–19",
            "20–29",
            "30–39",
            "40–49",
            "50–59",
            "60–69",
            "70+",
        ]

        age_all_cat = pd.cut(
            age_all,
            bins=bins,
            labels=labels,
            right=False,
            include_lowest=True,
        )
        age_no_cat = pd.cut(
            age_no,
            bins=bins,
            labels=labels,
            right=False,
            include_lowest=True,
        )
        age_warn_cat = pd.cut(
            age_warn,
            bins=bins,
            labels=labels,
            right=False,
            include_lowest=True,
        )
        age_sev_cat = pd.cut(
            age_sev,
            bins=bins,
            labels=labels,
            right=False,
            include_lowest=True,
        )

        age_block = _make_block_paper(
            "Age group (years)",
            age_all_cat,
            age_no_cat,
            age_warn_cat,
            age_sev_cat,
            order=labels,
        )
        blocks.append(age_block)

    # -------- Sex --------
    if "cs_sexo" in df_any.columns:
        print("[TABLE1] Usando coluna 'cs_sexo'.", flush=True)
        sex_map = {
            "M": "Male",
            "F": "Female",
        }
        sex_all = df_any["cs_sexo"].map(sex_map)
        sex_no = df_no["cs_sexo"].map(sex_map)
        sex_warn = df_warn["cs_sexo"].map(sex_map)
        sex_sev = df_sev["cs_sexo"].map(sex_map)

        sex_block = _make_block_paper(
            "Sex",
            sex_all,
            sex_no,
            sex_warn,
            sex_sev,
            order=["Male", "Female"],
        )
        blocks.append(sex_block)

    # -------- Race / colour --------
    if "cs_raca" in df_any.columns:
        race_map = {
            1: "White",
            2: "Black",
            3: "Asian",
            4: "Brown",
            5: "Indigenous",
            9: "Unknown",
        }
        race_all = df_any["cs_raca"].map(race_map)
        race_no = df_no["cs_raca"].map(race_map)
        race_warn = df_warn["cs_raca"].map(race_map)
        race_sev = df_sev["cs_raca"].map(race_map)

        race_block = _make_block_paper(
            "Race/colour",
            race_all,
            race_no,
            race_warn,
            race_sev,
            order=["White", "Brown", "Black", "Asian", "Indigenous", "Unknown"],
        )
        blocks.append(race_block)

    # -------- Schooling --------
    if "cs_escol_n" in df_any.columns:
        # Mapeamento simplificado, baseado em dicionários SINAN
        esc_map = {
            0: "Not applicable",
            1: "Illiterate",
            2: "1–5 years",
            3: "6–9 years",
            4: "10–12 years",
            5: "More than 12 years",
            9: "Unknown",
        }
        esc_all = df_any["cs_escol_n"].map(esc_map)
        esc_no = df_no["cs_escol_n"].map(esc_map)
        esc_warn = df_warn["cs_escol_n"].map(esc_map)
        esc_sev = df_sev["cs_escol_n"].map(esc_map)

        esc_block = _make_block_paper(
            "Schooling",
            esc_all,
            esc_no,
            esc_warn,
            esc_sev,
            order=[
                "Not applicable",
                "Illiterate",
                "1–5 years",
                "6–9 years",
                "10–12 years",
                "More than 12 years",
                "Unknown",
            ],
        )
        blocks.append(esc_block)

    # -------- Comorbidities (binary yes/no) --------
    comorb_cols = {
        "diabetes": "Diabetes",
        "hipertensa": "Hypertension",
        "renal": "Chronic kidney disease",
        "hepatopat": "Chronic liver disease",
        "hematolog": "Haematological disease",
        "acido_pept": "Peptic ulcer disease",
        "auto_imune": "Autoimmune disease",
    }

    for col, label in comorb_cols.items():
        if col not in df_any.columns:
            continue
        block = _make_block_binary_single(
            label,
            df_any[col],
            df_no[col],
            df_warn[col],
            df_sev[col],
            yes_codes=(1,),
        )
        blocks.append(block)

    if not blocks:
        return pd.DataFrame(
            columns=[
                "Characteristic",
                "Category",
                "All_n",
                "All_%",
                "NoWarning_n",
                "NoWarning_%",
                "Warning_n",
                "Warning_%",
                "Severe_n",
                "Severe_%",
            ]
        )

    table1_df = pd.concat(blocks, ignore_index=True)
    print("[TABLE1] Tabela 1 montada com formato:", table1_df.shape, flush=True)
    return table1_df


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
    """
    Cria todos os "visuals" do painel Table 1.
    """

    # 1) Carrega dados do SINAN (dengue, ano 2024)
    df_sinan = _load_sinan_cases(year=2024)

    # 2) Monta a tabela no formato do paper
    table1_df = _build_table1(df_sinan)

    # 3) Cria visual usando IsaricDraw
    table1 = idw.fig_table(
        table1_df,
        table_key="table1_sinan",
        suffix=suffix,
        filepath=filepath,
        save_inputs=save_inputs,
        graph_label="Table 1",
        graph_about="Description of characteristics and comorbidities among dengue cases (SINAN, 2024)",
    )

    return [table1]
