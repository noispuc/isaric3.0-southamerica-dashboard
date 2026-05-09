import os
from typing import List, Dict
from collections import OrderedDict

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
import vertex.IsaricDraw as idw
from dotenv import load_dotenv

load_dotenv()


def define_button():
    return {"item": "Tables", "label": "Table 1"}


def _get_engine_from_env():
    """
    Usa variáveis de ambiente para montar a URL de conexão.
    """
    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD")
    host = os.getenv("PGHOST")
    port = os.getenv("PGPORT")
    db = os.getenv("PGDATABASE")

    missing = [
        name
        for name, val in [
            ("PGUSER", user),
            ("PGPASSWORD", password),
            ("PGDATABASE", db),
        ]
        if not val
    ]

    if missing:
        raise RuntimeError(
            "Variáveis de ambiente do banco não configuradas. "
            f"Faltando: {', '.join(missing)}. "
            "Defina-as no arquivo .env ou no ambiente antes de rodar o dashboard."
        )

    if host in ("localhost", "127.0.0.1"):
        host = "127.0.0.1"

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    safe_url = f"postgresql+psycopg2://{user}:*****@{host}:{port}/{db}"

    print("[TABLE1_FA] Conectando ao Postgres com URL:", safe_url, flush=True)

    return create_engine(url, pool_pre_ping=True)


def _load_fa_view(year: int = 2024) -> pd.DataFrame:
    """
    Lê diretamente a view febre_amarela.vw_casos_tab12_base.
    """
    engine = _get_engine_from_env()

    sql = text(
        """
        SELECT *
        FROM febre_amarela.vw_casos_tab12_base
        WHERE ano = :ano
        """
    )

    try:
        with engine.connect() as conn:
            print(
                "[TABLE1_FA] Lendo febre_amarela.vw_casos_tab12_base...",
                flush=True,
            )
            df = pd.read_sql(sql, conn, params={"ano": year})

        print("[TABLE1_FA] Dados carregados da VIEW:", df.shape, flush=True)
        print("[TABLE1_FA] Colunas:", list(df.columns), flush=True)

        return df

    except Exception as e:
        print("[TABLE1_FA] ERRO ao conectar/buscar na VIEW:", repr(e), flush=True)
        raise

    except Exception as e:
        print("[TABLE1_FA] ERRO ao conectar/buscar na VIEW:", repr(e), flush=True)
        raise


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


def _decode_idade_sinan_to_years(idade):
    """
    Converte idade no padrão SINAN para anos.
    Se a idade já estiver em anos, mantém.
    """
    if pd.isna(idade):
        return np.nan

    try:
        idade = float(idade)
    except Exception:
        return np.nan

    if 4000 <= idade <= 4999:
        return idade - 4000

    if 3000 <= idade <= 3999:
        return (idade - 3000) / 12.0

    if 2000 <= idade <= 2999:
        return (idade - 2000) / 365.0

    if 1000 <= idade <= 1999:
        return (idade - 1000) / (24.0 * 365.0)

    if 0 <= idade <= 120:
        return idade

    return np.nan


def _age_group_from_years(age):
    if pd.isna(age):
        return np.nan

    if age < 5:
        return "0-4"
    if age < 10:
        return "5-9"
    if age < 15:
        return "10-14"
    if age < 20:
        return "15-19"
    if age < 30:
        return "20-29"
    if age < 40:
        return "30-39"
    if age < 50:
        return "40-49"
    if age < 60:
        return "50-59"
    if age < 70:
        return "60-69"
    if age < 80:
        return "70-79"

    return "80+"


def _prepare_fa_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Garante colunas mínimas necessárias para montar a Table 1:
    - idade_anos
    - faixa_etaria_view
    - sexo_label
    """
    df = df.copy()

    if "idade_anos" not in df.columns and "idade" in df.columns:
        df["idade_anos"] = df["idade"].apply(_decode_idade_sinan_to_years)

    if "faixa_etaria_view" not in df.columns:
        if "faixa_etaria" in df.columns:
            df["faixa_etaria_view"] = df["faixa_etaria"]
        elif "idade_anos" in df.columns:
            df["faixa_etaria_view"] = df["idade_anos"].apply(_age_group_from_years)

    if "sexo_label" not in df.columns and "sexo" in df.columns:
        df["sexo_label"] = (
            df["sexo"]
            .astype(str)
            .str.strip()
            .str.upper()
            .map(
                {
                    "F": "Feminino",
                    "FEMININO": "Feminino",
                    "M": "Masculino",
                    "MASCULINO": "Masculino",
                }
            )
        )

    return df


def _get_age_group_col(df: pd.DataFrame) -> str | None:
    if "faixa_etaria_view" in df.columns:
        return "faixa_etaria_view"

    if "faixa_etaria" in df.columns:
        return "faixa_etaria"

    return None


def _build_table1(df: pd.DataFrame):
    df = _prepare_fa_columns(df)

    groups = OrderedDict([("Yellow fever", df)])
    n_all = len(df)
    col_names = list(groups.keys())
    rows: List[Dict[str, str]] = []

    age_col = _get_age_group_col(df)

    def add_row(label: str, values: Dict[str, str] | None = None):
        row = {"Characteristics": label}

        for g in col_names:
            row[g] = "" if values is None else values.get(g, "")

        rows.append(row)

    # Idade mediana
    med_values = {
        g: _format_median_iqr(gdf.get("idade_anos", pd.Series(dtype=float)))
        for g, gdf in groups.items()
    }

    add_row("Age (Years), median (IQR)", med_values)

    # Faixas etárias
    for faixa in AGE_LABELS:
        valores = {}

        for g, gdf in groups.items():
            if age_col is not None and age_col in gdf.columns:
                valores[g] = _format_count_pct(gdf[age_col], faixa)
            else:
                valores[g] = ""

        add_row(f"{faixa}, No. (%)", valores)

    # Sexo feminino
    if "sexo_label" in df.columns:
        sex_all = df["sexo_label"].dropna()
        n_valid_sex = int(sex_all.shape[0])
        pct_valid_sex = 100.0 * n_valid_sex / n_all if n_all > 0 else np.nan

        if n_valid_sex > 0:
            label_genero = (
                f"Female Gender, No. (%) [n = {_fmt_N(n_valid_sex)}, "
                f"({pct_valid_sex:.0f}%)]"
            )
        else:
            label_genero = "Female Gender, No. (%)"

        valores = {}

        for g, gdf in groups.items():
            ser = (
                gdf["sexo_label"].dropna()
                if "sexo_label" in gdf.columns
                else pd.Series(dtype=object)
            )

            denom = len(ser)
            count_fem = int((ser == "Feminino").sum())

            valores[g] = _format_from_counts(count_fem, denom)

        add_row(label_genero, valores)

    # Escolaridade: só entra se a view tiver essa coluna
    if "escolaridade_nivel" in df.columns:
        esc_all = df["escolaridade_nivel"].dropna()
        n_valid_esc = int(esc_all.shape[0])
        pct_valid_esc = 100.0 * n_valid_esc / n_all if n_all > 0 else np.nan

        if n_valid_esc > 0:
            label_esc = (
                f"Education Level, No. (%) [n = {_fmt_N(n_valid_esc)}, "
                f"({pct_valid_esc:.0f}%)]"
            )
        else:
            label_esc = "Education Level, No. (%)"

        add_row(label_esc, None)

        esc_mapping = [
            ("Analfabeto", "Illiterate"),
            ("Ensino Fundamental Completo e Incompleto", "Primary Education"),
            ("Ensino Médio Completo e Incompleto", "Secondary Education"),
            ("Ensino Superior Completo e Incompleto", "Higher Education"),
        ]

        for cat, label in esc_mapping:
            valores = {}

            for g, gdf in groups.items():
                valores[g] = _format_count_pct(gdf["escolaridade_nivel"], cat)

            add_row(label, valores)

    # Raça: só entra se a view tiver essa coluna
    if "raca_label" in df.columns:
        race_all = df["raca_label"].dropna()
        n_valid_race = int(race_all.shape[0])
        pct_valid_race = 100.0 * n_valid_race / n_all if n_all > 0 else np.nan

        if n_valid_race > 0:
            label_race = (
                f"Race, No. (%) [n = {_fmt_N(n_valid_race)}, "
                f"({pct_valid_race:.0f}%)]"
            )
        else:
            label_race = "Race, No. (%)"

        add_row(label_race, None)

        race_mapping = [
            ("Amarela", "Asian"),
            ("Branca", "White"),
            ("Indígena", "Indigenous"),
            ("Parda", "Mixed"),
            ("Preta", "Black"),
        ]

        for cat, label in race_mapping:
            valores = {}

            for g, gdf in groups.items():
                valores[g] = _format_count_pct(gdf["raca_label"], cat)

            add_row(label, valores)

    table = pd.DataFrame(rows)
    table = table[["Characteristics"] + col_names]

    print("[TABLE1_FA] Tabela 1 montada no formato final:", table.shape, flush=True)

    return table, n_all


def create_visuals(
    df_map,
    df_forms_dict,
    dictionary,
    quality_report,
    filepath,
    suffix,
    save_inputs,
):
    df_fa = _load_fa_view(year=2024)

    disp, n_all = _build_table1(df_fa)

    disp = disp.rename(columns={"Yellow fever": f"Yellow fever N = {_fmt_N(n_all)}"})

    table1 = idw.fig_table(
        disp,
        table_key="table1_fa",
        suffix=suffix,
        filepath=filepath,
        save_inputs=save_inputs,
        graph_label="Table 1",
        graph_about=(
            "Demographic characteristics of yellow fever cases. "
            "The febre_amarela dataset is assumed to already contain only "
            "yellow fever cases. Variables unavailable in the dataset, such as "
            "comorbidities, are omitted."
        ),
    )

    return [table1]