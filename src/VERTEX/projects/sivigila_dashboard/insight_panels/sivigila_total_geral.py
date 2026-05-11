import os
from typing import Any

import pandas as pd
from sqlalchemy import create_engine
import vertex.IsaricDraw as idw


# ============================================================
# SIVIGILA insight panel
# Bucket: TOTAL_GERAL
# Uses the views:
#   - sivigila.vw_dengue_incidencia_100k_new
#   - sivigila.vw_dengue_mortalidade_100k_new
#   - sivigila.vw_dengue_hospitalizacao_porcent_new
#   - sivigila.vw_dengue_letalidade_porcent_new
#   - sivigila.vw_dengue_porcent_idade_new
# ============================================================

BUCKET = "TOTAL_GERAL"
DISEASE_LABEL = "Total Geral"
PANEL_SLUG = "total_geral"

FAIXAS_ORDENADAS = [
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


def define_button():
    """Defines the button in the main dashboard menu."""
    return {"item": "SIVIGILA", "label": DISEASE_LABEL}


def _get_engine_from_env():
    """Create a PostgreSQL SQLAlchemy engine using PG* environment variables."""
    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD")
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    dbname = os.getenv("PGDATABASE")

    missing = [
        name
        for name, value in {
            "PGUSER": user,
            "PGPASSWORD": password,
            "PGDATABASE": dbname,
        }.items()
        if not value
    ]
    if missing:
        raise RuntimeError(
            "Database environment variables missing: " + ", ".join(missing)
        )

    if host in ("localhost", "127.0.0.1"):
        host = "127.0.0.1"

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(url, pool_pre_ping=True)


def _safe_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _fmt_int(value: Any) -> str:
    if pd.isna(value):
        return ""
    return f"{int(round(float(value))):,}".replace(",", ".")


def _fmt_num(value: Any, decimals: int = 2) -> str:
    if pd.isna(value):
        return ""
    return f"{float(value):,.{decimals}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _load_main_rates(engine) -> pd.DataFrame:
    """Load incidence, mortality, hospitalization and fatality rates by year."""
    sql = """
        WITH inc AS (
            SELECT
                ano,
                classi_bucket,
                casos_confirmados,
                populacao_total,
                incidencia_100k
            FROM sivigila.vw_dengue_incidencia_100k_new
            WHERE classi_bucket = %(bucket)s
        ),
        mort AS (
            SELECT
                ano,
                classi_bucket,
                obitos_dengue,
                taxa_mortalidade_100k
            FROM sivigila.vw_dengue_mortalidade_100k_new
            WHERE classi_bucket = %(bucket)s
        ),
        hosp AS (
            SELECT
                ano,
                classi_bucket,
                casos_dengue,
                casos_hosp,
                taxa_hospitalizacao_pct
            FROM sivigila.vw_dengue_hospitalizacao_porcent_new
            WHERE classi_bucket = %(bucket)s
        ),
        letal AS (
            SELECT
                ano,
                classi_bucket,
                obitos_dengue AS obitos_letalidade,
                casos_dengue AS casos_letalidade,
                taxa_letalidade_pct
            FROM sivigila.vw_dengue_letalidade_porcent_new
            WHERE classi_bucket = %(bucket)s
        )
        SELECT
            COALESCE(inc.ano, mort.ano, hosp.ano, letal.ano) AS ano,
            %(bucket)s AS classi_bucket,
            inc.casos_confirmados,
            inc.populacao_total,
            inc.incidencia_100k,
            mort.obitos_dengue,
            mort.taxa_mortalidade_100k,
            hosp.casos_hosp,
            hosp.taxa_hospitalizacao_pct,
            letal.taxa_letalidade_pct
        FROM inc
        FULL OUTER JOIN mort
            ON mort.ano = inc.ano
           AND mort.classi_bucket = inc.classi_bucket
        FULL OUTER JOIN hosp
            ON hosp.ano = COALESCE(inc.ano, mort.ano)
           AND hosp.classi_bucket = COALESCE(inc.classi_bucket, mort.classi_bucket)
        FULL OUTER JOIN letal
            ON letal.ano = COALESCE(inc.ano, mort.ano, hosp.ano)
           AND letal.classi_bucket = COALESCE(inc.classi_bucket, mort.classi_bucket, hosp.classi_bucket)
        ORDER BY ano;
    """
    df = pd.read_sql(sql, engine, params={"bucket": BUCKET})
    if df.empty:
        return df

    numeric_cols = [
        "casos_confirmados",
        "populacao_total",
        "incidencia_100k",
        "obitos_dengue",
        "taxa_mortalidade_100k",
        "casos_hosp",
        "taxa_hospitalizacao_pct",
        "taxa_letalidade_pct",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = _safe_numeric(df[col])
    df["ano"] = df["ano"].astype(int)
    return df


def _load_age_rates(engine) -> pd.DataFrame:
    """Load age-group rates and counts by year."""
    sql = """
        SELECT
            ano,
            classi_bucket,
            faixa_etaria,
            faixa_ordem,
            casos_confirmados,
            casos_hosp,
            obitos_dengue,
            taxa_hosp_pct,
            taxa_letalidade_pct
        FROM sivigila.vw_dengue_porcent_idade_new
        WHERE classi_bucket = %(bucket)s
          AND COALESCE(faixa_etaria, 'Ignorado') <> 'Ignorado'
        ORDER BY ano, faixa_ordem;
    """
    df = pd.read_sql(sql, engine, params={"bucket": BUCKET})
    if df.empty:
        return df

    df["ano"] = df["ano"].astype(str)
    df["faixa_etaria"] = pd.Categorical(
        df["faixa_etaria"], categories=FAIXAS_ORDENADAS, ordered=True
    )
    for col in [
        "casos_confirmados",
        "casos_hosp",
        "obitos_dengue",
        "taxa_hosp_pct",
        "taxa_letalidade_pct",
    ]:
        df[col] = _safe_numeric(df[col])
    return df


def _add_bar_chart(visuals, data, title, xlabel, ylabel, index_column, graph_label, graph_about, filepath, suffix, save_inputs):
    if data is None or data.empty:
        return
    fig, gid, glab, gabout = idw.fig_bar_chart(
        data=data,
        title=title,
        xlabel=xlabel,
        ylabel=ylabel,
        index_column=index_column,
        barmode="group",
        xaxis_tickformat="",
        suffix=suffix,
        filepath=filepath,
        save_inputs=save_inputs,
        graph_label=graph_label,
        graph_about=graph_about,
    )
    visuals.append((fig, gid, glab, gabout))


def _create_main_rate_visuals(visuals, df_main, filepath, suffix, save_inputs):
    if df_main.empty:
        return

    year_df = df_main.copy()
    year_df["ano"] = year_df["ano"].astype(str)

    _add_bar_chart(
        visuals,
        year_df[["ano", "incidencia_100k"]].rename(
            columns={"incidencia_100k": "Incidence rate per 100k"}
        ),
        f"{DISEASE_LABEL} incidence rate per 100k population",
        "Year",
        "Cases per 100k inhabitants",
        "ano",
        f"{DISEASE_LABEL} incidence rate",
        f"Annual {DISEASE_LABEL} incidence rate per 100k inhabitants, using static Colombia population for 2023 and 2024.",
        filepath,
        suffix,
        save_inputs,
    )

    _add_bar_chart(
        visuals,
        year_df[["ano", "taxa_mortalidade_100k"]].rename(
            columns={"taxa_mortalidade_100k": "Mortality rate per 100k"}
        ),
        f"{DISEASE_LABEL} mortality rate per 100k population",
        "Year",
        "Deaths per 100k inhabitants",
        "ano",
        f"{DISEASE_LABEL} mortality rate",
        f"Annual {DISEASE_LABEL} mortality rate per 100k inhabitants, using static Colombia population for 2023 and 2024.",
        filepath,
        suffix,
        save_inputs,
    )

    _add_bar_chart(
        visuals,
        year_df[["ano", "taxa_hospitalizacao_pct"]].rename(
            columns={"taxa_hospitalizacao_pct": "Hospitalization rate (%)"}
        ),
        f"{DISEASE_LABEL} hospitalization rate (%)",
        "Year",
        "%",
        "ano",
        f"{DISEASE_LABEL} hospitalization rate",
        f"Share of {DISEASE_LABEL} cases with hospitalization flag equal to 1, by year.",
        filepath,
        suffix,
        save_inputs,
    )

    _add_bar_chart(
        visuals,
        year_df[["ano", "taxa_letalidade_pct"]].rename(
            columns={"taxa_letalidade_pct": "Case fatality rate (%)"}
        ),
        f"{DISEASE_LABEL} case fatality rate (%)",
        "Year",
        "%",
        "ano",
        f"{DISEASE_LABEL} case fatality rate",
        f"Share of {DISEASE_LABEL} cases with death outcome, by year.",
        filepath,
        suffix,
        save_inputs,
    )


def _create_age_rate_visuals(visuals, df_age, filepath, suffix, save_inputs):
    if df_age.empty:
        return

    df_age = df_age.sort_values(["ano", "faixa_etaria"])

    df_age_total = df_age.groupby("faixa_etaria", as_index=False, observed=True)[
        ["casos_confirmados", "casos_hosp", "obitos_dengue"]
    ].sum()
    df_age_total["faixa_etaria"] = pd.Categorical(
        df_age_total["faixa_etaria"], categories=FAIXAS_ORDENADAS, ordered=True
    )
    df_age_total = df_age_total.sort_values("faixa_etaria")
    df_age_total["taxa_hosp_pct"] = (
        df_age_total["casos_hosp"]
        / df_age_total["casos_confirmados"].where(df_age_total["casos_confirmados"] > 0)
    ) * 100.0
    df_age_total["taxa_letalidade_pct"] = (
        df_age_total["obitos_dengue"]
        / df_age_total["casos_confirmados"].where(df_age_total["casos_confirmados"] > 0)
    ) * 100.0

    _add_bar_chart(
        visuals,
        df_age_total[["faixa_etaria", "casos_confirmados"]].rename(
            columns={"casos_confirmados": "Confirmed cases"}
        ),
        f"{DISEASE_LABEL} confirmed cases by age group, total period",
        "Age group",
        "Number of confirmed cases",
        "faixa_etaria",
        f"{DISEASE_LABEL} cases by age group, total period",
        f"Total number of {DISEASE_LABEL} cases for all available years in the SIVIGILA views, by age group.",
        filepath,
        suffix,
        save_inputs,
    )

    _add_bar_chart(
        visuals,
        df_age_total[["faixa_etaria", "taxa_hosp_pct"]].rename(
            columns={"taxa_hosp_pct": "Hospitalization rate (%)"}
        ),
        f"{DISEASE_LABEL} hospitalization rate (%) by age group, total period",
        "Age group",
        "Hospitalization rate (%)",
        "faixa_etaria",
        f"{DISEASE_LABEL} hospitalization rate by age group, total period",
        f"Hospitalization rate among {DISEASE_LABEL} cases for all available years, by age group.",
        filepath,
        suffix,
        save_inputs,
    )

    _add_bar_chart(
        visuals,
        df_age_total[["faixa_etaria", "taxa_letalidade_pct"]].rename(
            columns={"taxa_letalidade_pct": "Case fatality rate (%)"}
        ),
        f"{DISEASE_LABEL} case fatality rate (%) by age group, total period",
        "Age group",
        "Case fatality rate (%)",
        "faixa_etaria",
        f"{DISEASE_LABEL} case fatality rate by age group, total period",
        f"Case fatality rate among {DISEASE_LABEL} cases for all available years, by age group.",
        filepath,
        suffix,
        save_inputs,
    )

    df_cases_age = df_age.pivot(index="ano", columns="faixa_etaria", values="casos_confirmados").reset_index()
    df_cases_age.columns.name = None
    _add_bar_chart(
        visuals,
        df_cases_age,
        f"{DISEASE_LABEL} confirmed cases by year and age group",
        "Year",
        "Number of confirmed cases",
        "ano",
        f"{DISEASE_LABEL} cases by year and age group",
        f"Number of {DISEASE_LABEL} cases by year and age group.",
        filepath,
        suffix,
        save_inputs,
    )

    df_hosp_age = df_age.pivot(index="ano", columns="faixa_etaria", values="taxa_hosp_pct").reset_index()
    df_hosp_age.columns.name = None
    _add_bar_chart(
        visuals,
        df_hosp_age,
        f"{DISEASE_LABEL} hospitalization rate (%) by year and age group",
        "Year",
        "Hospitalization rate (%)",
        "ano",
        f"{DISEASE_LABEL} hospitalization rate by year and age group",
        f"Hospitalization rate among {DISEASE_LABEL} cases by year and age group.",
        filepath,
        suffix,
        save_inputs,
    )

    df_letal_age = df_age.pivot(index="ano", columns="faixa_etaria", values="taxa_letalidade_pct").reset_index()
    df_letal_age.columns.name = None
    _add_bar_chart(
        visuals,
        df_letal_age,
        f"{DISEASE_LABEL} case fatality rate (%) by year and age group",
        "Year",
        "Case fatality rate (%)",
        "ano",
        f"{DISEASE_LABEL} case fatality rate by year and age group",
        f"Case fatality rate among {DISEASE_LABEL} cases by year and age group.",
        filepath,
        suffix,
        save_inputs,
    )


def _build_summary_table(df_main: pd.DataFrame) -> pd.DataFrame:
    if df_main.empty:
        return pd.DataFrame()

    rows = []
    for _, row in df_main.sort_values("ano").iterrows():
        rows.append(
            {
                "Year": str(int(row["ano"])),
                "Cases": _fmt_int(row.get("casos_confirmados")),
                "Population": _fmt_int(row.get("populacao_total")),
                "Incidence / 100k": _fmt_num(row.get("incidencia_100k"), 2),
                "Hospitalized cases": _fmt_int(row.get("casos_hosp")),
                "Hospitalization rate (%)": _fmt_num(row.get("taxa_hospitalizacao_pct"), 2),
                "Deaths": _fmt_int(row.get("obitos_dengue")),
                "Mortality / 100k": _fmt_num(row.get("taxa_mortalidade_100k"), 4),
                "Case fatality rate (%)": _fmt_num(row.get("taxa_letalidade_pct"), 4),
            }
        )

    total_cases = df_main["casos_confirmados"].sum(min_count=1)
    total_pop = df_main["populacao_total"].sum(min_count=1)
    total_hosp = df_main["casos_hosp"].sum(min_count=1)
    total_deaths = df_main["obitos_dengue"].sum(min_count=1)

    incidence_total = 100000.0 * total_cases / total_pop if pd.notna(total_cases) and pd.notna(total_pop) and total_pop > 0 else pd.NA
    mortality_total = 100000.0 * total_deaths / total_pop if pd.notna(total_deaths) and pd.notna(total_pop) and total_pop > 0 else pd.NA
    hosp_rate_total = 100.0 * total_hosp / total_cases if pd.notna(total_hosp) and pd.notna(total_cases) and total_cases > 0 else pd.NA
    fatality_total = 100.0 * total_deaths / total_cases if pd.notna(total_deaths) and pd.notna(total_cases) and total_cases > 0 else pd.NA

    rows.append(
        {
            "Year": "Total period",
            "Cases": _fmt_int(total_cases),
            "Population": _fmt_int(total_pop),
            "Incidence / 100k": _fmt_num(incidence_total, 2),
            "Hospitalized cases": _fmt_int(total_hosp),
            "Hospitalization rate (%)": _fmt_num(hosp_rate_total, 2),
            "Deaths": _fmt_int(total_deaths),
            "Mortality / 100k": _fmt_num(mortality_total, 4),
            "Case fatality rate (%)": _fmt_num(fatality_total, 4),
        }
    )

    return pd.DataFrame(rows)


def _create_summary_table(visuals, df_main, filepath, suffix, save_inputs):
    table_df = _build_summary_table(df_main)
    if table_df.empty:
        return
    table = idw.fig_table(
        table_df,
        table_key=f"sivigila_summary_{PANEL_SLUG}",
        suffix=suffix,
        filepath=filepath,
        save_inputs=save_inputs,
        graph_label=f"{DISEASE_LABEL} descriptive summary",
        graph_about=(
            f"Descriptive annual summary for {DISEASE_LABEL}, including cases, population, incidence, hospitalization, deaths, mortality and fatality rates."
        ),
    )
    visuals.append(table)


def create_visuals(df_map, df_forms_dict, dictionary, quality_report, filepath, suffix, save_inputs):
    """Create all visuals in the insight panel."""
    visuals = []
    engine = _get_engine_from_env()

    df_main = _load_main_rates(engine)
    df_age = _load_age_rates(engine)

    _create_main_rate_visuals(visuals, df_main, filepath, suffix, save_inputs)
    _create_age_rate_visuals(visuals, df_age, filepath, suffix, save_inputs)
    _create_summary_table(visuals, df_main, filepath, suffix, save_inputs)

    return tuple(visuals)
