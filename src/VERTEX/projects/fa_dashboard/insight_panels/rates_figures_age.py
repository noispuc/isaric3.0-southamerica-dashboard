import os
import pandas as pd
from sqlalchemy import create_engine
import vertex.IsaricDraw as idw


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


def _get_engine_from_env():
    """Cria uma engine PostgreSQL usando variáveis de ambiente (PGHOST, etc.)."""
    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD")
    host = os.getenv("PGHOST")
    port = os.getenv("PGPORT")
    dbname = os.getenv("PGDATABASE")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(url)


def _load_rates_by_age(engine) -> pd.DataFrame:
    """
    Retorna, para todos os anos disponíveis:
      - casos_confirmados
      - obitos_fa
      - taxa_letalidade_pct

    Tudo agregado por (ano, faixa_etaria).

    Adaptação para febre amarela:
    - A base já contém apenas casos de febre amarela.
    - Não há classi_fin na base disponibilizada.
    - Não há variável de hospitalização.
    - Óbito é tratado a partir da lógica já implementada nas views de FA.
    """
    sql = """
        SELECT
            ano,
            faixa_etaria,
            faixa_ordem,
            casos_confirmados,
            obitos_fa,
            taxa_letalidade_pct
        FROM febre_amarela.vw_porcent_idade
        WHERE faixa_etaria <> 'Ignorado'
        ORDER BY ano, faixa_ordem;
    """

    df = pd.read_sql(sql, engine)

    if df.empty:
        return df

    df["faixa_etaria"] = pd.Categorical(
        df["faixa_etaria"],
        categories=FAIXAS_ORDENADAS,
        ordered=True,
    )

    df["ano"] = df["ano"].astype(str)
    df["taxa_letal_pct"] = df["taxa_letalidade_pct"].astype(float)

    return df


def define_button():
    """Defines the button in the main dashboard menu."""
    button_item = "Rates"
    button_label = "Age Rates"

    return {"item": button_item, "label": button_label}


def create_visuals(
    df_map, df_forms_dict, dictionary, quality_report, filepath, suffix, save_inputs
):
    """
    Create all visuals in the insight panel.

    Visuals mantidos para febre amarela:
    - total de casos por faixa etária;
    - letalidade por faixa etária;
    - casos por ano e faixa etária;
    - letalidade por ano e faixa etária.

    Visuals removidos:
    - hospitalização por faixa etária, pois a base de FA não possui variável de hospitalização.
    """
    visuals = []

    engine = _get_engine_from_env()
    df_age = _load_rates_by_age(engine)

    if not df_age.empty:
        df_age = df_age.sort_values(["ano", "faixa_etaria"])

        df_age_total = df_age.groupby("faixa_etaria", as_index=False, observed=True)[
            ["casos_confirmados", "obitos_fa"]
        ].sum()

        df_age_total["faixa_etaria"] = pd.Categorical(
            df_age_total["faixa_etaria"],
            categories=FAIXAS_ORDENADAS,
            ordered=True,
        )

        df_age_total = df_age_total.sort_values("faixa_etaria")

        df_age_total["taxa_letal_pct"] = (
            df_age_total["obitos_fa"]
            / df_age_total["casos_confirmados"].where(
                df_age_total["casos_confirmados"] > 0
            )
        ) * 100.0

        # 1. Total de casos por faixa etária
        df_cases_total = df_age_total[["faixa_etaria", "casos_confirmados"]].rename(
            columns={"casos_confirmados": "Confirmed cases"}
        )

        fig_casos_total, gid_casos_total, glab_casos_total, gabout_casos_total = (
            idw.fig_bar_chart(
                data=df_cases_total,
                title="Confirmed yellow fever cases by age group (all years total)",
                xlabel="Age group",
                ylabel="Number of confirmed cases",
                index_column="faixa_etaria",
                barmode="group",
                xaxis_tickformat="",
                suffix=suffix,
                filepath=filepath,
                save_inputs=save_inputs,
                graph_label="Confirmed yellow fever cases by age group (all years total)",
                graph_about=(
                    "Total number of yellow fever cases across all available years, "
                    "by age group. The febre_amarela dataset is assumed to already "
                    "contain only yellow fever cases."
                ),
            )
        )

        visuals.append(
            (fig_casos_total, gid_casos_total, glab_casos_total, gabout_casos_total)
        )

        # 2. Letalidade total por faixa etária
        df_letal_total = df_age_total[["faixa_etaria", "taxa_letal_pct"]].rename(
            columns={"taxa_letal_pct": "Case fatality rate (%)"}
        )

        fig_letal_total, gid_letal_total, glab_letal_total, gabout_letal_total = (
            idw.fig_bar_chart(
                data=df_letal_total,
                title="Yellow fever case fatality rate (%) by age group (all years total)",
                xlabel="Age group",
                ylabel="Case fatality rate (%)",
                index_column="faixa_etaria",
                barmode="group",
                xaxis_tickformat="",
                suffix=suffix,
                filepath=filepath,
                save_inputs=save_inputs,
                graph_label="Yellow fever case fatality rate by age group (all years total)",
                graph_about=(
                    "Yellow fever case fatality rate across all available years, "
                    "calculated as deaths divided by cases, by age group."
                ),
            )
        )

        visuals.append(
            (fig_letal_total, gid_letal_total, glab_letal_total, gabout_letal_total)
        )

        # 3. Casos por ano e faixa etária
        df_cases_age = df_age.pivot(
            index="ano",
            columns="faixa_etaria",
            values="casos_confirmados",
        ).reset_index()

        df_cases_age.columns.name = None

        fig_casos_age, gid_casos_age, glab_casos_age, gabout_casos_age = (
            idw.fig_bar_chart(
                data=df_cases_age,
                title="Confirmed yellow fever cases by age group",
                xlabel="Year",
                ylabel="Number of confirmed cases",
                index_column="ano",
                barmode="group",
                xaxis_tickformat="",
                suffix=suffix,
                filepath=filepath,
                save_inputs=save_inputs,
                graph_label="Confirmed yellow fever cases by age group",
                graph_about=(
                    "Number of yellow fever cases by year and age group. "
                    "The febre_amarela dataset is assumed to already contain only "
                    "yellow fever cases."
                ),
            )
        )

        visuals.append((fig_casos_age, gid_casos_age, glab_casos_age, gabout_casos_age))

        # 4. Letalidade por ano e faixa etária
        df_letal_age = df_age.pivot(
            index="ano",
            columns="faixa_etaria",
            values="taxa_letal_pct",
        ).reset_index()

        df_letal_age.columns.name = None

        fig_letal_age, gid_letal_age, glab_letal_age, gabout_letal_age = (
            idw.fig_bar_chart(
                data=df_letal_age,
                title="Yellow fever case fatality rate (%) by age group",
                xlabel="Year",
                ylabel="Case fatality rate (%)",
                index_column="ano",
                barmode="group",
                xaxis_tickformat="",
                suffix=suffix,
                filepath=filepath,
                save_inputs=save_inputs,
                graph_label="Yellow fever case fatality rate by age group",
                graph_about=(
                    "Yellow fever case fatality rate by year and age group, "
                    "calculated as deaths divided by cases."
                ),
            )
        )

        visuals.append((fig_letal_age, gid_letal_age, glab_letal_age, gabout_letal_age))

    return tuple(visuals)