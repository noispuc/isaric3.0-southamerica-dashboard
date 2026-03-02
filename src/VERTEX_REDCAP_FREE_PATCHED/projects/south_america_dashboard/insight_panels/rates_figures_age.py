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
    user = os.getenv("PGUSER", "postgres")
    password = os.getenv("PGPASSWORD", "")
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    dbname = os.getenv("PGDATABASE", "datasus")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(url)


def _load_rates_by_age(engine) -> pd.DataFrame:
    """
    Retorna, para TODOS os anos disponíveis:

      - casos_confirmados
      - casos_hosp
      - obitos_dengue
      - taxa_hosp_pct
      - taxa_letalidade_pct

    Tudo agregado por (ano, faixa_etaria).

    Fonte: view sinan.vw_porcent_idade (conforme padronização do banco).
    """

    sql = """
        SELECT
            ano,
            faixa_etaria,
            faixa_ordem,
            casos_confirmados,
            casos_hosp,
            obitos_dengue,
            taxa_hosp_pct,
            taxa_letalidade_pct
        FROM sinan.vw_porcent_idade
        WHERE classi_bucket = '10_11_12'
          AND faixa_etaria <> 'Ignorado'
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
    df["taxa_hosp_pct"] = df["taxa_hosp_pct"].astype(float)

    return df


def define_button():
    """Defines the button in the main dashboard menu"""
    button_item = "Rates"
    button_label = "Age Rates"
    output = {"item": button_item, "label": button_label}
    return output


def create_visuals(df_map, df_forms_dict, dictionary, quality_report, filepath, suffix, save_inputs):
    """
    Create all visuals in the insight panel from the RAP dataframe
    """
    visuals = []
    engine = _get_engine_from_env()

    # ------------------------------------------------------------------
    # Casos, taxa de hospitalização e taxa de letalidade por faixa etária
    # ------------------------------------------------------------------
    df_age = _load_rates_by_age(engine)
    if not df_age.empty:
        # garante ordenação por ano e faixa
        df_age = df_age.sort_values(["ano", "faixa_etaria"])

        # ------------------------------------------------------------------
        # 5.0 – Consolidado 2017–2023 por faixa etária (totais do período)
        # ------------------------------------------------------------------
        df_age_total = (
            df_age
            .groupby("faixa_etaria", as_index=False, observed=True)[["casos_confirmados", "casos_hosp", "obitos_dengue"]]
            .sum()
        )

        # garante ordem das faixas
        df_age_total["faixa_etaria"] = pd.Categorical(
            df_age_total["faixa_etaria"],
            categories=FAIXAS_ORDENADAS,
            ordered=True,
        )
        df_age_total = df_age_total.sort_values("faixa_etaria")

        # recalcula taxas no período todo (ponderadas pelos casos)
        df_age_total["taxa_hosp_pct"] = (
            df_age_total["casos_hosp"] / df_age_total["casos_confirmados"].where(df_age_total["casos_confirmados"] > 0)
        ) * 100.0

        df_age_total["taxa_letal_pct"] = (
            df_age_total["obitos_dengue"] / df_age_total["casos_confirmados"].where(df_age_total["casos_confirmados"] > 0)
        ) * 100.0

        # ---------- 5.0.1 Casos confirmados totais por faixa ----------
        df_cases_total = (
            df_age_total[["faixa_etaria", "casos_confirmados"]]
            .rename(columns={"casos_confirmados": "Casos confirmados"})
        )

        fig_casos_total, gid_casos_total, glab_casos_total, gabout_casos_total = idw.fig_bar_chart(
            data=df_cases_total,
            title="Casos confirmados de dengue por faixa etária (total 2017–2023)",
            xlabel="Faixa etária",
            ylabel="Número de casos confirmados",
            index_column="faixa_etaria",
            barmode="group",
            xaxis_tickformat="",
            suffix=f"{suffix}/casos_confirmados_por_faixa_total",
            filepath=filepath,
            save_inputs=save_inputs,
            graph_label="Casos confirmados de dengue por faixa etária (total 2017–2023)",
            graph_about=(
                "Número total de casos confirmados de dengue (10, 11, 12) "
                "no período 2017–2023, por faixa etária."
            ),
        )
        visuals.append((fig_casos_total, gid_casos_total, glab_casos_total, gabout_casos_total))

        # ---------- 5.0.2 Taxa de hospitalização total por faixa ----------
        df_hosp_total = (
            df_age_total[["faixa_etaria", "taxa_hosp_pct"]]
            .rename(columns={"taxa_hosp_pct": "Taxa de hospitalização (%)"})
        )

        fig_hosp_total, gid_hosp_total, glab_hosp_total, gabout_hosp_total = idw.fig_bar_chart(
            data=df_hosp_total,
            title="Taxa de hospitalização por dengue (%) por faixa etária (total 2017–2023)",
            xlabel="Faixa etária",
            ylabel="Taxa de hospitalização (%)",
            index_column="faixa_etaria",
            barmode="group",
            xaxis_tickformat="",
            suffix=f"{suffix}/taxa_hosp_por_faixa_total",
            filepath=filepath,
            save_inputs=save_inputs,
            graph_label="Taxa de hospitalização por dengue por faixa etária (total 2017–2023)",
            graph_about=(
                "Taxa de hospitalização entre os casos confirmados de dengue (10, 11, 12), "
                "no período 2017–2023, por faixa etária."
            ),
        )
        visuals.append((fig_hosp_total, gid_hosp_total, glab_hosp_total, gabout_hosp_total))

        # ---------- 5.0.3 Taxa de letalidade total por faixa ----------
        df_letal_total = (
            df_age_total[["faixa_etaria", "taxa_letal_pct"]]
            .rename(columns={"taxa_letal_pct": "Taxa de letalidade (%)"})
        )

        fig_letal_total, gid_letal_total, glab_letal_total, gabout_letal_total = idw.fig_bar_chart(
            data=df_letal_total,
            title="Taxa de letalidade por dengue (%) por faixa etária (total 2017–2023)",
            xlabel="Faixa etária",
            ylabel="Taxa de letalidade (%)",
            index_column="faixa_etaria",
            barmode="group",
            xaxis_tickformat="",
            suffix=f"{suffix}/taxa_letalidade_por_faixa_total",
            filepath=filepath,
            save_inputs=save_inputs,
            graph_label="Taxa de letalidade por dengue por faixa etária (total 2017–2023)",
            graph_about=(
                "Taxa de letalidade por dengue (óbitos entre casos confirmados) "
                "no período 2017–2023, por faixa etária."
            ),
        )
        visuals.append((fig_letal_total, gid_letal_total, glab_letal_total, gabout_letal_total))

        # ------------------------------------------------------------------
        # Séries anuais por faixa etária
        # ------------------------------------------------------------------

        # ---------- 5.1 Casos confirmados por faixa etária ----------
        df_cases_age = (
            df_age
            .pivot(index="ano", columns="faixa_etaria", values="casos_confirmados")
            .reset_index()
        )
        df_cases_age.columns.name = None  # tira o nome do eixo de colunas

        fig_casos_age, gid_casos_age, glab_casos_age, gabout_casos_age = idw.fig_bar_chart(
            data=df_cases_age,
            title="Casos confirmados de dengue por faixa etária",
            xlabel="Ano",
            ylabel="Número de casos confirmados",
            index_column="ano",
            barmode="group",
            xaxis_tickformat="",
            suffix=f"{suffix}/casos_confirmados_por_faixa",
            filepath=filepath,
            save_inputs=save_inputs,
            graph_label="Casos confirmados de dengue por faixa etária",
            graph_about=(
                "Número de casos confirmados de dengue (10, 11, 12) "
                "por ano e faixa etária."
            ),
        )
        visuals.append((fig_casos_age, gid_casos_age, glab_casos_age, gabout_casos_age))

        # ---------- 5.2 Taxa de hospitalização (%) por faixa etária ----------
        df_hosp_age = (
            df_age
            .pivot(index="ano", columns="faixa_etaria", values="taxa_hosp_pct")
            .reset_index()
        )
        df_hosp_age.columns.name = None

        fig_hosp_age, gid_hosp_age, glab_hosp_age, gabout_hosp_age = idw.fig_bar_chart(
            data=df_hosp_age,
            title="Taxa de hospitalização por dengue (%) por faixa etária",
            xlabel="Ano",
            ylabel="Taxa de hospitalização (%)",
            index_column="ano",
            barmode="group",
            xaxis_tickformat="",
            suffix=f"{suffix}/taxa_hosp_por_faixa",
            filepath=filepath,
            save_inputs=save_inputs,
            graph_label="Taxa de hospitalização por dengue por faixa etária",
            graph_about=(
                "Proporção de casos confirmados de dengue (10, 11, 12) "
                "que evoluíram com hospitalização (hospitaliz = 1), por ano e faixa etária."
            ),
        )
        visuals.append((fig_hosp_age, gid_hosp_age, glab_hosp_age, gabout_hosp_age))

        # ---------- 5.3 Taxa de letalidade (%) por faixa etária ----------
        df_letal_age = (
            df_age
            .pivot(index="ano", columns="faixa_etaria", values="taxa_letal_pct")
            .reset_index()
        )
        df_letal_age.columns.name = None

        fig_letal_age, gid_letal_age, glab_letal_age, gabout_letal_age = idw.fig_bar_chart(
            data=df_letal_age,
            title="Taxa de letalidade por dengue (%) por faixa etária",
            xlabel="Ano",
            ylabel="Taxa de letalidade (%)",
            index_column="ano",
            barmode="group",
            xaxis_tickformat="",
            suffix=f"{suffix}/taxa_letalidade_por_faixa",
            filepath=filepath,
            save_inputs=save_inputs,
            graph_label="Taxa de letalidade por dengue por faixa etária",
            graph_about=(
                "Taxa de letalidade por dengue: óbitos por dengue (evolucao = 2) "
                "divididos pelos casos confirmados (10, 11, 12), "
                "por ano e faixa etária."
            ),
        )
        visuals.append((fig_letal_age, gid_letal_age, glab_letal_age, gabout_letal_age))

    return tuple(visuals)
