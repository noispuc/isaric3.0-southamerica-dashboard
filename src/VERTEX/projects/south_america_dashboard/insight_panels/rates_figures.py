import os
import pandas as pd
from sqlalchemy import create_engine

import vertex.IsaricDraw as idw


def define_button():
    """Defines the button in the main dashboard menu"""
    button_item = "Figures"
    button_label = "Database Rates"
    output = {"item": button_item, "label": button_label}
    return output


# --- helpers que eu acrescentaria acima do create_visuals ---

def _get_engine_from_env():
    """Cria uma engine PostgreSQL usando variáveis de ambiente (PGHOST, etc.)."""
    user = os.getenv("PGUSER", "postgres")
    password = os.getenv("PGPASSWORD", "")
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    dbname = os.getenv("PGDATABASE", "datasus")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(url)


def _load_taxas_dengue_ano(engine, ano: int) -> pd.DataFrame:
    """
    Lê do sinan.casos os casos confirmados de dengue por forma clínica
    e calcula a taxa por 100 mil notificações (total de linhas daquele ano).
    """
    
    totais_ano = {
        "2017": 207_353_391,
        "2018": 208_494_900,
        "2019": 210_147_125,
        "2020": 211_755_692,
        "2021": 212_559_417,
        "2022": 213_798_492,
        "2023": 214_748_364,
        "2024": 215_500_000,
    }
    
    sql = """
        WITH base AS (
            SELECT
                ano,
                COUNT(*) AS total_notificacoes,
                COUNT(*) FILTER (WHERE classi_fin IN (10, 11, 12)) AS casos_confirmados_total,
                COUNT(*) FILTER (WHERE classi_fin = 10)            AS casos_sem_sinais,
                COUNT(*) FILTER (WHERE classi_fin = 11)            AS casos_com_sinais,
                COUNT(*) FILTER (WHERE classi_fin = 12)            AS casos_graves
            FROM sinan.casos
            WHERE ano = %(ano)s
            GROUP BY ano
        )
        SELECT
            ano,
            total_notificacoes,
            casos_confirmados_total,
            casos_sem_sinais,
            casos_com_sinais,
            casos_graves
        FROM base;
    """

    df = pd.read_sql(sql, engine, params={"ano": ano})
    if df.empty:
        return df

    total = float(df["total_notificacoes"].iloc[0])

    df["Sem sinais de alarme"] = (df["casos_sem_sinais"] / totais_ano[str(ano)]) * 100_000
    df["Com sinais de alarme"] = (df["casos_com_sinais"] / totais_ano[str(ano)]) * 100_000
    df["Dengue grave"]         = (df["casos_graves"]     / totais_ano[str(ano)]) * 100_000
    df["Casos Confirmado total"] = (df["casos_confirmados_total"] / totais_ano[str(ano)]) * 100_000

    df_plot = df[["ano", "Sem sinais de alarme", "Com sinais de alarme", "Dengue grave", "Casos Confirmado total"]].copy()
    df_plot["ano"] = df_plot["ano"].astype(str)
    return df_plot

def _load_taxa_obito_ano(engine, ano: int) -> pd.DataFrame:
    """
    Calcula taxa de óbito por dengue por 100 mil notificações para um ano.
    Numerador: casos com classi_fin em (10,11,12) e evolucao = 2 (óbito por dengue).
    Denominador: total de registros em sinan.casos para o ano.
    """
    
    totais_ano = {
        "2017": 207_353_391,
        "2018": 208_494_900,
        "2019": 210_147_125,
        "2020": 211_755_692,
        "2021": 212_559_417,
        "2022": 213_798_492,
        "2023": 214_748_364,
        "2024": 215_500_000,
    }
    
    sql = """
        SELECT
            %(ano)s::int AS ano,
            COUNT(*) FILTER (
                WHERE classi_fin IN (10, 11, 12) AND evolucao = 2
            ) AS obitos_dengue,
            COUNT(*) AS total_notificacoes
        FROM sinan.casos
        WHERE ano = %(ano)s;
    """

    df = pd.read_sql(sql, engine, params={"ano": ano})
    if df.empty:
        return df

    total = float(df["total_notificacoes"].iloc[0])
    obitos = float(df["obitos_dengue"].iloc[0])

    if total == 0:
        taxa = 0.0
    else:
        total = totais_ano[str(ano)]
        taxa = obitos / total * 100_000

    df_plot = pd.DataFrame(
        {
            "ano": [str(ano)],
            "Taxa de óbito": [taxa],
        }
    )
    return df_plot

def _load_taxa_hosp_dengue_ano(engine, ano: int) -> pd.DataFrame:
    """
    Taxa de hospitalização por dengue (%):
        hospitalizados / casos de dengue * 100

    - Casos de hospitalização por dengue: hospitaliz = 1
    - Casos de dengue: classi_fin IN (10, 11, 12)
    """
    sql = """
        SELECT
            ano,
            COUNT(*) FILTER (
                WHERE classi_fin IN (10, 11, 12)
            ) AS casos_dengue,
            COUNT(*) FILTER (
                WHERE classi_fin IN (10, 11, 12) AND hospitaliz = 1
            ) AS casos_hosp
        FROM sinan.casos
        WHERE ano = %(ano)s
        GROUP BY ano
        ORDER BY ano;
    """

    df = pd.read_sql(sql, engine, params={"ano": ano})

    if df.empty:
        return df

    # taxa em porcentagem (%)
    df["Taxa de hospitalização (%)"] = (
        df["casos_hosp"] / df["casos_dengue"]
    ) * 100.0

    df_plot = df[["ano", "Taxa de hospitalização (%)"]].copy()
    df_plot["ano"] = df_plot["ano"].astype(str)

    return df_plot

def _load_taxa_letalidade_ano(engine, ano: int) -> pd.DataFrame:
    """
    Taxa de letalidade por dengue (%):
    - numerador: óbitos por dengue (evolucao = 2)
    - denominador: casos de dengue (classi_fin IN (10, 11, 12))
    fórmula: (obitos / casos_confirmados) * 100
    """
    
    sql = """
        SELECT
            ano,
            COUNT(*) FILTER (
                WHERE classi_fin IN (10, 11, 12)
                  AND evolucao = 2
            ) AS obitos_dengue,
            COUNT(*) FILTER (
                WHERE classi_fin IN (10, 11, 12)
            ) AS casos_dengue
        FROM sinan.casos
        WHERE ano = %(ano)s
        GROUP BY ano
        ORDER BY ano;
    """
    df = pd.read_sql(sql, engine, params={"ano": ano})

    if df.empty:
        return df

    # taxa de letalidade em %
    df["Taxa de letalidade (%)"] = df["obitos_dengue"] / df["casos_dengue"] * 100

    df_plot = df[["ano", "Taxa de letalidade (%)"]].copy()
    df_plot["ano"] = df_plot["ano"].astype(str)  # eixo X mais amigável

    return df_plot


def create_visuals(df_map, df_forms_dict, dictionary, quality_report, filepath, suffix, save_inputs):
    """
    Create all visuals in the insight panel from the RAP dataframe
    """

    visuals = []
    engine = _get_engine_from_env()

    df_taxas_2022 = _load_taxas_dengue_ano(engine, ano=2022)
    total_2022 = df_taxas_2022[["ano", "Casos Confirmado total"]]
    if not df_taxas_2022.empty:
        # 3) criar bar-chart empilhado
        fig, graph_id, graph_label, graph_about = idw.fig_bar_chart(
            data=total_2022,
            title="Taxa de Casos Confirmados de Dengue por 100 mil hab. (Brasil, 2022)",
            xlabel="Ano",
            ylabel="Casos por 100 mil habitantes",
            index_column="ano",
            barmode="stack",
            xaxis_tickformat="",  # não é data, é ano
            suffix=f"{suffix}/taxa_casos_confirmados_2022",
            filepath=filepath,
            save_inputs=save_inputs,
            graph_label="Taxa de casos confirmados de dengue (por forma clínica, 2022)",
            graph_about=(
                "Taxa de casos confirmados de dengue por 100 mil habitantes em 2022, "
                "segmentada por forma clínica (sem sinais de alarme, com sinais de alarme, dengue grave), "
                "com base nos registros do SINAN."
            ),
        )

        visuals.append((fig, graph_id, graph_label, graph_about))

    df_obito_2022 = _load_taxa_obito_ano(engine, ano=2022)
    if not df_obito_2022.empty:
        fig_obito, graph_id_obito, graph_label_obito, graph_about_obito = idw.fig_bar_chart(
            data=df_obito_2022,
            title="Taxa de Óbito por Dengue por 100 mil hab. (2022)",
            xlabel="Ano",
            ylabel="Óbitos por 100 mil hab.",
            index_column="ano",
            barmode="group",
            xaxis_tickformat="",
            suffix=f"{suffix}/taxa_obito_dengue_2022",
            filepath=filepath,
            save_inputs=save_inputs,
            graph_label="Taxa de óbito por dengue (por 100 mil hab.)",
            graph_about=(
                "Taxa de óbitos por dengue por 100 mil habitantes em 2022, "
                "calculada como (óbitos por dengue / total de notificações) * 100.000."
            ),
        )

        visuals.append((fig_obito, graph_id_obito, graph_label_obito, graph_about_obito))
        
    df_hosp_2022 = _load_taxa_hosp_dengue_ano(engine, ano=2022)
    if not df_hosp_2022.empty:
        fig, graph_id, graph_label, graph_about = idw.fig_bar_chart(
            data=df_hosp_2022,
            title="Taxa de Hospitalização por Dengue (%) (2022)",
            xlabel="Ano",
            ylabel="%",
            index_column="ano",
            barmode="group",       # só 1 série → tanto faz, deixei group
            xaxis_tickformat="",
            suffix=f"{suffix}/taxa_hosp_dengue_2022",
            filepath=filepath,
            save_inputs=save_inputs,
            graph_label="Taxa de hospitalização por dengue (%)",
            graph_about=(
                "Proporção de casos de dengue confirmados (classi_fin 10, 11, 12) "
                "que tiveram hospitalização (hospitaliz = 1) em 2022."
            ),
        )
        visuals.append((fig, graph_id, graph_label, graph_about))
        
    df_letalidade_2022 = _load_taxa_letalidade_ano(engine, ano=2022)
    if not df_letalidade_2022.empty:
        fig_letal, graph_id_letal, graph_label_letal, graph_about_letal = idw.fig_bar_chart(
            data=df_letalidade_2022,
            title="Taxa de Letalidade por Dengue (%) – 2022",
            xlabel="Ano",
            ylabel="%",
            index_column="ano",
            barmode="group",
            xaxis_tickformat="",
            suffix=f"{suffix}/taxa_letalidade_dengue_2022",
            filepath=filepath,
            save_inputs=save_inputs,
            graph_label="Taxa de letalidade por dengue (%)",
            graph_about=(
                "Taxa de letalidade por dengue em 2022: "
                "óbitos por dengue (evolucao = 2) divididos pelos casos confirmados "
                "(classi_fin IN 10, 11, 12), multiplicado por 100."
            ),
        )

        visuals.append((fig_letal, graph_id_letal, graph_label_letal, graph_about_letal))



    return tuple(visuals)
