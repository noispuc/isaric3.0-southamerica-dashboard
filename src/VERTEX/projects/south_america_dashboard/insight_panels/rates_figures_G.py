import os
import pandas as pd
from sqlalchemy import create_engine

import vertex.IsaricDraw as idw

totais_ano = {
        "2017": 204_703_445,
        "2018": 206_107_260,
        "2019": 207_455_459,
        "2020": 208_660_842,
        "2021": 209_550_294,
        "2022": 210_306_414,
        "2023": 211_140_729,
    }

def define_button():
    """Defines the button in the main dashboard menu"""
    button_item = "Rates"
    button_label = "Dengue with Serious Signs Rates"
    output = {"item": button_item, "label": button_label}
    return output

def _get_anos_disponiveis(engine) -> list[int]:
    """
    Lê os anos disponíveis na tabela sinan.casos.
    """
    sql = "SELECT DISTINCT ano FROM sinan.casos ORDER BY ano;"
    df = pd.read_sql(sql, engine)
    return df["ano"].astype(int).tolist()

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

def _load_taxa_hosp_dengue_ano(engine, ano: int) -> pd.DataFrame:
    """
    Taxa de hospitalização por dengue (%):
        hospitalizados / casos de dengue * 100

    - Casos de hospitalização por dengue: hospitaliz = 1
    - Casos de dengue: classi_fin IN (12)
    """
    sql = """
        SELECT
            ano,
            COUNT(*) FILTER (
                WHERE classi_fin IN (12)
            ) AS casos_dengue,
            COUNT(*) FILTER (
                WHERE classi_fin IN (12) AND hospitaliz = 1
            ) AS casos_hosp
        FROM sinan.casos
        WHERE ano = %(ano)s
        GROUP BY ano
        ORDER BY ano;
    """

    df = pd.read_sql(sql, engine, params={"ano": ano})

    if df.empty:
        return df

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
    - denominador: casos de dengue (classi_fin IN (12))
    fórmula: (obitos / casos_confirmados) * 100
    """
    
    sql = """
        SELECT
            ano,
            COUNT(*) FILTER (
                WHERE classi_fin IN (12)
                  AND evolucao = 2
            ) AS obitos_dengue,
            COUNT(*) FILTER (
                WHERE classi_fin IN (12)
            ) AS casos_dengue
        FROM sinan.casos
        WHERE ano = %(ano)s
        GROUP BY ano
        ORDER BY ano;
    """
    df = pd.read_sql(sql, engine, params={"ano": ano})

    if df.empty:
        return df

    df["Taxa de letalidade (%)"] = df["obitos_dengue"] / df["casos_dengue"] * 100

    df_plot = df[["ano", "Taxa de letalidade (%)"]].copy()
    df_plot["ano"] = df_plot["ano"].astype(str)

    return df_plot


def create_visuals(df_map, df_forms_dict, dictionary, quality_report, filepath, suffix, save_inputs):
    """
    Create all visuals in the insight panel from the RAP dataframe
    """
    visuals = []
    engine = _get_engine_from_env()

    anos = _get_anos_disponiveis(engine)
    if not anos:
        return tuple(visuals)


    dfs_casos = []
    for ano in anos:
        df_ano = _load_taxas_dengue_ano(engine, ano=ano)
        if not df_ano.empty:
            dfs_casos.append(df_ano[["ano", "Dengue grave"]])

    if dfs_casos:
        df_casos_all = pd.concat(dfs_casos, ignore_index=True)
        df_casos_all.rename(
            columns={"Dengue grave": "Taxa de casos confirmados"},
            inplace=True,
        )

        fig_casos, gid_casos, glab_casos, gabout_casos = idw.fig_bar_chart(
            data=df_casos_all,
            title="Taxa de Casos Confirmados de Dengue por 100 mil hab.",
            xlabel="Ano",
            ylabel="Casos por 100 mil habitantes",
            index_column="ano",
            barmode="group",
            xaxis_tickformat="",
            suffix=f"{suffix}/taxa_casos_confirmados_todos_anos",
            filepath=filepath,
            save_inputs=save_inputs,
            graph_label="Taxa de casos confirmados de dengue",
            graph_about=(
                "Taxa de casos confirmados de dengue por 100 mil habitantes, "
                "para cada ano disponível, utilizando população anual como denominador."
            ),
        )
        visuals.append((fig_casos, gid_casos, glab_casos, gabout_casos))


    dfs_hosp = []
    for ano in anos:
        df_ano = _load_taxa_hosp_dengue_ano(engine, ano=ano)
        if not df_ano.empty:
            dfs_hosp.append(df_ano)

    if dfs_hosp:
        df_hosp_all = pd.concat(dfs_hosp, ignore_index=True)

        fig_hosp, gid_hosp, glab_hosp, gabout_hosp = idw.fig_bar_chart(
            data=df_hosp_all,
            title="Taxa de Hospitalização por Dengue (%)",
            xlabel="Ano",
            ylabel="%",
            index_column="ano",
            barmode="group",
            xaxis_tickformat="",
            suffix=f"{suffix}/taxa_hosp_dengue_todos_anos",
            filepath=filepath,
            save_inputs=save_inputs,
            graph_label="Taxa de hospitalização por dengue",
            graph_about=(
                "Proporção de casos de dengue confirmados (classi_fin 12) "
                "que tiveram hospitalização (hospitaliz = 1), por ano."
            ),
        )
        visuals.append((fig_hosp, gid_hosp, glab_hosp, gabout_hosp))


    dfs_letal = []
    for ano in anos:
        df_ano = _load_taxa_letalidade_ano(engine, ano=ano)
        if not df_ano.empty:
            dfs_letal.append(df_ano)

    if dfs_letal:
        df_letal_all = pd.concat(dfs_letal, ignore_index=True)

        fig_letal, gid_letal, glab_letal, gabout_letal = idw.fig_bar_chart(
            data=df_letal_all,
            title="Taxa de Letalidade por Dengue (%)",
            xlabel="Ano",
            ylabel="%",
            index_column="ano",
            barmode="group",
            xaxis_tickformat="",
            suffix=f"{suffix}/taxa_letalidade_dengue_todos_anos",
            filepath=filepath,
            save_inputs=save_inputs,
            graph_label="Taxa de letalidade por dengue",
            graph_about=(
                "Taxa de letalidade por dengue: óbitos por dengue (evolucao = 2) "
                "divididos pelos casos confirmados (classi_fin IN 12), por ano."
            ),
        )
        visuals.append((fig_letal, gid_letal, glab_letal, gabout_letal))

    return tuple(visuals)
