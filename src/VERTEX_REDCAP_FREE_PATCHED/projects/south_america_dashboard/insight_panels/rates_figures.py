import os
import pandas as pd
from sqlalchemy import create_engine

import vertex.IsaricDraw as idw

#################
# SQL FUNCTIONS #
#################
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
        SELECT ano, classi_bucket, incidencia_100k
        FROM sinan.vw_dengue_incidencia_100k
        WHERE ano = %(ano)s
        AND classi_bucket IN ('10', '11', '12', '10_11_12')
    """
    df = pd.read_sql(sql, engine, params={"ano": ano})
    if df.empty:
        return df

    wide = (
        df.pivot(index="ano", columns="classi_bucket", values="incidencia_100k")
        .reset_index()
        .fillna(0)
    )

    wide.rename(columns={
        "10": "Sem sinais de alarme",
        "11": "Com sinais de alarme",
        "12": "Dengue grave",
        "10_11_12": "Casos Confirmado total",
    }, inplace=True)

    df_plot = wide[["ano", "Sem sinais de alarme", "Com sinais de alarme", "Dengue grave", "Casos Confirmado total"]].copy()
    df_plot["ano"] = df_plot["ano"].astype(str)
    return df_plot

def _load_taxa_obito_ano(engine, ano: int) -> pd.DataFrame:
    """
    Calcula taxa de óbito por dengue por 100 mil notificações para um ano.
    Numerador: casos com classi_fin em (10,11,12) e evolucao = 2 (óbito por dengue).
    Denominador: total de registros em sinan.casos para o ano.
    """
    
    sql = """
        SELECT ano, taxa_mortalidade_100k
        FROM sinan.vw_dengue_mortalidade_100k
        WHERE ano = %(ano)s
        AND classi_bucket = '10_11_12'
    """
    df = pd.read_sql(sql, engine, params={"ano": ano})
    if df.empty:
        return df

    return pd.DataFrame({
        "ano": [str(int(df["ano"].iloc[0]))],
        "Taxa de óbito": [float(df["taxa_mortalidade_100k"].iloc[0])],
    })

def _load_taxa_hosp_dengue_ano(engine, ano: int) -> pd.DataFrame:
    """
    Taxa de hospitalização por dengue (%):
        hospitalizados / casos de dengue * 100

    - Casos de hospitalização por dengue: hospitaliz = 1
    - Casos de dengue: classi_fin IN (10, 11, 12)
    """
    
    sql = """
        SELECT ano, taxa_hospitalizacao_pct
        FROM sinan.vw_dengue_hospitalizacao_porcent
        WHERE ano = %(ano)s
        AND classi_bucket = '10_11_12'
    """
    df = pd.read_sql(sql, engine, params={"ano": ano})
    if df.empty:
        return df

    df_plot = pd.DataFrame({
        "ano": df["ano"].astype(str),
        "Taxa de hospitalização (%)": df["taxa_hospitalizacao_pct"].astype(float),
    })
    return df_plot

def _load_taxa_letalidade_ano(engine, ano: int) -> pd.DataFrame:
    """
    Taxa de letalidade por dengue (%):
    - numerador: óbitos por dengue (evolucao = 2)
    - denominador: casos de dengue (classi_fin IN (10, 11, 12))
    fórmula: (obitos / casos_confirmados) * 100
    """
    
    sql = """
        SELECT ano, taxa_letalidade_pct
        FROM sinan.vw_dengue_letalidade_porcent
        WHERE ano = %(ano)s
        AND classi_bucket = '10_11_12'
    """
    df = pd.read_sql(sql, engine, params={"ano": ano})
    if df.empty:
        return df

    df_plot = pd.DataFrame({
        "ano": df["ano"].astype(str),
        "Taxa de letalidade (%)": df["taxa_letalidade_pct"].astype(float),
    })
    return df_plot

####################
# VERTEX FUNCTIONS #
####################
def define_button():
    """Defines the button in the main dashboard menu"""
    button_item = "Rates"
    button_label = "Main Rates"
    output = {"item": button_item, "label": button_label}
    return output


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
            dfs_casos.append(df_ano[["ano", "Casos Confirmado total"]])

    if dfs_casos:
        df_casos_all = pd.concat(dfs_casos, ignore_index=True)
        df_casos_all.rename(
            columns={"Casos Confirmado total": "Taxa de casos confirmados"},
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

    
    dfs_obito = []
    for ano in anos:
        df_ano = _load_taxa_obito_ano(engine, ano=ano)
        if not df_ano.empty:
            dfs_obito.append(df_ano)

    if dfs_obito:
        df_obito_all = pd.concat(dfs_obito, ignore_index=True)

        fig_obito, gid_obito, glab_obito, gabout_obito = idw.fig_bar_chart(
            data=df_obito_all,
            title="Taxa de Óbito por Dengue por 100 mil hab.",
            xlabel="Ano",
            ylabel="Óbitos por 100 mil habitantes",
            index_column="ano",
            barmode="group",
            xaxis_tickformat="",
            suffix=f"{suffix}/taxa_obito_dengue_todos_anos",
            filepath=filepath,
            save_inputs=save_inputs,
            graph_label="Taxa de óbito por dengue",
            graph_about=(
                "Taxa de óbitos por dengue por 100 mil habitantes, "
                "para cada ano disponível, usando população anual como denominador."
            ),
        )
        visuals.append((fig_obito, gid_obito, glab_obito, gabout_obito))

    
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
                "Proporção de casos de dengue confirmados (classi_fin 10, 11, 12) "
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
                "divididos pelos casos confirmados (classi_fin IN 10, 11, 12), por ano."
            ),
        )
        visuals.append((fig_letal, gid_letal, glab_letal, gabout_letal))

    return tuple(visuals)
