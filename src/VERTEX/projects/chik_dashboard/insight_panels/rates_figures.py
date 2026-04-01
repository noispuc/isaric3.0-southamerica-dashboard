import os
import pandas as pd
from sqlalchemy import create_engine
import vertex.IsaricDraw as idw


def _get_anos_disponiveis(engine) -> list[int]:
    """
    Lê os anos disponíveis na view de incidência de chikungunya.
    """
    sql = "SELECT DISTINCT ano FROM sinan_chik.vw_chik_incidencia_100k ORDER BY ano;"
    df = pd.read_sql(sql, engine)
    return df["ano"].astype(int).tolist()


def _get_engine_from_env():
    """Cria uma engine PostgreSQL usando variáveis de ambiente (PGHOST, etc.)."""
    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD")
    host = os.getenv("PGHOST")
    port = os.getenv("PGPORT")
    dbname = os.getenv("PGDATABASE")
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(url)


def _load_taxa_incidencia_ano(engine, ano: int) -> pd.DataFrame:
    """
    Lê a incidência de casos confirmados de chikungunya por 100 mil habitantes.
    Fonte: sinan_chik.vw_chik_incidencia_100k.
    """
    sql = """
        SELECT ano, incidencia_100k
        FROM sinan_chik.vw_chik_incidencia_100k
        WHERE ano = %(ano)s
    """
    df = pd.read_sql(sql, engine, params={"ano": ano})
    if df.empty:
        return df
    return pd.DataFrame(
        {
            "ano": df["ano"].astype(str),
            "Confirmed cases rate": df["incidencia_100k"].astype(float),
        }
    )


def _load_taxa_obito_ano(engine, ano: int) -> pd.DataFrame:
    """
    Taxa de mortalidade por chikungunya por 100 mil habitantes.
    Óbito considera evolucao IN (2, 3, 4).
    Fonte: sinan_chik.vw_chik_mortalidade_100k.
    """
    sql = """
        SELECT ano, taxa_mortalidade_100k
        FROM sinan_chik.vw_chik_mortalidade_100k
        WHERE ano = %(ano)s
    """
    df = pd.read_sql(sql, engine, params={"ano": ano})
    if df.empty:
        return df
    return pd.DataFrame(
        {
            "ano": df["ano"].astype(str),
            "Mortality rate": df["taxa_mortalidade_100k"].astype(float),
        }
    )


def _load_taxa_hosp_ano(engine, ano: int) -> pd.DataFrame:
    """
    Taxa de hospitalização por chikungunya (%).
    Fonte: sinan_chik.vw_chik_hospitalizacao_porcent.
    """
    sql = """
        SELECT ano, taxa_hospitalizacao_pct
        FROM sinan_chik.vw_chik_hospitalizacao_porcent
        WHERE ano = %(ano)s
    """
    df = pd.read_sql(sql, engine, params={"ano": ano})
    if df.empty:
        return df
    return pd.DataFrame(
        {
            "ano": df["ano"].astype(str),
            "Hospitalization rate (%)": df["taxa_hospitalizacao_pct"].astype(float),
        }
    )


def _load_taxa_letalidade_ano(engine, ano: int) -> pd.DataFrame:
    """
    Taxa de letalidade por chikungunya (%).
    Óbito considera evolucao IN (2, 3, 4).
    Fonte: sinan_chik.vw_chik_letalidade_porcent.
    """
    sql = """
        SELECT ano, taxa_letalidade_pct
        FROM sinan_chik.vw_chik_letalidade_porcent
        WHERE ano = %(ano)s
    """
    df = pd.read_sql(sql, engine, params={"ano": ano})
    if df.empty:
        return df
    return pd.DataFrame(
        {
            "ano": df["ano"].astype(str),
            "Case fatality rate (%)": df["taxa_letalidade_pct"].astype(float),
        }
    )


def define_button():
    """Defines the button in the main dashboard menu"""
    button_item = "Rates"
    button_label = "Main Rates"
    output = {"item": button_item, "label": button_label}
    return output


def create_visuals(
    df_map, df_forms_dict, dictionary, quality_report, filepath, suffix, save_inputs
):
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
        df_ano = _load_taxa_incidencia_ano(engine, ano=ano)
        if not df_ano.empty:
            dfs_casos.append(df_ano)
    if dfs_casos:
        df_casos_all = pd.concat(dfs_casos, ignore_index=True)
        fig_casos, gid_casos, glab_casos, gabout_casos = idw.fig_bar_chart(
            data=df_casos_all,
            title="Confirmed Chikungunya Cases Rate per 100k pop.",
            xlabel="Year",
            ylabel="Cases per 100k inhabitants",
            index_column="ano",
            barmode="group",
            xaxis_tickformat="",
            suffix=suffix,
            filepath=filepath,
            save_inputs=save_inputs,
            graph_label="Confirmed chikungunya cases rate",
            graph_about=(
                "Confirmed chikungunya cases rate per 100k inhabitants, "
                "for each available year, using annual population as denominator."
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
            title="Chikungunya Mortality Rate per 100k pop.",
            xlabel="Year",
            ylabel="Deaths per 100k inhabitants",
            index_column="ano",
            barmode="group",
            xaxis_tickformat="",
            suffix=suffix,
            filepath=filepath,
            save_inputs=save_inputs,
            graph_label="Chikungunya mortality rate",
            graph_about=(
                "Chikungunya mortality rate per 100k inhabitants, "
                "for each available year, using annual population as denominator."
            ),
        )
        visuals.append((fig_obito, gid_obito, glab_obito, gabout_obito))

    dfs_hosp = []
    for ano in anos:
        df_ano = _load_taxa_hosp_ano(engine, ano=ano)
        if not df_ano.empty:
            dfs_hosp.append(df_ano)
    if dfs_hosp:
        df_hosp_all = pd.concat(dfs_hosp, ignore_index=True)
        fig_hosp, gid_hosp, glab_hosp, gabout_hosp = idw.fig_bar_chart(
            data=df_hosp_all,
            title="Chikungunya Hospitalization Rate (%)",
            xlabel="Year",
            ylabel="%",
            index_column="ano",
            barmode="group",
            xaxis_tickformat="",
            suffix=suffix,
            filepath=filepath,
            save_inputs=save_inputs,
            graph_label="Chikungunya hospitalization rate",
            graph_about=(
                "Proportion of confirmed chikungunya cases (classi_fin = 13) "
                "with hospitalization (hospitaliz = 1), by year."
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
            title="Chikungunya Case Fatality Rate (%)",
            xlabel="Year",
            ylabel="%",
            index_column="ano",
            barmode="group",
            xaxis_tickformat="",
            suffix=suffix,
            filepath=filepath,
            save_inputs=save_inputs,
            graph_label="Chikungunya case fatality rate",
            graph_about=(
                "Chikungunya case fatality rate: deaths (evolucao IN (2, 3, 4)) "
                "divided by confirmed cases (classi_fin = 13), by year."
            ),
        )
        visuals.append((fig_letal, gid_letal, glab_letal, gabout_letal))

    return tuple(visuals)
