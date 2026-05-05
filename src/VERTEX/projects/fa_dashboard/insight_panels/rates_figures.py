import os
import pandas as pd
from sqlalchemy import create_engine
import vertex.IsaricDraw as idw


def _get_engine_from_env():
    """Cria uma engine PostgreSQL usando variáveis de ambiente (PGHOST, etc.)."""
    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD")
    host = os.getenv("PGHOST")
    port = os.getenv("PGPORT")
    dbname = os.getenv("PGDATABASE")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(url)


def _get_anos_disponiveis(engine) -> list[int]:
    """
    Lê os anos disponíveis na view de incidência de febre amarela.
    """
    sql = """
        SELECT DISTINCT ano
        FROM febre_amarela.vw_fa_incidencia_100k
        ORDER BY ano;
    """
    df = pd.read_sql(sql, engine)

    if df.empty:
        return []

    return df["ano"].astype(int).tolist()


def _load_taxa_incidencia_ano(engine, ano: int) -> pd.DataFrame:
    """
    Lê a incidência de casos de febre amarela por 100 mil habitantes.

    Fonte: febre_amarela.vw_fa_incidencia_100k.
    """
    sql = """
        SELECT
            ano,
            incidencia_100k
        FROM febre_amarela.vw_fa_incidencia_100k
        WHERE ano = %(ano)s;
    """

    df = pd.read_sql(sql, engine, params={"ano": ano})

    if df.empty:
        return df

    return pd.DataFrame(
        {
            "ano": df["ano"].astype(str),
            "Cases rate": df["incidencia_100k"].astype(float),
        }
    )


def _load_taxa_obito_ano(engine, ano: int) -> pd.DataFrame:
    """
    Lê a taxa de mortalidade por febre amarela por 100 mil habitantes.

    Fonte: febre_amarela.vw_fa_mortalidade_100k.
    """
    sql = """
        SELECT
            ano,
            taxa_mortalidade_100k
        FROM febre_amarela.vw_fa_mortalidade_100k
        WHERE ano = %(ano)s;
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


def _load_taxa_letalidade_ano(engine, ano: int) -> pd.DataFrame:
    """
    Lê a taxa de letalidade por febre amarela (%).

    Fonte: febre_amarela.vw_fa_letalidade_porcent.
    """
    sql = """
        SELECT
            ano,
            taxa_letalidade_pct
        FROM febre_amarela.vw_fa_letalidade_porcent
        WHERE ano = %(ano)s;
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
    """Defines the button in the main dashboard menu."""
    button_item = "Rates"
    button_label = "Main Rates"

    return {"item": button_item, "label": button_label}


def create_visuals(
    df_map, df_forms_dict, dictionary, quality_report, filepath, suffix, save_inputs
):
    """
    Create all visuals in the insight panel.

    Visuals mantidos para febre amarela:
    - taxa de casos por 100 mil habitantes;
    - taxa de mortalidade por 100 mil habitantes;
    - taxa de letalidade (%).

    Visual removido:
    - taxa de hospitalização, pois a base de febre amarela não possui variável
      de hospitalização.
    """
    visuals = []

    engine = _get_engine_from_env()
    anos = _get_anos_disponiveis(engine)

    if not anos:
        return tuple(visuals)

    # 1. Taxa de casos por 100 mil habitantes
    dfs_casos = []

    for ano in anos:
        df_ano = _load_taxa_incidencia_ano(engine, ano=ano)

        if not df_ano.empty:
            dfs_casos.append(df_ano)

    if dfs_casos:
        df_casos_all = pd.concat(dfs_casos, ignore_index=True)

        fig_casos, gid_casos, glab_casos, gabout_casos = idw.fig_bar_chart(
            data=df_casos_all,
            title="Yellow Fever Cases Rate per 100k pop.",
            xlabel="Year",
            ylabel="Cases per 100k inhabitants",
            index_column="ano",
            barmode="group",
            xaxis_tickformat="",
            suffix=suffix,
            filepath=filepath,
            save_inputs=save_inputs,
            graph_label="Yellow fever cases rate",
            graph_about=(
                "Yellow fever cases rate per 100k inhabitants, "
                "for each available year, using annual population as denominator. "
                "The febre_amarela dataset is assumed to already contain only "
                "yellow fever cases."
            ),
        )

        visuals.append((fig_casos, gid_casos, glab_casos, gabout_casos))

    # 2. Taxa de mortalidade por 100 mil habitantes
    dfs_obito = []

    for ano in anos:
        df_ano = _load_taxa_obito_ano(engine, ano=ano)

        if not df_ano.empty:
            dfs_obito.append(df_ano)

    if dfs_obito:
        df_obito_all = pd.concat(dfs_obito, ignore_index=True)

        fig_obito, gid_obito, glab_obito, gabout_obito = idw.fig_bar_chart(
            data=df_obito_all,
            title="Yellow Fever Mortality Rate per 100k pop.",
            xlabel="Year",
            ylabel="Deaths per 100k inhabitants",
            index_column="ano",
            barmode="group",
            xaxis_tickformat="",
            suffix=suffix,
            filepath=filepath,
            save_inputs=save_inputs,
            graph_label="Yellow fever mortality rate",
            graph_about=(
                "Yellow fever mortality rate per 100k inhabitants, "
                "for each available year, using annual population as denominator. "
                "Deaths are based on the febre_amarela mortality view."
            ),
        )

        visuals.append((fig_obito, gid_obito, glab_obito, gabout_obito))

    # 3. Taxa de letalidade
    dfs_letal = []

    for ano in anos:
        df_ano = _load_taxa_letalidade_ano(engine, ano=ano)

        if not df_ano.empty:
            dfs_letal.append(df_ano)

    if dfs_letal:
        df_letal_all = pd.concat(dfs_letal, ignore_index=True)

        fig_letal, gid_letal, glab_letal, gabout_letal = idw.fig_bar_chart(
            data=df_letal_all,
            title="Yellow Fever Case Fatality Rate (%)",
            xlabel="Year",
            ylabel="%",
            index_column="ano",
            barmode="group",
            xaxis_tickformat="",
            suffix=suffix,
            filepath=filepath,
            save_inputs=save_inputs,
            graph_label="Yellow fever case fatality rate",
            graph_about=(
                "Yellow fever case fatality rate, calculated as deaths divided "
                "by cases, by year. Deaths are based on the febre_amarela "
                "fatality view."
            ),
        )

        visuals.append((fig_letal, gid_letal, glab_letal, gabout_letal))

    return tuple(visuals)