'''
FINAL
'''

import os
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import plotly.graph_objects as go
import vertex.IsaricDraw as idw


def define_button():
    """
    Define o botão no menu principal do dashboard.
    Ajuste 'item' e 'label' se quiser outro texto na UI.
    """
    button_item = "Rates"
    button_label = "Age Mortality Risk"
    return {"item": button_item, "label": button_label}


def _get_engine_from_env():
    """Cria uma engine PostgreSQL usando variáveis de ambiente (PGHOST, etc.)."""
    user = os.getenv("PGUSER", "postgres")
    password = os.getenv("PGPASSWORD", "")
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    dbname = os.getenv("PGDATABASE", "datasus")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(url)


def _load_mortality_curve_by_age(engine) -> pd.DataFrame:
    """
    Calcula risco de mortalidade por idade (anos), consolidando todos os anos.

    - Usa apenas casos confirmados de dengue (classi_fin IN 10, 11, 12)
    - Evento = óbito por dengue (evolucao = 2)
    - idades convertidas para anos (idade_anos) a partir do campo 'idade'
    """

    sql = """
        WITH base AS (
            SELECT
                CASE
                    WHEN idade BETWEEN 4000 AND 4999 THEN (idade - 4000)
                    WHEN idade BETWEEN 3000 AND 3999 THEN (idade - 3000) / 12.0
                    WHEN idade BETWEEN 2000 AND 2999 THEN (idade - 2000) / 365.0
                    WHEN idade BETWEEN 1000 AND 1999 THEN (idade - 1000) / (24.0 * 365.0)
                    ELSE NULL
                END AS idade_anos,
                classi_fin,
                evolucao
            FROM sinan.casos
        )
        SELECT
            FLOOR(idade_anos)::int AS idade,
            COUNT(*) FILTER (
                WHERE classi_fin IN 12)
            ) AS casos_confirmados,
            COUNT(*) FILTER (
                WHERE classi_fin IN (12) AND evolucao = 2
            ) AS obitos_dengue
        FROM base
        WHERE idade_anos IS NOT NULL
        GROUP BY FLOOR(idade_anos)
        HAVING COUNT(*) FILTER (WHERE classi_fin IN (12)) >= 30  -- evita idades com pouca amostra
        ORDER BY idade;
    """

    df = pd.read_sql(sql, engine)

    if df.empty:
        return df

    # mantém idades em um range “clínico” razoável (ajuste se quiser)
    df = df[(df["idade"] >= 0) & (df["idade"] <= 100)].copy()

    # risco bruto
    df["risk"] = df["obitos_dengue"] / df["casos_confirmados"]

    # regressao
    # y = obitos
    # x = [idade (valor)]

    # intervalo de confiança binomial aproximado (normal)
    z = 1.96
    df["se"] = np.sqrt(df["risk"] * (1 - df["risk"]) / df["casos_confirmados"])
    df["ci_low"] = (df["risk"] - z * df["se"]).clip(lower=0.0)
    df["ci_high"] = (df["risk"] + z * df["se"]).clip(upper=1.0)

    # suavização por média móvel (risco "ajustado"/suavizado)
    window = 5  # ~5 anos de largura
    df = df.sort_values("idade")
    df["risk_smooth"] = df["risk"].rolling(window=window, center=True, min_periods=1).mean()
    df["ci_low_smooth"] = df["ci_low"].rolling(window=window, center=True, min_periods=1).mean()
    df["ci_high_smooth"] = df["ci_high"].rolling(window=window, center=True, min_periods=1).mean()

    return df


def _build_mortality_figure(df: pd.DataFrame) -> go.Figure:
    """
    Constroi o gráfico estilo “risco ajustado por idade” com faixa de confiança.
    """
    fig = go.Figure()

    # Faixa de confiança (cinza)
    fig.add_trace(
        go.Scatter(
            x=df["idade"],
            y=df["ci_low_smooth"],
            mode="lines",
            line=dict(color="rgba(0,0,0,0)"),
            showlegend=False,
            hoverinfo="skip",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df["idade"],
            y=df["ci_high_smooth"],
            mode="lines",
            fill="tonexty",
            line=dict(color="rgba(0,0,0,0)"),
            fillcolor="rgba(0,0,0,0.12)",
            showlegend=False,
            hoverinfo="skip",
        )
    )

    # Linha principal (preta)
    fig.add_trace(
        go.Scatter(
            x=df["idade"],
            y=df["risk_smooth"],
            mode="lines",
            line=dict(color="black", width=2),
            name="Risco ajustado",
        )
    )

    fig.update_layout(
        title="Risco de mortalidade ajustado por idade",
        xaxis_title="Idade (anos)",
        yaxis_title="Risco de mortalidade ajustado",
        template="simple_white",
        margin=dict(l=60, r=20, t=60, b=60),
    )

    # y entre 0 e um pouco acima do máximo observado
    ymax = float(df["risk_smooth"].max())
    fig.update_yaxes(range=[0, min(1.0, ymax * 1.1)])

    return fig


def create_visuals(df_map, df_forms_dict, dictionary, quality_report, filepath, suffix, save_inputs):
    """
    Função padrão do Vertex para criar os visuais do painel.
    Retorna um tuple de (fig, graph_id, graph_label, graph_about).
    """
    visuals = []
    engine = _get_engine_from_env()

    df_curve = _load_mortality_curve_by_age(engine)
    if not df_curve.empty:
        fig = _build_mortality_figure(df_curve)

        graph_id = "age_mortality_risk_curve"
        graph_label = "Risco de mortalidade ajustado por idade"
        graph_about = (
            "Curva de risco de mortalidade por dengue em função da idade, "
            "calculada como proporção de óbitos entre casos confirmados (classi_fin 10, 11, 12), "
            "consolidando todos os anos disponíveis e suavizada por média móvel. "
            "A faixa cinza representa um intervalo de confiança aproximado (95%)."
        )

        visuals.append((fig, graph_id, graph_label, graph_about))

    return tuple(visuals)
