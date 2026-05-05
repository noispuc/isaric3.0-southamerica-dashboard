"""
Insight panel - Febre Amarela
Age Mortality Risk
"""

import os
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import plotly.graph_objects as go


def define_button():
    """
    Define o botão no menu principal do dashboard.
    """
    button_item = "Rates"
    button_label = "Age Mortality Risk"
    return {"item": button_item, "label": button_label}


def _get_engine_from_env():
    """Cria uma engine PostgreSQL usando variáveis de ambiente (PGHOST, etc.)."""
    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD")
    host = os.getenv("PGHOST")
    port = os.getenv("PGPORT")
    dbname = os.getenv("PGDATABASE")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(url)


def _load_mortality_curve_by_age(engine) -> pd.DataFrame:
    """
    Calcula risco de mortalidade por idade, consolidando todos os anos.

    Adaptação para febre amarela:
    - A base já contém apenas casos de febre amarela.
    - Não há classi_fin na base disponibilizada.
    - Não há evolucao no formato SINAN original.
    - Evento de óbito = obito_flag = 1.
    - A idade é tratada de forma robusta:
        * se estiver no padrão SINAN codificado, converte para anos;
        * se já estiver em anos, mantém o valor original.
    """
    sql = """
        WITH base AS (
            SELECT
                CASE
                    -- Caso a idade esteja no padrão SINAN codificado
                    WHEN idade BETWEEN 4000 AND 4999 THEN (idade - 4000)
                    WHEN idade BETWEEN 3000 AND 3999 THEN (idade - 3000) / 12.0
                    WHEN idade BETWEEN 2000 AND 2999 THEN (idade - 2000) / 365.0
                    WHEN idade BETWEEN 1000 AND 1999 THEN (idade - 1000) / (24.0 * 365.0)

                    -- Caso a idade já esteja em anos
                    WHEN idade BETWEEN 0 AND 120 THEN idade::numeric

                    ELSE NULL
                END AS idade_anos,
                obito_flag
            FROM febre_amarela.casos
        )
        SELECT
            FLOOR(idade_anos)::int AS idade,
            COUNT(*) AS casos,
            COUNT(*) FILTER (
                WHERE obito_flag = 1
            ) AS obitos
        FROM base
        WHERE idade_anos IS NOT NULL
        GROUP BY FLOOR(idade_anos)
        HAVING COUNT(*) >= 30
        ORDER BY idade;
    """

    df = pd.read_sql(sql, engine)

    if df.empty:
        return df

    df = df[(df["idade"] >= 0) & (df["idade"] <= 100)].copy()

    df["risk"] = df["obitos"] / df["casos"]

    z = 1.96
    df["se"] = np.sqrt(df["risk"] * (1 - df["risk"]) / df["casos"])
    df["ci_low"] = (df["risk"] - z * df["se"]).clip(lower=0.0)
    df["ci_high"] = (df["risk"] + z * df["se"]).clip(upper=1.0)

    window = 5
    df = df.sort_values("idade")
    df["risk_smooth"] = (
        df["risk"].rolling(window=window, center=True, min_periods=1).mean()
    )
    df["ci_low_smooth"] = (
        df["ci_low"].rolling(window=window, center=True, min_periods=1).mean()
    )
    df["ci_high_smooth"] = (
        df["ci_high"].rolling(window=window, center=True, min_periods=1).mean()
    )

    return df


def _build_mortality_figure(df: pd.DataFrame) -> go.Figure:
    """
    Constrói o gráfico de risco de mortalidade por idade com faixa de confiança.
    """
    fig = go.Figure()

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

    fig.add_trace(
        go.Scatter(
            x=df["idade"],
            y=df["risk_smooth"],
            mode="lines",
            line=dict(color="black", width=2),
            name="Mortality risk",
        )
    )

    fig.update_layout(
        title="Age-specific mortality risk",
        xaxis_title="Age (years)",
        yaxis_title="Mortality risk",
        template="simple_white",
        margin=dict(l=60, r=20, t=60, b=60),
    )

    ymax = float(df["risk_smooth"].max()) if not df.empty else 0
    fig.update_yaxes(range=[0, min(1.0, ymax * 1.1 if ymax > 0 else 0.1)])

    return fig


def create_visuals(
    df_map, df_forms_dict, dictionary, quality_report, filepath, suffix, save_inputs
):
    """
    Função padrão do Vertex para criar os visuais do painel.
    Retorna um tuple de (fig, graph_id, graph_label, graph_about).
    """
    visuals = []

    engine = _get_engine_from_env()
    df_curve = _load_mortality_curve_by_age(engine)

    if not df_curve.empty:
        fig = _build_mortality_figure(df_curve)

        graph_id = "fa_age_mortality_risk_curve"
        graph_label = "Age-specific mortality risk"
        graph_about = (
            "Yellow fever mortality risk curve by age, calculated as the proportion "
            "of deaths among cases available in the febre_amarela dataset. "
            "The event is defined using obito_flag = 1. "
            "The curve consolidates all available years and is smoothed using a moving average. "
            "The gray area represents an approximate 95% confidence interval."
        )

        visuals.append((fig, graph_id, graph_label, graph_about))

    return tuple(visuals)