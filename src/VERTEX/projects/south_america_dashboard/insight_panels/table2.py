import os
from typing import Dict, List, Tuple
from collections import OrderedDict

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

import vertex.IsaricDraw as idw


# ----------------------------------------------------------------------
# Botão no menu do dashboard
# ----------------------------------------------------------------------
def define_button():
    """
    Defines the button in the main dashboard menu.
    """
    button_item = "Tables"
    button_label = "Table 2"
    return {"item": button_item, "label": button_label}


# ----------------------------------------------------------------------
# Helpers de conexão / carga do SINAN
# ----------------------------------------------------------------------
def _get_engine_from_env():
    """
    Cria um engine SQLAlchemy usando as variáveis de ambiente do .env:

    PGUSER, PGPASSWORD, PGHOST, PGPORT, PGDATABASE
    """
    user = os.getenv("PGUSER", "postgres")
    password = os.getenv("PGPASSWORD", "benech")  # fallback

    host_env = os.getenv("PGHOST", "host.docker.internal")
    if host_env in ("localhost", "127.0.0.1"):
        host = "host.docker.internal"
    else:
        host = host_env

    port = os.getenv("PGPORT", "5432")
    db = os.getenv("PGDATABASE", "datasus")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"

    safe_url = f"postgresql+psycopg2://{user}:*****@{host}:{port}/{db}"
    print("[TABLE2] Conectando ao Postgres com URL:", safe_url, flush=True)

    return create_engine(url, pool_pre_ping=True)


def _load_sinan_cases(year: int = 2024) -> pd.DataFrame:
    """
    Carrega os casos do SINAN (schema sinan.casos) para um ano específico.
    """
    engine = _get_engine_from_env()

    sql = text(
        """
        SELECT *
        FROM sinan.casos
        WHERE ano = :ano
          AND doenca = 'dengue'
        """
    )

    try:
        with engine.connect() as conn:
            print("[TABLE2] Conexão com Postgres aberta com sucesso.", flush=True)
            df = pd.read_sql(sql, conn, params={"ano": year})
        print("[TABLE2] Dados do SINAN carregados:", df.shape, flush=True)
        print("[TABLE2] Colunas disponíveis:", list(df.columns), flush=True)
        if "classi_fin" in df.columns:
            print(
                "[TABLE2] Distribuição de classi_fin:\n",
                df["classi_fin"].value_counts(),
                flush=True,
            )
        if "reclass_dengue_oms_v2" in df.columns:
            print(
                "[TABLE2] Distribuição de reclass_dengue_oms_v2:\n",
                df["reclass_dengue_oms_v2"].value_counts(),
                flush=True,
            )
        if "evolucao" in df.columns:
            print(
                "[TABLE2] Distribuição de evolucao:\n",
                df["evolucao"].value_counts(),
                flush=True,
            )
        return df

    except Exception as e:
        print("[TABLE2] ERRO ao conectar/buscar no Postgres:", repr(e), flush=True)
        raise


# ----------------------------------------------------------------------
# Helpers de variáveis derivadas
# ----------------------------------------------------------------------
def _get_age_years(df: pd.DataFrame) -> pd.Series:
    """
    Retorna idade em anos numa Series (0–120), usando as colunas disponíveis.

    Prioridade:
    - 'nu_idade_n' se existir (já em anos ou codificada 4000+)
    - senão, 'idade' no formato SINAN (4000 + anos)
    """
    if "nu_idade_n" in df.columns:
        age_raw = pd.to_numeric(df["nu_idade_n"], errors="coerce")
    elif "idade" in df.columns:
        age_raw = pd.to_numeric(df["idade"], errors="coerce")
    else:
        return pd.Series(index=df.index, dtype="float")

    # Se vier no padrão 4000+ anos, tira 4000
    age = np.where(age_raw >= 4000, age_raw - 4000, age_raw).astype(float)
    age = pd.Series(age, index=df.index)
    age = age.where((age >= 0) & (age <= 120))
    return age


# Faixas etárias para Tabela 2 (20–29, 30–39, ..., 80+)
AGE_BINS = [20, 30, 40, 50, 60, 70, 80, np.inf]
AGE_LABELS = [
    "20–29",
    "30–39",
    "40–49",
    "50–59",
    "60–69",
    "70–79",
    "80+",
]


def _age_to_cat(age: pd.Series) -> pd.Series:
    return pd.cut(
        age,
        bins=AGE_BINS,
        labels=AGE_LABELS,
        right=False,
        include_lowest=True,
    )


COMORB_COLS = [
    "diabetes",
    "hipertensa",
    "renal",
    "hepatopat",
    "hematolog",
    "acido_pept",
    "auto_imune",
]


def _count_comorbidities(df: pd.DataFrame) -> pd.Series:
    """
    Conta número de comorbidades por linha, somando as colunas 0/1 de COMORB_COLS.
    """
    series_list = []
    for col in COMORB_COLS:
        if col in df.columns:
            s = pd.to_numeric(df[col], errors="coerce")
            series_list.append((s == 1).astype(int))

    if not series_list:
        return pd.Series(0, index=df.index)

    total = series_list[0]
    for s in series_list[1:]:
        total = total + s
    return total


def _comorb_to_cat(cnt: pd.Series) -> pd.Series:
    """
    Categoriza número de comorbidades em: 0, 1, 2, ≥3
    """
    s = pd.to_numeric(cnt, errors="coerce").fillna(0).astype(int)
    bins = [-0.5, 0.5, 1.5, 2.5, np.inf]
    labels = ["0", "1", "2", "≥3"]
    return pd.cut(s, bins=bins, labels=labels, right=True, include_lowest=True)


# ----------------------------------------------------------------------
# Helpers de formatação
# ----------------------------------------------------------------------
def _fmt_N(n: int) -> str:
    """Formata N com separador de milhar estilo brasileiro."""
    return f"{n:,}".replace(",", ".")


def _format_median_iqr(series: pd.Series) -> str:
    """
    Formata mediana (IQR) como 'm (q1, q3)'.
    """
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return ""
    median = s.median()
    q1 = s.quantile(0.25)
    q3 = s.quantile(0.75)
    return f"{median:.1f} ({q1:.1f}, {q3:.1f})"


def _format_from_counts(count: int, denom: int) -> str:
    if denom <= 0:
        return ""
    pct = 100.0 * count / denom
    return f"{count:,d} ({pct:.1f}%)".replace(",", ".")


def _format_count_pct(series: pd.Series, value) -> str:
    """
    Dado uma Series categórica e um valor/categoria, retorna 'X (Y%)'.
    """
    s = series.dropna()
    denom = len(s)
    if denom == 0:
        return ""
    count = int((s == value).sum())
    return _format_from_counts(count, denom)


# ----------------------------------------------------------------------
# Split por desfecho (Cura vs Óbito)
# ----------------------------------------------------------------------
def _split_outcomes(df: pd.DataFrame):
    """
    Separa o data frame em:
      - df_all  : todos os casos confirmados de dengue (10/11/12) com desfecho 1/2
      - df_cure : evolução = cura (1)
      - df_death: evolução = óbito por dengue (2)
    """
    if "evolucao" not in df.columns:
        raise ValueError("Coluna 'evolucao' não encontrada em sinan.casos")

    s = pd.to_numeric(df["evolucao"], errors="coerce")
    mask_cure = s == 1
    mask_death = s == 2

    df_cure = df[mask_cure]
    df_death = df[mask_death]
    df_all = pd.concat([df_cure, df_death], axis=0)

    print(
        "[TABLE2] Evolução - total: "
        f"{len(df_all)}, cura: {len(df_cure)}, óbito dengue: {len(df_death)}",
        flush=True,
    )

    return df_all, df_cure, df_death


# ----------------------------------------------------------------------
# Construção da Tabela 2 (formato final, como no paper)
# ----------------------------------------------------------------------
def _build_table2(df: pd.DataFrame) -> Tuple[pd.DataFrame, int, int, int]:
    """
    Monta a Tabela 2 já no formato final (strings), com colunas:

      - 'Características'
      - 'Todos'
      - 'Cura'
      - 'Óbito'
    """
    df = df.copy()

    # Coluna de classificação (usamos a reclassificada se existir)
    cls_col = "reclass_dengue_oms_v2" if "reclass_dengue_oms_v2" in df.columns else "classi_fin"

    # Filtro: dengue confirmada (10/11/12)
    mask_conf = df[cls_col].isin([10, 11, 12])
    df_conf = df.loc[mask_conf].copy()

    # Split por desfecho
    df_all, df_cure, df_death = _split_outcomes(df_conf)

    n_all = len(df_all)
    n_cure = len(df_cure)
    n_death = len(df_death)

    groups: "OrderedDict[str, pd.DataFrame]" = OrderedDict(
        [
            ("Todos", df_all),
            ("Cura", df_cure),
            ("Óbito", df_death),
        ]
    )

    # ---------------------- Pré-computos ----------------------
    ages = {name: _get_age_years(gdf) for name, gdf in groups.items()}
    age_cats = {name: _age_to_cat(ages[name]) for name in groups.keys()}

    com_counts = {name: _count_comorbidities(gdf) for name, gdf in groups.items()}
    com_cats = {name: _comorb_to_cat(com_counts[name]) for name in groups.keys()}

    # Escolaridade
    esc_map = {
        1: "Analfabeto",
        2: "Ensino Fundamental Completo e Incompleto",
        3: "Ensino Fundamental Completo e Incompleto",
        4: "Ensino Médio Completo e Incompleto",
        5: "Ensino Médio Completo e Incompleto",
        6: "Ensino Superior Completo e Incompleto",
        7: "Ensino Superior Completo e Incompleto",
    }
    if "cs_escol_n" in df_all.columns:
        df_all["esc_label"] = df_all["cs_escol_n"].map(esc_map)
    else:
        df_all["esc_label"] = pd.NA
    df_cure = df_all.loc[df_all["evolucao"] == 1]
    df_death = df_all.loc[df_all["evolucao"] == 2]
    groups_esc = OrderedDict(
        [
            ("Todos", df_all),
            ("Cura", df_cure),
            ("Óbito", df_death),
        ]
    )

    # Raça / cor
    race_map = {
        1: "Branca",
        2: "Preta",
        3: "Amarela",
        4: "Parda",
        5: "Indígena",
    }
    if "cs_raca" in df_all.columns:
        df_all["raca_label"] = df_all["cs_raca"].map(race_map)
    else:
        df_all["raca_label"] = pd.NA
    df_cure = df_all.loc[df_all["evolucao"] == 1]
    df_death = df_all.loc[df_all["evolucao"] == 2]
    groups_race = OrderedDict(
        [
            ("Todos", df_all),
            ("Cura", df_cure),
            ("Óbito", df_death),
        ]
    )

    # ---------------------- Montagem linha-a-linha ----------------------
    rows: List[Dict[str, str]] = []
    col_names = list(groups.keys())

    def add_row(label: str, values: Dict[str, str] | None = None):
        row = {"Características": label}
        if values is None:
            for g in col_names:
                row[g] = ""
        else:
            for g in col_names:
                row[g] = values.get(g, "")
        rows.append(row)

    # 1) Idade (Anos), mediana (IQR)
    med_values: Dict[str, str] = {}
    for g, age_series in ages.items():
        med_values[g] = _format_median_iqr(age_series)
    add_row("Idade (Anos), mediana (IQR)", med_values)

    # 2) Faixas etárias (20–29 ... 80+), No. (%)
    for faixa in AGE_LABELS:
        valores = {}
        for g in col_names:
            valores[g] = _format_count_pct(age_cats[g], faixa)
        add_row(f"{faixa}, No. (%)", valores)

    # 3) Número de comorbidades, No. (%)
    add_row("No. de comorbidades, No. (%)", None)
    com_order = [("0", "Nenhuma"), ("1", "1"), ("2", "2"), ("≥3", ">= 3")]
    for internal, label in com_order:
        valores = {}
        for g in col_names:
            valores[g] = _format_count_pct(com_cats[g], internal)
        add_row(label, valores)

    # 4) Gênero Feminino, No. (%) [n = ...]
    if "cs_sexo" in df_all.columns:
        sex_all = df_all["cs_sexo"]
        n_valid_sex = int(sex_all.notna().sum())
        pct_valid_sex = 100.0 * n_valid_sex / n_all if n_all > 0 else np.nan

        if n_valid_sex > 0:
            label_genero = (
                f"Gênero Feminino, No. (%) [n = {_fmt_N(n_valid_sex)}, "
                f"({pct_valid_sex:.0f}%)]"
            )
        else:
            label_genero = "Gênero Feminino, No. (%)"

        valores = {}
        for g, gdf in groups.items():
            ser = gdf["cs_sexo"].dropna()
            denom = len(ser)
            count_fem = int((ser == "F").sum())
            valores[g] = _format_from_counts(count_fem, denom)
        add_row(label_genero, valores)

    # 5) Escolaridade, No. (%) [n = ...]
    if "cs_escol_n" in df_all.columns:
        esc_all = df_all["esc_label"]
        n_valid_esc = int(esc_all.notna().sum())
        pct_valid_esc = 100.0 * n_valid_esc / n_all if n_all > 0 else np.nan

        if n_valid_esc > 0:
            label_esc = (
                f"Escolaridade, No. (%) [n = {_fmt_N(n_valid_esc)}, "
                f"({pct_valid_esc:.0f}%)]"
            )
        else:
            label_esc = "Escolaridade, No. (%)"

        add_row(label_esc, None)

        esc_order = [
            "Analfabeto",
            "Ensino Fundamental Completo e Incompleto",
            "Ensino Médio Completo e Incompleto",
            "Ensino Superior Completo e Incompleto",
        ]
        for cat in esc_order:
            valores = {}
            for g, gdf in groups_esc.items():
                ser = gdf["esc_label"].dropna()
                valores[g] = _format_count_pct(ser, cat)
            add_row(cat, valores)

    # 6) Raça, No. (%) [n = ...]
    if "cs_raca" in df_all.columns:
        race_all = df_all["raca_label"]
        n_valid_race = int(race_all.notna().sum())
        pct_valid_race = 100.0 * n_valid_race / n_all if n_all > 0 else np.nan

        if n_valid_race > 0:
            label_race = (
                f"Raça, No. (%) [n = {_fmt_N(n_valid_race)}, "
                f"({pct_valid_race:.0f}%)]"
            )
        else:
            label_race = "Raça, No. (%)"

        add_row(label_race, None)

        race_order = ["Amarela", "Branca", "Indígena", "Parda", "Preta"]
        for cat in race_order:
            valores = {}
            for g, gdf in groups_race.items():
                ser = gdf["raca_label"].dropna()
                valores[g] = _format_count_pct(ser, cat)
            add_row(cat, valores)

    table = pd.DataFrame(rows)
    table = table[["Características"] + col_names]
    print("[TABLE2] Tabela 2 montada no formato final:", table.shape, flush=True)

    return table, n_all, n_cure, n_death


# ----------------------------------------------------------------------
# Função principal chamada pelo VERTEX
# ----------------------------------------------------------------------
def create_visuals(
    df_map,
    df_forms_dict,
    dictionary,
    quality_report,
    filepath,
    suffix,
    save_inputs,
):
    """
    Cria todos os "visuals" do painel Table 2.
    """

    # 1) Carrega dados do SINAN (dengue, ano 2024)
    df_sinan = _load_sinan_cases(year=2024)

    # 2) Monta a tabela já formatada (strings) + Ns
    disp, n_all, n_cure, n_death = _build_table2(df_sinan)

    # 3) Ajusta cabeçalhos das colunas para incluir N (como no paper)
    rename_map = {
        "Todos": f"Todos N = {_fmt_N(n_all)}",
        "Cura": f"Cura N = {_fmt_N(n_cure)}",
        "Óbito": f"Óbito N = {_fmt_N(n_death)}",
    }
    disp = disp.rename(columns=rename_map)

    # 4) Cria visual usando IsaricDraw
    table2 = idw.fig_table(
        disp,
        table_key="table2_sinan",
        suffix=suffix,
        filepath=filepath,
        save_inputs=save_inputs,
        graph_label="Tabela 2",
        graph_about=(
            "Descrição de características e desfechos por desfecho "
            "(casos de dengue, SINAN 2024)"
        ),
    )

    return [table2]
