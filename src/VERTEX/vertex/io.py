"""io.py

Data loading utilities for VERTEX.

Goal: support multiple data sources (REDCap API, local files, or empty placeholders)
so the dashboard can run "REDCap-free" for development/demo.

Key behaviors:
- If api_url and api_key are provided (or data_source == "api"), load from REDCap.
- Else, try to load from local files (project-configured directory or repository root).
- If no local data exists (or data_source == "none"), create empty dataframes with the minimal
  schema required for the app to render.
"""

import json
import os
import shutil
from pathlib import Path
from typing import Dict, Tuple, Optional

import pandas as pd

import vertex.getREDCapData as getRC
from vertex.layout.insight_panels import get_visuals
from vertex.logging.logger import setup_logger

logger = setup_logger(__name__)

# -----------------------------
# Defaults
# -----------------------------

config_defaults = {
    "project_name": None,
    "data_access_groups": None,
    "map_layout_center_latitude": 6,
    "map_layout_center_longitude": -75,
    "map_layout_zoom": 1.7,
    "save_public_outputs": False,
    "save_base_files_to_public_path": False,
    "public_path": "PUBLIC/",
    "save_filtered_public_outputs": False,
    "insight_panels_path": "insight_panels/",
    "insight_panels": [],

    # New / optional
    # data_source: "auto" | "api" | "files" | "none"
    "data_source": "auto",
    # Path (dir) where df_map.csv + dictionary live. Relative paths are resolved from project_path.
    "vertex_dataframes_path": None,
    # Optional explicit filenames (resolved from vertex_dataframes_path or project_path)
    "df_map_filename": "df_map.csv",
    "dictionary_filename": None,  # will search common names
}


# -----------------------------
# Config
# -----------------------------

def get_config(project_path, config_defaults):
    """Read project config and apply defaults.

    For "REDCap-free" usage we do NOT require api_key/api_url; if missing they are set to None.
    """

    config_file = os.path.join(project_path, "config_file.json")
    try:
        with open(config_file, "r", encoding="utf-8") as json_data:
            config_dict = json.load(json_data)
    except Exception as e:
        logger.error(f"Could not read {config_file}: {e}")
        raise SystemExit

    # Ensure expected keys exist
    config_dict.setdefault("api_key", None)
    config_dict.setdefault("api_url", None)
    config_dict.setdefault("data_source", config_defaults.get("data_source", "auto"))

    # Insight panels discovery
    if "insight_panels_path" not in config_dict:
        config_dict["insight_panels_path"] = config_defaults["insight_panels_path"]

    insight_panels_path = os.path.join(project_path, config_dict["insight_panels_path"])
    insight_panels = []
    for _, _, filenames in os.walk(insight_panels_path):
        insight_panels = [file.split(".py")[0] for file in filenames if file.endswith(".py") and not file.startswith("_")]
        break
    config_defaults = dict(config_defaults)
    config_defaults["insight_panels"] = insight_panels

    # Apply defaults for missing keys
    missing_defaults = {k: v for k, v in config_defaults.items() if k not in config_dict}
    config_dict = {**config_dict, **missing_defaults}

    # Validate insight panel list
    if any([x not in insight_panels for x in config_dict["insight_panels"]]):
        missing_insight_panels = [x for x in config_dict["insight_panels"] if x not in insight_panels]
        logger.warning(
            f"The following insight panels are ignored and will not appear in the dashboard: {missing_insight_panels}"
        )
        config_dict["insight_panels"] = [x for x in config_dict["insight_panels"] if x in insight_panels]

    if any([x not in config_dict["insight_panels"] for x in insight_panels]):
        missing_insight_panels = [x for x in insight_panels if x not in config_dict["insight_panels"]]
        logger.warning(
            "The following insight panels are available but not included in the dashboard "
            f"(add these to config_file.json to include them): {missing_insight_panels}"
        )

    return config_dict


# -----------------------------
# Data loading entrypoint
# -----------------------------

def load_vertex_data(project_path, config_dict):
    """Load VERTEX data from the configured data source."""

    data_source = (config_dict.get("data_source") or "auto").lower()
    api_url = config_dict.get("api_url")
    api_key = config_dict.get("api_key")

    if data_source == "api" or (data_source == "auto" and api_url and api_key):
        return load_vertex_from_api(api_url, api_key, config_dict)

    if data_source in ("files", "auto"):
        try:
            return load_vertex_from_files(project_path, config_dict)
        except Exception as e:
            logger.warning(f"Failed to load from files ({e}). Falling back to empty data.")
            return empty_vertex_payload()

    # data_source == "none" or unknown
    return empty_vertex_payload()


def load_vertex_from_api(api_url, api_key, config_dict):
    """Load data from the REDCap API."""

    logger.info("Retrieving data from REDCap API")
    user_assigned_to_dag = getRC.user_assigned_to_dag(api_url, api_key)
    get_data_kwargs = {
        "data_access_groups": config_dict.get("data_access_groups"),
        "user_assigned_to_dag": user_assigned_to_dag,
    }
    df_map, df_forms_dict, dictionary, quality_report = getRC.get_redcap_data(api_url, api_key, **get_data_kwargs)
    return df_map, df_forms_dict, dictionary, quality_report


# -----------------------------
# Local files
# -----------------------------

def _resolve_dir(project_path: str, vertex_dataframes_path: Optional[str]) -> Optional[Path]:
    if not vertex_dataframes_path:
        return None
    p = Path(vertex_dataframes_path)
    if not p.is_absolute():
        p = Path(project_path) / p
    return p


def _repo_root() -> Path:
    # /.../VERTEX/vertex/io.py -> repo root is parent of "vertex" package
    return Path(__file__).resolve().parents[1]


def _pick_dictionary_file(base: Path, preferred: Optional[str]) -> Optional[Path]:
    candidates = []
    if preferred:
        candidates.append(base / preferred)
    # common names
    candidates += [
        base / "vertex_dictionary.csv",
        base / "dic.csv",
        base / "dictionary.csv",
        base / "data_dictionary.csv",
    ]
    for p in candidates:
        if p.exists():
            return p
    return None


def _pick_df_map_file(base: Path, filename: str) -> Optional[Path]:
    p = base / filename
    if p.exists():
        return p
    # common fallback
    fallback = base / "df_map.csv"
    return fallback if fallback.exists() else None


def empty_vertex_payload() -> Tuple[pd.DataFrame, Dict[str, pd.DataFrame], pd.DataFrame, Dict]:
    """Return empty-but-valid payload so the app can render."""

    # minimal schema required by the dashboard core
    df_map = pd.DataFrame(
        {
            "subjid": pd.Series(dtype="str"),
            "demog_sex": pd.Series(dtype="str"),
            "demog_age": pd.Series(dtype="float"),
            "pres_date": pd.Series(dtype="datetime64[ns]"),
            "country_iso": pd.Series(dtype="str"),
            "outco_binary_outcome": pd.Series(dtype="str"),
        }
    )

    dictionary = pd.DataFrame(
        columns=["field_name", "form_name", "field_type", "field_label", "parent", "branching_logic"]
    )

    return df_map, {}, dictionary, {}


def load_vertex_from_files(project_path, config_dict):
    """Load dataframes from local CSV files.

    Resolution order:
    1) config.vertex_dataframes_path (dir)
    2) <project_path>/vertex_dataframes (dir)
    3) repository root (where df_map.csv / dic.csv sit in this repo)

    Expected files:
    - df_map.csv
    - dictionary: vertex_dictionary.csv OR dic.csv
    - any additional *.csv with a 'subjid' column will be treated as a form dataframe
    """

    project_dir = Path(project_path)

    data_dir = _resolve_dir(project_path, config_dict.get("vertex_dataframes_path"))
    if data_dir is None:
        candidate = project_dir / "vertex_dataframes"
        data_dir = candidate if candidate.exists() else None

    if data_dir is None:
        data_dir = _repo_root()

    if not data_dir.exists():
        logger.warning(f"Data directory not found: {data_dir}. Using empty payload.")
        return empty_vertex_payload()

    df_map_path = _pick_df_map_file(data_dir, config_dict.get("df_map_filename") or "df_map.csv")
    dict_path = _pick_dictionary_file(data_dir, config_dict.get("dictionary_filename"))

    if df_map_path is None:
        logger.warning(f"df_map file not found under {data_dir}. Using empty payload.")
        return empty_vertex_payload()

    if dict_path is None:
        logger.warning(f"Dictionary file not found under {data_dir}. Will load df_map without typed schema.")
        dictionary = pd.DataFrame(
            columns=["field_name", "form_name", "field_type", "field_label", "parent", "branching_logic"]
        )
        df_map = pd.read_csv(df_map_path, keep_default_na=False)
        # try to coerce pres_date if present
        if "pres_date" in df_map.columns:
            df_map["pres_date"] = pd.to_datetime(df_map["pres_date"], errors="coerce")
        df_forms_dict = {}
        return df_map, df_forms_dict, dictionary, {}

    # Load dictionary
    dictionary = pd.read_csv(dict_path, dtype={"field_label": "str"}, keep_default_na=False)

    # Ensure canonical columns exist
    for col in ["field_name", "field_type", "field_label"]:
        if col not in dictionary.columns:
            dictionary[col] = ""

    str_ind = dictionary["field_type"].isin(["freetext", "categorical"]) if "field_type" in dictionary.columns else pd.Series([])
    str_columns = dictionary.loc[str_ind, "field_name"].tolist() if len(dictionary) else []
    non_str_columns = dictionary.loc[(~str_ind), "field_name"].tolist() if len(dictionary) else []

    dtype_dict = {**{x: "str" for x in str_columns}}

    pandas_default_na_values = [
        "",
        " ",
        "#N/A",
        "#N/A N/A",
        "#NA",
        "-1.#IND",
        "-1.#QNAN",
        "-NaN",
        "-nan",
        "1.#IND",
        "1.#QNAN",
        "<NA>",
        "N/A",
        "NA",
        "NULL",
        "NaN",
        "None",
        "n/a",
        "nan",
        "null",
    ]
    na_values = {**{x: pandas_default_na_values for x in non_str_columns}, **{x: "" for x in str_columns}}

    df_map = pd.read_csv(df_map_path, dtype=dtype_dict, keep_default_na=False, na_values=na_values)

    # Fix dates (if dictionary indicates date type)
    if "field_type" in dictionary.columns:
        date_variables = dictionary.loc[(dictionary["field_type"] == "date"), "field_name"].tolist()
        if len(date_variables) > 0:
            for col in date_variables:
                if col in df_map.columns:
                    df_map[col] = pd.to_datetime(df_map[col], errors="coerce")

    # Load additional form csv files
    df_forms_dict: Dict[str, pd.DataFrame] = {}
    for file in sorted(data_dir.glob("*.csv")):
        if file.name in {df_map_path.name, dict_path.name}:
            continue
        try:
            df_form = pd.read_csv(file, dtype=dtype_dict, keep_default_na=False, na_values=na_values)
        except Exception:
            continue
        if "subjid" in df_form.columns:
            key = file.stem
            df_forms_dict[key] = df_form

    return df_map, df_forms_dict, dictionary, {}


# -----------------------------
# Projects helpers
# -----------------------------

def get_projects():
    project_path = Path("projects/")
    logger.info(f"Looking for projects in: {project_path.resolve()}")
    projects = [p for p in project_path.iterdir() if p.is_dir()]
    names = [get_project_name(p) for p in projects]
    logger.info(f"Found projects: {[p.name for p in projects]}")
    return [str(p) + "/" for p in projects], names


def get_project_name(project_path):
    config_file = Path(project_path) / "config_file.json"
    if config_file.exists():
        try:
            with open(config_file, "r", encoding="utf-8") as f:
                config = json.load(f)
                logger.debug(f"Loaded config for {project_path.name}: {config}")
                project_name = config.get("project_name", project_path.name)
        except Exception as e:
            logger.warning(f"Could not read config for {project_path.name}: {e}")
            project_name = project_path.name
    else:
        project_name = project_path.name
    return project_name


# -----------------------------
# Public outputs
# -----------------------------

def save_public_outputs(
    buttons, insight_panels, df_map, df_countries, df_forms_dict, dictionary, quality_report, project_path, config_dict
):
    """Save public outputs to the PUBLIC folder."""

    public_path = os.path.join(project_path, config_dict["public_path"])
    if os.path.exists(public_path):
        logger.warning(f'Folder "{public_path}" already exists, removing this')
        shutil.rmtree(public_path)

    logger.info(f'Saving files for public dashboard to "{public_path}"')
    os.makedirs(os.path.dirname(os.path.join(public_path, "")), exist_ok=True)

    for ip in config_dict["insight_panels"]:
        os.makedirs(os.path.dirname(os.path.join(public_path, ip, "")), exist_ok=True)

    buttons = get_visuals(
        buttons,
        insight_panels,
        df_map=df_map,
        df_forms_dict=df_forms_dict,
        dictionary=dictionary,
        quality_report=quality_report,
        filepath=os.path.join(public_path, ""),
    )

    os.makedirs(os.path.dirname(public_path), exist_ok=True)
    if config_dict.get("save_base_files_to_public_path"):
        shutil.copy("descriptive_dashboard_public.py", public_path)
        shutil.copy("IsaricDraw.py", public_path)
        shutil.copy("requirements.txt", public_path)
        assets_path = os.path.join(public_path, "assets/")
        os.makedirs(os.path.dirname(assets_path), exist_ok=True)
        shutil.copytree("assets", assets_path, dirs_exist_ok=True)
