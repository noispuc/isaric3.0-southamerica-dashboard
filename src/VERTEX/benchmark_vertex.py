#!/usr/bin/env python3
"""
benchmark_vertex.py — Benchmark de insight panels COM o VERTEX rodando.

Sobe o VERTEX internamente (sem browser), carrega todos os projetos,
e executa create_visuals() de cada panel sobre os dados reais carregados,
medindo o tempo de cada execução.

Este script precisa do banco de dados configurado (variáveis PG*)
ou dados locais, pois usa o pipeline real do VERTEX para carregar dados.

Uso:
    python benchmark_vertex.py                     # Todos os projetos
    python benchmark_vertex.py --project dengue    # Filtro por projeto
    python benchmark_vertex.py --csv results.csv   # Salvar CSV
    python benchmark_vertex.py --repeat 3          # 3 repetições

Rode de dentro da pasta VERTEX:
    cd src/VERTEX && python benchmark_vertex.py
"""

import argparse
import csv
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Ensure VERTEX root is in path
VERTEX_ROOT = Path(__file__).resolve().parent
os.chdir(VERTEX_ROOT)
if str(VERTEX_ROOT) not in sys.path:
    sys.path.insert(0, str(VERTEX_ROOT))

import pandas as pd
from vertex.io import config_defaults, get_config, load_vertex_data
from vertex.layout.insight_panels import get_insight_panels


# ─────────────────────────────────────────────
# Data structures
# ─────────────────────────────────────────────

class BenchmarkResult:
    def __init__(self, project: str, panel: str, status: str, time_s: float,
                 n_visuals: int, error: Optional[str] = None):
        self.project = project
        self.panel = panel
        self.status = status
        self.time_s = time_s
        self.n_visuals = n_visuals
        self.error = error


# ─────────────────────────────────────────────
# Benchmark runner
# ─────────────────────────────────────────────

def load_project(project_path: str) -> dict:
    """
    Load a project using the real VERTEX pipeline (same as descriptive_dashboard.py).
    Returns the project data dict.
    """
    config_dict = get_config(project_path, config_defaults)
    insight_panels_path = os.path.join(project_path, config_dict["insight_panels_path"])
    insight_panels, buttons = get_insight_panels(config_dict, insight_panels_path)

    df_map, df_forms_dict, dictionary, quality_report = load_vertex_data(
        project_path, config_dict
    )

    # Ensure minimum schema (same logic as descriptive_dashboard.py)
    if not isinstance(df_map, pd.DataFrame):
        df_map = pd.DataFrame()
    if not isinstance(df_forms_dict, dict):
        df_forms_dict = {}

    required_cols = [
        "subjid", "demog_sex", "demog_age", "pres_date",
        "country_iso", "outco_binary_outcome",
    ]
    for col in required_cols:
        if col not in df_map.columns:
            df_map[col] = pd.NA

    df_map["demog_age"] = pd.to_numeric(df_map["demog_age"], errors="coerce")
    df_map["pres_date"] = pd.to_datetime(df_map["pres_date"], errors="coerce")

    return {
        "df_map": df_map,
        "df_forms_dict": df_forms_dict,
        "dictionary": dictionary,
        "quality_report": quality_report,
        "insight_panels": insight_panels,
        "buttons": buttons,
        "config_dict": config_dict,
    }


def benchmark_panel(
    module, panel_name: str, project_data: dict, project_path: str
) -> Tuple[float, int, Optional[str]]:
    """Run create_visuals for a single panel with real data."""
    error = None
    n_visuals = 0

    t0 = time.perf_counter()
    try:
        result = module.create_visuals(
            df_map=project_data["df_map"].copy(),
            df_forms_dict={k: v.copy() for k, v in project_data["df_forms_dict"].items()},
            dictionary=(
                project_data["dictionary"].copy()
                if hasattr(project_data["dictionary"], "copy")
                else project_data["dictionary"]
            ),
            quality_report=project_data["quality_report"],
            filepath=project_path,
            suffix=panel_name,
            save_inputs=False,
        )
        if result is not None:
            n_visuals = len(result) if isinstance(result, (list, tuple)) else 1
    except Exception as e:
        error = str(e)
    t1 = time.perf_counter()

    return t1 - t0, n_visuals, error


# ─────────────────────────────────────────────
# Output formatting
# ─────────────────────────────────────────────

def print_results(results: List[BenchmarkResult], repeat: int):
    """Pretty-print benchmark results grouped by project."""
    projects: Dict[str, List[BenchmarkResult]] = {}
    for r in results:
        projects.setdefault(r.project, []).append(r)

    line = "═" * 68
    print(f"\n╔{line}╗")
    title = "VERTEX In-App Benchmark Report"
    if repeat > 1:
        title += f"  (avg of {repeat} runs)"
    print(f"║{title:^{len(line)}}║")
    print(f"╚{line}╝\n")

    grand_total_time = 0.0
    grand_total_panels = 0
    grand_total_visuals = 0

    for proj_name, proj_results in projects.items():
        print(f"  Project: {proj_name}")
        col_w = [30, 10, 11, 14]
        print(f"  ┌{'─' * col_w[0]}┬{'─' * col_w[1]}┬{'─' * col_w[2]}┬{'─' * col_w[3]}┐")
        print(f"  │{'Panel':<{col_w[0]}}│{'Status':^{col_w[1]}}│{'Time(s)':^{col_w[2]}}│{'# Visuals':^{col_w[3]}}│")
        print(f"  ├{'─' * col_w[0]}┼{'─' * col_w[1]}┼{'─' * col_w[2]}┼{'─' * col_w[3]}┤")

        proj_total_time = 0.0
        proj_total_visuals = 0
        for r in proj_results:
            status = "✓" if r.status == "ok" else "✗"
            time_str = f"{r.time_s:.4f}"
            vis_str = str(r.n_visuals) if r.status == "ok" else (r.error[:12] if r.error else "—")
            print(f"  │{r.panel:<{col_w[0]}}│{status:^{col_w[1]}}│{time_str:^{col_w[2]}}│{vis_str:^{col_w[3]}}│")
            proj_total_time += r.time_s
            proj_total_visuals += r.n_visuals

        print(f"  ├{'─' * col_w[0]}┼{'─' * col_w[1]}┼{'─' * col_w[2]}┼{'─' * col_w[3]}┤")
        print(f"  │{'TOTAL':<{col_w[0]}}│{'':^{col_w[1]}}│{proj_total_time:^{col_w[2]}.4f}│{proj_total_visuals:^{col_w[3]}}│")
        print(f"  └{'─' * col_w[0]}┴{'─' * col_w[1]}┴{'─' * col_w[2]}┴{'─' * col_w[3]}┘")
        print()

        grand_total_time += proj_total_time
        grand_total_panels += len(proj_results)
        grand_total_visuals += proj_total_visuals

    print(f"  GRAND TOTAL: {grand_total_time:.4f}s across {len(projects)} project(s), "
          f"{grand_total_panels} panel(s), {grand_total_visuals} visual(s)\n")

    errors = [r for r in results if r.error]
    if errors:
        print("  ⚠ Errors:")
        for r in errors:
            print(f"    [{r.project}/{r.panel}] {r.error}")
        print()


def save_csv(results: List[BenchmarkResult], csv_path: str):
    """Save results to CSV."""
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["project", "panel", "status", "time_s", "n_visuals", "error"])
        for r in results:
            writer.writerow([r.project, r.panel, r.status, f"{r.time_s:.6f}", r.n_visuals, r.error or ""])
    print(f"  Results saved to: {csv_path}")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Benchmark VERTEX insight panels using the real VERTEX data pipeline."
    )
    parser.add_argument("--project", "-p", type=str, default=None,
                        help="Filter projects by name (substring match).")
    parser.add_argument("--csv", type=str, default=None,
                        help="Save results to a CSV file.")
    parser.add_argument("--repeat", "-r", type=int, default=1,
                        help="Number of repetitions per panel (reports average).")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Print detailed output.")
    args = parser.parse_args()

    print(f"\n  VERTEX root: {VERTEX_ROOT}")
    print(f"  Mode: REAL (using VERTEX data pipeline)")
    if args.repeat > 1:
        print(f"  Repeats: {args.repeat}")

    # Discover projects
    projects_dir = VERTEX_ROOT / "projects"
    project_paths = sorted(
        p for p in projects_dir.iterdir()
        if p.is_dir() and not p.name.startswith(".")
    )
    if args.project:
        project_paths = [p for p in project_paths if args.project.lower() in p.name.lower()]

    if not project_paths:
        print("  No projects found.")
        return

    print(f"  Found {len(project_paths)} project(s): {[p.name for p in project_paths]}\n")

    all_results: List[BenchmarkResult] = []

    for project_path in project_paths:
        path_str = str(project_path) + "/"

        # Load project data
        print(f"  Loading project: {project_path.name}...")
        t_load_start = time.perf_counter()
        try:
            project_data = load_project(path_str)
        except Exception as e:
            print(f"  ✗ Failed to load {project_path.name}: {e}")
            continue
        t_load = time.perf_counter() - t_load_start
        print(f"  ✓ Loaded in {t_load:.2f}s")

        panels = project_data["insight_panels"]
        if not panels:
            if args.verbose:
                print(f"    No insight panels for {project_path.name}, skipping.")
            continue

        for panel_name, module in panels.items():
            times = []
            last_n_visuals = 0
            last_error = None

            for i in range(args.repeat):
                elapsed, n_vis, err = benchmark_panel(
                    module, panel_name, project_data, path_str
                )
                times.append(elapsed)
                last_n_visuals = n_vis
                last_error = err

                if args.verbose:
                    status_icon = "✓" if err is None else "✗"
                    run_label = f" run {i+1}/{args.repeat}" if args.repeat > 1 else ""
                    print(f"    {status_icon} {panel_name}{run_label}: "
                          f"{elapsed:.4f}s, {n_vis} visual(s)"
                          + (f" — {err}" if err else ""))

            avg_time = sum(times) / len(times)
            all_results.append(BenchmarkResult(
                project=project_path.name,
                panel=panel_name,
                status="ok" if last_error is None else "error",
                time_s=avg_time,
                n_visuals=last_n_visuals,
                error=last_error,
            ))

    print_results(all_results, args.repeat)

    if args.csv:
        save_csv(all_results, args.csv)


if __name__ == "__main__":
    main()
