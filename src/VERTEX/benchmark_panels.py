#!/usr/bin/env python3
"""
benchmark_panels.py — Benchmark de tempo dos insight panels do VERTEX (sem Dash).

Importa e executa create_visuals() de cada insight panel de cada projeto,
medindo o tempo de execução. Funciona SEM banco de dados: intercepta chamadas
SQL com mock (pd.read_sql retorna DataFrame vazio).

Uso:
    python benchmark_panels.py                    # Todos os projetos
    python benchmark_panels.py --project dengue   # Só projetos que contêm "dengue"
    python benchmark_panels.py --csv results.csv  # Salva resultados em CSV
    python benchmark_panels.py --repeat 3         # Repete 3x e mostra média

Rode de dentro da pasta VERTEX:
    cd src/VERTEX && python benchmark_panels.py
"""

import argparse
import csv
import importlib
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock, patch

import pandas as pd


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def discover_projects(base: Path, filter_name: Optional[str] = None) -> List[Path]:
    """Discover project directories under base/projects/."""
    projects_dir = base / "projects"
    if not projects_dir.exists():
        print(f"[ERROR] Projects directory not found: {projects_dir}")
        sys.exit(1)

    projects = sorted(
        p for p in projects_dir.iterdir()
        if p.is_dir() and not p.name.startswith(".")
    )

    if filter_name:
        projects = [p for p in projects if filter_name.lower() in p.name.lower()]

    return projects


def load_config(project_path: Path) -> dict:
    """Load config_file.json from a project."""
    config_file = project_path / "config_file.json"
    if not config_file.exists():
        return {}
    with open(config_file, "r", encoding="utf-8") as f:
        return json.load(f)


def discover_insight_panels(project_path: Path, config: dict) -> List[str]:
    """Discover insight panel names for a project."""
    ip_path = project_path / config.get("insight_panels_path", "insight_panels/")
    if not ip_path.exists():
        return []
    panels = [
        f.stem for f in sorted(ip_path.iterdir())
        if f.suffix == ".py" and not f.name.startswith("_")
    ]
    # Respect order from config if specified
    config_panels = config.get("insight_panels", [])
    if config_panels:
        ordered = [p for p in config_panels if p in panels]
        extra = [p for p in panels if p not in config_panels]
        return ordered + extra
    return panels


def import_panel(panel_name: str, panel_path: Path):
    """Dynamically import an insight panel module."""
    spec = importlib.util.spec_from_file_location(panel_name, panel_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[panel_name] = module
    spec.loader.exec_module(module)
    return module


# ─────────────────────────────────────────────
# Mock infrastructure
# ─────────────────────────────────────────────

def _mock_read_sql(*args, **kwargs) -> pd.DataFrame:
    """Return an empty DataFrame for any SQL query."""
    return pd.DataFrame()


def _mock_create_engine(*args, **kwargs) -> MagicMock:
    """Return a mock engine that supports .connect() context manager."""
    engine = MagicMock()
    # Support `with engine.connect() as conn:` pattern
    conn_mock = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn_mock)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return engine


# ─────────────────────────────────────────────
# Benchmark runner
# ─────────────────────────────────────────────

def benchmark_panel(
    module,
    panel_name: str,
    suffix: str,
    filepath: str,
) -> Tuple[float, int, Optional[str]]:
    """
    Run create_visuals() for a single panel and return (time_seconds, n_visuals, error).

    All SQL calls are mocked — pd.read_sql always returns empty DataFrame.
    """
    # Build dummy arguments matching create_visuals signature
    dummy_args = dict(
        df_map=pd.DataFrame(),
        df_forms_dict={},
        dictionary=pd.DataFrame(),
        quality_report={},
        filepath=filepath,
        suffix=suffix,
        save_inputs=False,
    )

    error = None
    n_visuals = 0

    # Patch pd.read_sql globally and create_engine wherever it's used
    with (
        patch("pandas.read_sql", side_effect=_mock_read_sql),
        patch("pandas.read_sql_query", side_effect=_mock_read_sql),
        patch("pandas.read_sql_table", side_effect=_mock_read_sql),
        patch("sqlalchemy.create_engine", side_effect=_mock_create_engine),
    ):
        # Also patch at the module level if the panel imported create_engine directly
        patches = []
        for attr_name in dir(module):
            obj = getattr(module, attr_name, None)
            if obj is not None:
                # Patch _get_engine_from_env-like functions
                if callable(obj) and "engine" in attr_name.lower():
                    p = patch.object(module, attr_name, side_effect=lambda *a, **k: _mock_create_engine())
                    patches.append(p)
                    p.start()

        # Patch create_engine at the module level if imported there
        if hasattr(module, "create_engine"):
            p = patch.object(module, "create_engine", side_effect=_mock_create_engine)
            patches.append(p)
            p.start()

        # Patch pd at the module level too (since some panels do `pd.read_sql`)
        if hasattr(module, "pd"):
            original_read_sql = module.pd.read_sql
            module.pd.read_sql = _mock_read_sql

        t0 = time.perf_counter()
        try:
            result = module.create_visuals(**dummy_args)
            if result is not None:
                if isinstance(result, (list, tuple)):
                    n_visuals = len(result)
                else:
                    n_visuals = 1
        except Exception as e:
            error = str(e)
        t1 = time.perf_counter()

        # Restore
        if hasattr(module, "pd"):
            module.pd.read_sql = original_read_sql

        for p in patches:
            p.stop()

    elapsed = t1 - t0
    return elapsed, n_visuals, error


# ─────────────────────────────────────────────
# Results formatting
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


def print_results(results: List[BenchmarkResult], repeat: int):
    """Pretty-print benchmark results grouped by project."""

    # Group by project
    projects: Dict[str, List[BenchmarkResult]] = {}
    for r in results:
        projects.setdefault(r.project, []).append(r)

    # Header
    line = "═" * 68
    print(f"\n╔{line}╗")
    title = "VERTEX Insight Panel Benchmark Report"
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
        header = f"  ┌{'─' * col_w[0]}┬{'─' * col_w[1]}┬{'─' * col_w[2]}┬{'─' * col_w[3]}┐"
        print(header)
        print(f"  │{'Panel':<{col_w[0]}}│{'Status':^{col_w[1]}}│{'Time(s)':^{col_w[2]}}│{'# Visuals':^{col_w[3]}}│")
        sep = f"  ├{'─' * col_w[0]}┼{'─' * col_w[1]}┼{'─' * col_w[2]}┼{'─' * col_w[3]}┤"
        print(sep)

        proj_total_time = 0.0
        proj_total_visuals = 0
        for r in proj_results:
            status = "✓" if r.status == "ok" else "✗"
            time_str = f"{r.time_s:.4f}"
            vis_str = str(r.n_visuals) if r.status == "ok" else r.error[:12] if r.error else "—"
            print(f"  │{r.panel:<{col_w[0]}}│{status:^{col_w[1]}}│{time_str:^{col_w[2]}}│{vis_str:^{col_w[3]}}│")
            proj_total_time += r.time_s
            proj_total_visuals += r.n_visuals

        print(sep)
        print(f"  │{'TOTAL':<{col_w[0]}}│{'':^{col_w[1]}}│{proj_total_time:^{col_w[2]}.4f}│{proj_total_visuals:^{col_w[3]}}│")
        footer = f"  └{'─' * col_w[0]}┴{'─' * col_w[1]}┴{'─' * col_w[2]}┴{'─' * col_w[3]}┘"
        print(footer)
        print()

        grand_total_time += proj_total_time
        grand_total_panels += len(proj_results)
        grand_total_visuals += proj_total_visuals

    print(f"  GRAND TOTAL: {grand_total_time:.4f}s across {len(projects)} project(s), "
          f"{grand_total_panels} panel(s), {grand_total_visuals} visual(s)\n")

    # Print errors if any
    errors = [r for r in results if r.error]
    if errors:
        print("  ⚠ Errors:")
        for r in errors:
            print(f"    [{r.project}/{r.panel}] {r.error}")
        print()


def save_csv(results: List[BenchmarkResult], csv_path: str):
    """Save benchmark results to a CSV file."""
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
        description="Benchmark VERTEX insight panels without running the Dash app."
    )
    parser.add_argument(
        "--project", "-p",
        type=str,
        default=None,
        help="Filter projects by name (substring match). E.g. --project dengue",
    )
    parser.add_argument(
        "--csv",
        type=str,
        default=None,
        help="Save results to a CSV file.",
    )
    parser.add_argument(
        "--repeat", "-r",
        type=int,
        default=1,
        help="Number of repetitions per panel (reports average). Default: 1",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Print detailed output during execution.",
    )
    args = parser.parse_args()

    # Resolve VERTEX root
    vertex_root = Path(__file__).resolve().parent
    os.chdir(vertex_root)

    # Ensure the vertex package is importable
    if str(vertex_root) not in sys.path:
        sys.path.insert(0, str(vertex_root))

    print(f"\n  VERTEX root: {vertex_root}")
    print(f"  Mode: MOCK (no database, SQL returns empty DataFrames)")
    if args.repeat > 1:
        print(f"  Repeats: {args.repeat}")

    projects = discover_projects(vertex_root, args.project)
    if not projects:
        print("  No projects found.")
        return

    print(f"  Found {len(projects)} project(s): {[p.name for p in projects]}\n")

    all_results: List[BenchmarkResult] = []

    for project_path in projects:
        config = load_config(project_path)
        panel_names = discover_insight_panels(project_path, config)

        if not panel_names:
            if args.verbose:
                print(f"  [{project_path.name}] No insight panels found, skipping.")
            continue

        ip_dir = project_path / config.get("insight_panels_path", "insight_panels/")

        if args.verbose:
            print(f"  [{project_path.name}] Found {len(panel_names)} panel(s): {panel_names}")

        for panel_name in panel_names:
            panel_file = ip_dir / f"{panel_name}.py"
            if not panel_file.exists():
                all_results.append(BenchmarkResult(
                    project=project_path.name,
                    panel=panel_name,
                    status="error",
                    time_s=0.0,
                    n_visuals=0,
                    error="File not found",
                ))
                continue

            # Import panel
            try:
                # Use unique module name to avoid collisions between projects
                module_key = f"_bench_{project_path.name}_{panel_name}"
                module = import_panel(module_key, panel_file)
            except Exception as e:
                all_results.append(BenchmarkResult(
                    project=project_path.name,
                    panel=panel_name,
                    status="error",
                    time_s=0.0,
                    n_visuals=0,
                    error=f"Import error: {e}",
                ))
                continue

            # Benchmark with repetitions
            times = []
            last_n_visuals = 0
            last_error = None

            for i in range(args.repeat):
                elapsed, n_vis, err = benchmark_panel(
                    module,
                    panel_name,
                    suffix=panel_name,
                    filepath=str(project_path) + "/",
                )
                times.append(elapsed)
                last_n_visuals = n_vis
                last_error = err

                if args.verbose:
                    status_icon = "✓" if err is None else "✗"
                    run_label = f"  run {i+1}/{args.repeat}" if args.repeat > 1 else ""
                    print(f"    {status_icon} {project_path.name}/{panel_name}{run_label}: "
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

    # Output
    print_results(all_results, args.repeat)

    if args.csv:
        save_csv(all_results, args.csv)


if __name__ == "__main__":
    main()
