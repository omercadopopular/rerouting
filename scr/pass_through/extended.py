"""Long-horizon extension of the locked historical pass-through design.

The original benchmark remains fixed at ``[-6, 6]``.  This module estimates
separate ``[-6, 24]`` specifications on archive-native raw trade.  Because
the present project stage has not reconstructed post-April-2019 policy
actions, the validated April 2019 tariff level is held fixed thereafter and
the choice is disclosed in every manifest.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from .config import PipelineConfig
from .estimators import _prepare_dynamic, _prepare_event_study, _run_dynamic_one, _run_event_study_one
from .io_utils import read_table, sha256_file, write_metadata_json, write_parquet

VERSION = "historical_long_horizon_v1"
MODES = ("independent_paper_full_policy", "independent_legal_full_policy")
SPECS = ("event", "dynamic")
OUTCOMES = ("val", "q1", "p", "pduty")
START_PERIOD = "2017-01"
END_PERIOD = "2020-10"
TERMINAL_TARIFF_PERIOD = "2019-04"


def _sql(path: Path) -> str:
    return str(path).replace("'", "''")


def _relative(config: PipelineConfig, path: Path) -> str:
    return path.resolve().relative_to(config.repo_root.resolve()).as_posix()


def root(config: PipelineConfig) -> Path:
    path = config.verification_dir / VERSION
    path.mkdir(parents=True, exist_ok=True)
    return path


def panel_path(config: PipelineConfig, mode: str) -> Path:
    return root(config) / "panels" / f"{mode}.parquet"


def coefficient_path(config: PipelineConfig, mode: str, spec: str, outcome: str) -> Path:
    return root(config) / "coefficients" / mode / spec / f"{outcome}.parquet"


def _atomic_query(con: duckdb.DuckDBPyConnection, query: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{VERSION}.tmp")
    temporary.unlink(missing_ok=True)
    try:
        con.execute(f"COPY ({query}) TO '{_sql(temporary)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        rows = int(con.execute(f"SELECT count(*) FROM read_parquet('{_sql(temporary)}')").fetchone()[0])
        if rows == 0:
            raise RuntimeError("Extended panel is empty")
        temporary.replace(destination)
    finally:
        temporary.unlink(missing_ok=True)


def build_panels(config: PipelineConfig, *, overwrite: bool = False) -> dict[str, Any]:
    canonical_glob = config.processed_trade_dir / "intermediate" / "monthly_imports" / "year=*" / "month=*" / "part.parquet"
    legacy_glob = config.repo_root / "data" / "analysis" / "passthru_data" / "extension_v4_cif" / "flow=imports" / "year=*" / "month=*" / "part.parquet"
    trade_glob = canonical_glob if list(canonical_glob.parent.parent.glob("year=*/month=*/part.parquet")) else legacy_glob
    outcomes = config.processed_trade_dir / "final" / "historical_replication_outcomes.parquet"
    tariffs = config.processed_tariff_dir / "final" / "historical_tariffs.parquet"
    clock = config.processed_tariff_dir / "final" / "historical_event_clock.parquet"
    for required in (outcomes, tariffs, clock):
        if not required.exists():
            raise FileNotFoundError(required)
    if not all(panel_path(config, mode).exists() for mode in MODES) and not list(trade_glob.parent.parent.glob("year=*/month=*/part.parquet")):
        raise FileNotFoundError(trade_glob)

    con = duckdb.connect()
    records: dict[str, Any] = {}
    try:
        for mode in MODES:
            destination = panel_path(config, mode)
            event_field = "paper_event_month" if mode.startswith("independent_paper") else "legal_event_month"
            if overwrite or not destination.exists():
                query = f"""
                WITH trade AS (
                  SELECT CAST(partner_code AS INTEGER) AS cty_code,
                         partner_name AS cty_name, hs10, hs8, hs6, hs4, hs2,
                         CAST(year AS INTEGER) AS year, CAST(month AS INTEGER) AS month,
                         make_date(CAST(year AS INTEGER),CAST(month AS INTEGER),1) AS mdate,
                         gen_cif_mo/1000000.0 AS m_val,
                         gen_qy1_mo/1000000.0 AS m_q1,
                         gen_cif_mo/nullif(gen_qy1_mo,0) AS m_p,
                         (gen_cif_mo+cal_dut_mo)/nullif(gen_qy1_mo,0) AS m_pduty
                  FROM read_parquet('{_sql(trade_glob)}', hive_partitioning=false)
                  WHERE make_date(CAST(year AS INTEGER),CAST(month AS INTEGER),1)
                    BETWEEN DATE '{START_PERIOD}-01' AND DATE '{END_PERIOD}-01'
                ), naics AS (
                  SELECT hs10, min(naics_str) AS naics_str
                  FROM read_parquet('{_sql(outcomes)}') GROUP BY hs10
                ), monthly AS (
                  SELECT CAST(cty_code AS INTEGER) AS cty_code,hs10,year,month,
                         paper_dynamic_total_tariff AS tariff
                  FROM read_parquet('{_sql(tariffs)}')
                  WHERE make_date(CAST(year AS INTEGER),CAST(month AS INTEGER),1) <= DATE '{TERMINAL_TARIFF_PERIOD}-01'
                ), terminal AS (
                  SELECT cty_code,hs10,arg_max(tariff,year*12+month) AS terminal_tariff
                  FROM monthly GROUP BY cty_code,hs10
                ), clocks AS (
                  SELECT CAST(cty_code AS INTEGER) AS cty_code,hs10,
                         max(historical_status) AS historical_status,
                         min({event_field}) AS event_month
                  FROM read_parquet('{_sql(clock)}')
                  GROUP BY cty_code,hs10
                ), joined AS (
                  SELECT t.*,n.naics_str,c.historical_status,c.event_month,
                         coalesce(m.tariff,z.terminal_tariff) AS tariff
                  FROM trade t
                  JOIN naics n USING(hs10)
                  JOIN clocks c USING(cty_code,hs10)
                  LEFT JOIN monthly m USING(cty_code,hs10,year,month)
                  LEFT JOIN terminal z USING(cty_code,hs10)
                )
                SELECT *,
                       CAST(event_month AS TIMESTAMP) AS m_effective_mdate2,
                       tariff AS m_stattariff2,
                       historical_status AS m_status2,
                       historical_status AS m_ess,
                       '{mode}' AS policy_mode,
                       '{TERMINAL_TARIFF_PERIOD}' AS tariff_freeze_period
                FROM joined WHERE tariff IS NOT NULL
                """
                _atomic_query(con, query, destination)
            period = con.execute(f"SELECT min(year*100+month),max(year*100+month),count(*) FROM read_parquet('{_sql(destination)}')").fetchone()
            records[mode] = {
                "path": _relative(config, destination),
                "sha256": sha256_file(destination),
                "start_period": str(period[0]),
                "end_period": str(period[1]),
                "rows": int(period[2]),
            }
    finally:
        con.close()
    manifest = {
        "version": VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "complete",
        "event_horizon": [-6, 24],
        "dynamic_horizon": [-6, 24],
        "terminal_tariff_period": TERMINAL_TARIFF_PERIOD,
        "post_terminal_policy_rule": "hold validated terminal tariff level fixed; do not infer later actions",
        "panels": records,
    }
    write_metadata_json(root(config) / "panel_manifest.json", manifest)
    return manifest


def run_fits(
    config: PipelineConfig,
    *,
    modes: tuple[str, ...] = MODES,
    specs: tuple[str, ...] = SPECS,
    outcomes: tuple[str, ...] = OUTCOMES,
    overwrite: bool = False,
) -> dict[str, Any]:
    completed: list[dict[str, Any]] = []
    for mode in modes:
        source = panel_path(config, mode)
        mode_needs_fit = overwrite or any(
            not coefficient_path(config, mode, spec, outcome).exists()
            for spec in specs for outcome in outcomes
        )
        frame = read_table(source) if mode_needs_fit else None
        for spec in specs:
            spec_needs_fit = overwrite or any(
                not coefficient_path(config, mode, spec, outcome).exists()
                for outcome in outcomes
            )
            prepared = None
            if spec_needs_fit:
                if frame is None:
                    raise RuntimeError("Internal error: missing source frame for incomplete fit grid")
                prepared = (
                    _prepare_event_study("imports", frame, post_horizon=24)
                    if spec == "event"
                    else _prepare_dynamic("imports", frame, package_logs=False, lag_horizon=24)
                )
            for outcome in outcomes:
                destination = coefficient_path(config, mode, spec, outcome)
                if destination.exists() and not overwrite:
                    result = read_table(destination)
                else:
                    if prepared is None:
                        raise RuntimeError(f"Missing prepared design for {mode}|{spec}|{outcome}")
                    fit = (
                        _run_event_study_one(config, "imports", outcome, prepared, mode, _relative(config, source), post_horizon=24)
                        if spec == "event"
                        else _run_dynamic_one(config, "imports", outcome, prepared, mode, _relative(config, source), lag_horizon=24)
                    )
                    result = fit.frame
                    write_parquet(result, destination, overwrite=True)
                horizon = "event_time" if spec == "event" else "horizon"
                if len(result) != 31 or result[horizon].nunique() != 31:
                    raise RuntimeError(f"Incomplete long-horizon fit: {mode}|{spec}|{outcome}")
                record = {
                    "fit_id": f"{mode}|{spec}|{outcome}|h24",
                    "path": _relative(config, destination),
                    "source_sha256": sha256_file(source),
                    "rows": 31,
                    "nobs": int(result["nobs"].iloc[0]),
                }
                write_metadata_json(destination.with_suffix(".json"), record)
                completed.append(record)
            if prepared is not None:
                del prepared
        if frame is not None:
            del frame
    manifest = {
        "version": VERSION,
        "status": "complete" if len(completed) == 16 and tuple(modes) == MODES and tuple(specs) == SPECS and tuple(outcomes) == OUTCOMES else "partial",
        "expected_fits": 16,
        "completed_fits": len(completed),
        "fits": completed,
    }
    write_metadata_json(root(config) / "regression_manifest.json", manifest)
    return manifest


def plot(config: PipelineConfig) -> dict[str, str]:
    """Plot the locked original line together with the two +24 extensions."""
    package_root = config.processed_trade_dir / "package_benchmark"
    figure_root = config.repo_root / "figs" / "replication"
    figure_root.mkdir(parents=True, exist_ok=True)
    colors = {
        "independent_paper_full_policy": ("#2ca02c", "Replication", "-"),
        "independent_legal_full_policy": ("#ff7f0e", "Alternative timing (legal clock)", "--"),
    }
    labels = {"val": "Import value", "q1": "Quantity", "p": "Pre-duty unit value", "pduty": "Duty-inclusive unit value"}
    outputs: dict[str, str] = {}
    for spec in SPECS:
        horizon = "event_time" if spec == "event" else "horizon"
        package = read_table(package_root / f"package_full_{spec}_coefficients.parquet")
        fig, axes = plt.subplots(2, 2, figsize=(13, 8.5), sharex=True)
        for axis, outcome in zip(axes.flat, OUTCOMES):
            original = package.loc[(package["spec"] == spec) & (package["outcome"] == outcome)].sort_values(horizon)
            axis.fill_between(original[horizon].to_numpy(float), original["conf_low"].to_numpy(float), original["conf_high"].to_numpy(float), color="#202020", alpha=.12)
            axis.plot(original[horizon], original["estimate"], color="#202020", marker="o", markersize=3, label="Original regression")
            for mode in MODES:
                line = read_table(coefficient_path(config, mode, spec, outcome)).sort_values(horizon)
                color, label, style = colors[mode]
                axis.fill_between(line[horizon].to_numpy(float), line["conf_low"].to_numpy(float), line["conf_high"].to_numpy(float), color=color, alpha=.12)
                axis.plot(line[horizon], line["estimate"], color=color, linestyle=style, linewidth=1.7, label=label)
            axis.axhline(0, color=".25", linewidth=.7)
            axis.axvline(0, color=".6", linewidth=.7, linestyle="--")
            axis.set_title(labels[outcome])
            axis.grid(alpha=.2)
        handles, legend = axes[0, 0].get_legend_handles_labels()
        fig.legend(handles, legend, loc="upper center", bbox_to_anchor=(.5, .985), ncol=3, frameon=False, fontsize=8)
        fig.suptitle(f"Historical replication and +24-month extension: {spec}", y=.925)
        fig.tight_layout(rect=(0, 0, 1, .87))
        destination = figure_root / f"historical_replication_{spec}_h24"
        fig.savefig(destination.with_suffix(".png"), dpi=190)
        fig.savefig(destination.with_suffix(".pdf"))
        plt.close(fig)
        outputs[spec] = _relative(config, destination.with_suffix(".pdf"))
    write_metadata_json(root(config) / "figure_manifest.json", {"version": VERSION, "figures": outputs})
    return outputs


def plot_dynamic_h12(config: PipelineConfig) -> dict[str, str]:
    """Extend Appendix Figure 2 to h=12 using validated h=24 fits.

    The authors-package benchmark remains available only through h=6. The
    independent replication and legal-clock sensitivity continue through
    h=12 and are not extrapolated beyond estimated coefficients.
    """
    package_root = config.processed_trade_dir / "package_benchmark"
    figure_root = config.repo_root / "figs" / "replication"
    figure_root.mkdir(parents=True, exist_ok=True)
    colors = {
        "independent_paper_full_policy": (
            "#2ca02c",
            "Replication",
            "-",
        ),
        "independent_legal_full_policy": (
            "#ff7f0e",
            "Alternative timing (legal clock)",
            "--",
        ),
    }
    labels = {
        "val": "Import value",
        "q1": "Quantity",
        "p": "Pre-duty unit value",
        "pduty": "Duty-inclusive unit value",
    }
    package = read_table(
        package_root / "package_full_dynamic_coefficients.parquet"
    )
    fig, axes = plt.subplots(
        2,
        2,
        figsize=(13, 8.5),
        sharex=True,
    )
    for axis, outcome in zip(axes.flat, OUTCOMES):
        original = package.loc[
            (package["spec"] == "dynamic")
            & (package["outcome"] == outcome)
        ].sort_values("horizon")
        axis.fill_between(
            original["horizon"].to_numpy(float),
            original["conf_low"].to_numpy(float),
            original["conf_high"].to_numpy(float),
            color="#202020",
            alpha=0.12,
        )
        axis.plot(
            original["horizon"],
            original["estimate"],
            color="#202020",
            marker="o",
            markersize=3,
            label="Original regression (through +6)",
        )
        for mode in MODES:
            line = read_table(
                coefficient_path(
                    config,
                    mode,
                    "dynamic",
                    outcome,
                )
            )
            line = line.loc[line["horizon"] <= 12].sort_values(
                "horizon"
            )
            color, label, style = colors[mode]
            axis.fill_between(
                line["horizon"].to_numpy(float),
                line["conf_low"].to_numpy(float),
                line["conf_high"].to_numpy(float),
                color=color,
                alpha=0.12,
            )
            axis.plot(
                line["horizon"],
                line["estimate"],
                color=color,
                linestyle=style,
                linewidth=1.7,
                label=label,
            )
        axis.axhline(0, color=".25", linewidth=.7)
        axis.axvline(0, color=".6", linewidth=.7, linestyle="--")
        axis.set_xlim(-6.5, 12.5)
        axis.set_title(labels[outcome])
        axis.grid(alpha=.2)
    handles, legend = axes[0, 0].get_legend_handles_labels()
    fig.legend(
        handles,
        legend,
        loc="upper center",
        bbox_to_anchor=(.5, .985),
        ncol=3,
        frameon=False,
        fontsize=8,
    )
    fig.suptitle(
        "Historical dynamic response: replication through +12 months",
        y=.925,
    )
    fig.tight_layout(rect=(0, 0, 1, .87))
    destination = (
        figure_root / "historical_replication_dynamic_h12"
    )
    fig.savefig(destination.with_suffix(".png"), dpi=190)
    fig.savefig(destination.with_suffix(".pdf"))
    plt.close(fig)
    result = {
        "dynamic_h12": _relative(
            config,
            destination.with_suffix(".pdf"),
        ),
        "original_benchmark_last_horizon": 6,
        "replication_last_horizon": 12,
    }
    write_metadata_json(
        root(config) / "figure_h12_manifest.json",
        {"version": VERSION, **result},
    )
    return result


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-panels", action="store_true")
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--plot", action="store_true")
    parser.add_argument("--plot-h12", action="store_true")
    parser.add_argument("--mode", choices=(*MODES, "all"), default="all")
    parser.add_argument("--spec", choices=(*SPECS, "all"), default="all")
    parser.add_argument("--outcome", choices=(*OUTCOMES, "all"), default="all")
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()
    config = PipelineConfig.default()
    if args.build_panels:
        print(json.dumps(build_panels(config, overwrite=args.overwrite), indent=2))
    if args.run:
        modes = MODES if args.mode == "all" else (args.mode,)
        specs = SPECS if args.spec == "all" else (args.spec,)
        outcomes = OUTCOMES if args.outcome == "all" else (args.outcome,)
        print(json.dumps(run_fits(config, modes=modes, specs=specs, outcomes=outcomes, overwrite=args.overwrite), indent=2))
    if args.plot:
        print(json.dumps(plot(config), indent=2))
    if args.plot_h12:
        print(json.dumps(plot_dynamic_h12(config), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
