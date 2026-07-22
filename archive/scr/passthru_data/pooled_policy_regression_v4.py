"""Final same-sample policy-substitution audit for the historical window.

The independent modes are sourced from the corrected v3 partner-specific
panel.  Paper and legal event clocks may differ, but both dynamic modes use
the same bilateral day-weighted tariff path.  Package policy is used only as
the validation anchor.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from .config import PipelineConfig
from .io_utils import read_table, sha256_file, write_metadata_json, write_parquet
from .pooled_policy_replication_v4 import analysis_root as policy_root
from .trade_regressions import (
    _prepare_dynamic,
    _prepare_event_study,
    _run_dynamic_one,
    _run_event_study_one,
)

VERSION = "pooled_policy_regressions_v4"
MODES = (
    "package_full_policy_anchor",
    "independent_paper_full_policy",
    "independent_legal_full_policy",
)
SPECS = ("event", "dynamic")
OUTCOMES = ("val", "q1", "p", "pduty")
THRESHOLDS = {
    "correlation": 0.95,
    "rmse": 1.25,
    "max_abs_difference": 2.50,
    "post_treatment_sign_agreement": 0.50,
}
HISTORICAL_ACTION_CUTOFF = "2018-09-30"
FINAL_CHART_SERIES = (
    ("original", "#202020", "Original regression", "-", "o"),
    ("independent_paper_full_policy", "#2ca02c", "Replication", "-", "o"),
    (
        "independent_legal_full_policy",
        "#ff7f0e",
        "Alternative timing (independent policy, legal clock)",
        "--",
        "s",
    ),
)
OUTCOME_LABELS = {
    "val": "Import value",
    "q1": "Quantity",
    "p": "Pre-duty unit value",
    "pduty": "Duty-inclusive unit value",
}


def root(config: PipelineConfig) -> Path:
    path = config.verification_dir / "raw_replication_imports" / VERSION
    path.mkdir(parents=True, exist_ok=True)
    return path


def _relative(config: PipelineConfig, path: Path) -> str:
    return path.resolve().relative_to(config.repo_root.resolve()).as_posix()


def _sql(path: Path) -> str:
    return str(path).replace("'", "''")


def panel_paths(config: PipelineConfig) -> dict[str, Path]:
    return {mode: root(config) / "panels" / f"{mode}.parquet" for mode in MODES}


def _mode_fields(mode: str) -> tuple[str, str, str]:
    if mode == "independent_paper_full_policy":
        return "paper_event_month", "paper_dynamic_total_tariff", "historical_status"
    if mode == "independent_legal_full_policy":
        return "legal_event_month", "legal_dynamic_total_tariff", "historical_status"
    raise ValueError(f"not an independent mode: {mode}")


def _validate_parquet(path: Path, *, expected_rows: int | None = None) -> dict[str, Any]:
    pf = pq.ParquetFile(path)
    compression = sorted({
        pf.metadata.row_group(r).column(c).compression
        for r in range(pf.metadata.num_row_groups)
        for c in range(pf.metadata.row_group(r).num_columns)
    })
    rows = int(pf.metadata.num_rows)
    if compression != ["ZSTD"]:
        raise RuntimeError(f"expected ZSTD Parquet: {path} -> {compression}")
    if expected_rows is not None and rows != expected_rows:
        raise RuntimeError(f"row-count mismatch for {path}: {rows} != {expected_rows}")
    return {"rows": rows, "compression": compression, "sha256": sha256_file(path)}


def _atomic_copy(con: duckdb.DuckDBPyConnection, query: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{VERSION}.tmp")
    temporary.unlink(missing_ok=True)
    try:
        con.execute(f"COPY ({query}) TO '{_sql(temporary)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        _validate_parquet(temporary)
        temporary.replace(destination)
    finally:
        temporary.unlink(missing_ok=True)


def build_panels(config: PipelineConfig, *, overwrite: bool = False) -> dict[str, Any]:
    raw = config.verification_dir / "trade_regressions" / "package_benchmark_v5" / "common_sample_v5_cif" / "raw_outcomes_package_policy_cif.parquet"
    package = policy_root(config) / "package_full_policy_anchor.parquet"
    independent = config.analysis_dir / "policy" / "pooled_policy_replication_v3" / "pooled_policy_replication_v3_panel.parquet"
    if not raw.exists() or not package.exists() or not independent.exists():
        raise FileNotFoundError("raw common sample, package anchor, and corrected v3 policy panel are required")

    paths = panel_paths(config)
    con = duckdb.connect()
    clock = policy_root(config) / "historical_event_clock_v4.parquet"
    result: dict[str, Any] = {
        "version": VERSION,
        "raw_source": _relative(config, raw),
        "package_policy_source": _relative(config, package),
        "independent_policy_source": _relative(config, independent),
        "historical_event_clock": _relative(config, clock),
        "historical_action_cutoff": HISTORICAL_ACTION_CUTOFF,
        "panels": {},
        "paper_and_legal_dynamic_paths_identical_by_design": True,
    }
    try:
        expected = int(con.execute(f"SELECT count(*) FROM read_parquet('{_sql(raw)}')").fetchone()[0])
        if overwrite or not clock.exists():
            clock_query = f"""
            WITH source AS (
              SELECT cty_code,hs10,year,month,source_effective_date,
                     bilateral_dayweighted_additional_rate,
                     year*12+month-1 AS month_index
              FROM read_parquet('{_sql(independent)}')
              WHERE source_effective_date <= DATE '{HISTORICAL_ACTION_CUTOFF}'
                AND make_date(year::INTEGER,month::INTEGER,1) <= DATE '2018-09-01'
            ), partner_first AS (
              SELECT cty_code,hs10,min(month_index) AS partner_first_index
              FROM source
              WHERE bilateral_dayweighted_additional_rate > 0
              GROUP BY cty_code,hs10
            ), partner_dates AS (
              SELECT s.cty_code,s.hs10,p.partner_first_index,
                     min(s.source_effective_date) AS partner_source_date
              FROM source s JOIN partner_first p USING(cty_code,hs10)
              WHERE s.month_index=p.partner_first_index
                AND s.bilateral_dayweighted_additional_rate > 0
              GROUP BY s.cty_code,s.hs10,p.partner_first_index
            ), product_first AS (
              SELECT hs10,min(month_index) AS product_first_index
              FROM source
              WHERE bilateral_dayweighted_additional_rate > 0
              GROUP BY hs10
            ), product_dates AS (
              SELECT s.hs10,p.product_first_index,
                     min(s.source_effective_date) AS product_source_date
              FROM source s JOIN product_first p USING(hs10)
              WHERE s.month_index=p.product_first_index
                AND s.bilateral_dayweighted_additional_rate > 0
              GROUP BY s.hs10,p.product_first_index
            ), ids AS (
              SELECT DISTINCT id,cty_code,hs10 FROM read_parquet('{_sql(raw)}')
            ), joined AS (
              SELECT i.*,pd.partner_first_index,pd.partner_source_date,
                     pr.product_first_index,pr.product_source_date,
                     CASE WHEN pd.partner_first_index IS NOT NULL THEN
                       make_date(floor(pd.partner_first_index/12)::INTEGER,
                                 (pd.partner_first_index%12)::INTEGER+1,1)
                     END AS partner_first_month,
                     CASE WHEN pr.product_first_index IS NOT NULL THEN
                       make_date(floor(pr.product_first_index/12)::INTEGER,
                                 (pr.product_first_index%12)::INTEGER+1,1)
                     END AS product_first_month
              FROM ids i
              LEFT JOIN partner_dates pd USING(cty_code,hs10)
              LEFT JOIN product_dates pr USING(hs10)
            )
            SELECT *,
              CASE WHEN partner_first_index IS NOT NULL THEN 2
                   WHEN product_first_index IS NOT NULL THEN 1 ELSE 0 END::TINYINT AS historical_status,
              CASE WHEN partner_first_index IS NOT NULL THEN partner_first_month
                   WHEN product_first_index IS NOT NULL THEN product_first_month END AS legal_event_month,
              CASE
                WHEN partner_first_index IS NOT NULL THEN
                  CASE WHEN extract(day FROM partner_source_date)>15
                       THEN partner_first_month + INTERVAL 1 MONTH ELSE partner_first_month END
                WHEN product_first_index IS NOT NULL THEN
                  CASE WHEN extract(day FROM product_source_date)>15
                       THEN product_first_month + INTERVAL 1 MONTH ELSE product_first_month END
              END AS paper_event_month
            FROM joined
            """
            _atomic_copy(con, clock_query, clock)
        result["historical_event_clock_artifact"] = _validate_parquet(clock)
        max_paper_date = con.execute(
            f"SELECT max(paper_event_month) FROM read_parquet('{_sql(clock)}')"
        ).fetchone()[0]
        result["historical_event_clock_max_paper_date"] = str(max_paper_date)
        if str(max_paper_date)[:10] > "2018-10-01":
            raise RuntimeError(f"historical paper clock extends beyond 2018-10: {max_paper_date}")
        for mode, destination in paths.items():
            if overwrite or not destination.exists():
                if mode == "package_full_policy_anchor":
                    query = f"SELECT * FROM read_parquet('{_sql(package)}')"
                else:
                    event_date, tariff, _ = _mode_fields(mode)
                    query = f"""
                    SELECT r.* EXCLUDE(m_effective_mdate2,m_stattariff2,m_status2,m_ess),
                           CAST(c.{event_date} AS TIMESTAMP) AS m_effective_mdate2,
                           p.{tariff} AS m_stattariff2,
                           c.historical_status AS m_status2,
                           c.historical_status AS m_ess,
                           '{mode}' AS policy_mode,
                           '{_relative(config, independent)}' AS policy_source,
                           'partner_specific_v3_clock_scope_fix' AS policy_semantics
                    FROM read_parquet('{_sql(raw)}') r
                    JOIN read_parquet('{_sql(independent)}') p USING(cty_code,hs10,year,month)
                    JOIN read_parquet('{_sql(clock)}') c USING(id,cty_code,hs10)
                    """
                _atomic_copy(con, query, destination)
            result["panels"][mode] = {
                "path": _relative(config, destination),
                **_validate_parquet(destination, expected_rows=expected),
            }
        paper, legal = paths["independent_paper_full_policy"], paths["independent_legal_full_policy"]
        mismatch = int(con.execute(f"""
            SELECT count(*)
            FROM read_parquet('{_sql(paper)}') p
            JOIN read_parquet('{_sql(legal)}') l USING(cty_code,hs10,year,month)
            WHERE p.m_stattariff2 IS DISTINCT FROM l.m_stattariff2
        """).fetchone()[0])
        result["paper_legal_dynamic_tariff_mismatch_rows"] = mismatch
        if mismatch:
            raise RuntimeError(f"paper/legal dynamic tariff paths differ on {mismatch} rows")
    finally:
        con.close()
    write_metadata_json(root(config) / "pooled_policy_regression_v4_panel_manifest.json", result)
    return result


def _coefficient_path(config: PipelineConfig, mode: str, spec: str, outcome: str) -> Path:
    return root(config) / "coefficients" / mode / spec / f"{outcome}.parquet"


def run_fits(
    config: PipelineConfig,
    *,
    modes: list[str],
    specs: list[str],
    outcomes: list[str],
    overwrite: bool = False,
) -> dict[str, Any]:
    panels = panel_paths(config)
    fits: list[dict[str, Any]] = []
    for mode in modes:
        frame = read_table(panels[mode])
        for spec in specs:
            prepared = _prepare_event_study("imports", frame) if spec == "event" else _prepare_dynamic("imports", frame, package_logs=False)
            for outcome in outcomes:
                destination = _coefficient_path(config, mode, spec, outcome)
                destination.parent.mkdir(parents=True, exist_ok=True)
                if destination.exists() and not overwrite:
                    result = read_table(destination)
                else:
                    fit = (
                        _run_event_study_one(config, "imports", outcome, prepared, mode, _relative(config, panels[mode]))
                        if spec == "event"
                        else _run_dynamic_one(config, "imports", outcome, prepared, mode, _relative(config, panels[mode]))
                    )
                    result = fit.frame
                    write_parquet(result, destination, overwrite=True)
                horizon = "event_time" if spec == "event" else "horizon"
                if len(result) != 13 or result[horizon].nunique() != 13:
                    raise RuntimeError(f"incomplete fit: {mode}|{spec}|{outcome}")
                record = {
                    "fit_id": f"{mode}|{spec}|{outcome}",
                    "path": _relative(config, destination),
                    "source_panel": _relative(config, panels[mode]),
                    "source_hash": sha256_file(panels[mode]),
                    "nobs": int(result["nobs"].iloc[0]),
                    "horizons": 13,
                    "created_at_utc": datetime.now(timezone.utc).isoformat(),
                }
                write_metadata_json(destination.with_suffix(".json"), record)
                fits.append(record)
            del prepared
        del frame
    partial = {"version": VERSION, "status": "partial", "fits": fits}
    write_metadata_json(root(config) / "pooled_policy_v4_partial_manifest.json", partial)
    return partial


def _metric(left: pd.DataFrame, right: pd.DataFrame, spec: str, outcome: str, mode: str, comparison: str) -> dict[str, Any]:
    horizon = "event_time" if spec == "event" else "horizon"
    a = left.rename(columns={horizon: "h"})
    b = right.rename(columns={horizon: "h"})
    merged = a[["h", "estimate"]].merge(
        b[["h", "estimate"]].rename(columns={"estimate": "candidate_estimate"}),
        on="h", validate="one_to_one",
    )
    difference = merged["candidate_estimate"] - merged["estimate"]
    post = merged.loc[merged["h"] >= 0]
    return {
        "comparison": comparison,
        "source_mode": mode,
        "spec": spec,
        "outcome": outcome,
        "n_horizons": int(len(merged)),
        "correlation": float(merged["estimate"].corr(merged["candidate_estimate"])),
        "rmse": float(np.sqrt(np.mean(difference * difference))),
        "max_abs_difference": float(np.max(np.abs(difference))),
        "post_treatment_sign_agreement": float(np.mean(np.sign(post["estimate"]) == np.sign(post["candidate_estimate"]))),
    }


def finalize(config: PipelineConfig) -> dict[str, Any]:
    frames: dict[tuple[str, str, str], pd.DataFrame] = {}
    fit_records: list[dict[str, Any]] = []
    for mode in MODES:
        for spec in SPECS:
            for outcome in OUTCOMES:
                path = _coefficient_path(config, mode, spec, outcome)
                if not path.exists():
                    raise FileNotFoundError(path)
                frame = read_table(path)
                horizon = "event_time" if spec == "event" else "horizon"
                if len(frame) != 13 or frame[horizon].nunique() != 13:
                    raise RuntimeError(f"invalid checkpoint: {path}")
                frames[(mode, spec, outcome)] = frame
                fit_records.append({"fit_id": f"{mode}|{spec}|{outcome}", "path": _relative(config, path), "nobs": int(frame["nobs"].iloc[0])})

    all_coefficients = pd.concat(frames.values(), ignore_index=True, sort=False)
    write_parquet(all_coefficients, root(config) / "pooled_policy_v4_coefficients.parquet", overwrite=True)
    package_root = config.verification_dir / "trade_regressions" / "package_benchmark_v5"
    same_sample: list[dict[str, Any]] = []
    package_context: list[dict[str, Any]] = []
    for spec in SPECS:
        package = read_table(package_root / f"package_full_{spec}_coefficients.parquet")
        for outcome in OUTCOMES:
            anchor = frames[("package_full_policy_anchor", spec, outcome)]
            package_outcome = package.loc[(package["spec"] == spec) & (package["outcome"] == outcome)]
            for mode in MODES[1:]:
                candidate = frames[(mode, spec, outcome)]
                same_sample.append(_metric(anchor, candidate, spec, outcome, mode, "same_raw_sample_package_vs_independent_policy"))
                package_context.append(_metric(package_outcome, candidate, spec, outcome, mode, "package_full_vs_independent_policy"))
            package_context.append(_metric(package_outcome, anchor, spec, outcome, "package_full_policy_anchor", "package_full_vs_raw_outcomes_package_policy"))

    same = pd.DataFrame(same_sample)
    context = pd.DataFrame(package_context)
    write_parquet(same, root(config) / "pooled_policy_v4_same_sample_comparisons.parquet", overwrite=True)
    same.to_csv(root(config) / "pooled_policy_v4_same_sample_comparisons.csv", index=False)
    write_parquet(context, root(config) / "pooled_policy_v4_package_context_comparisons.parquet", overwrite=True)
    context.to_csv(root(config) / "pooled_policy_v4_package_context_comparisons.csv", index=False)

    paper = same.loc[same["source_mode"] == "independent_paper_full_policy"].copy()
    checks = {
        "all_13_horizons": bool((paper["n_horizons"] == 13).all()),
        "correlation": bool((paper["correlation"] >= THRESHOLDS["correlation"]).all()),
        "rmse": bool((paper["rmse"] <= THRESHOLDS["rmse"]).all()),
        "max_abs_difference": bool((paper["max_abs_difference"] <= THRESHOLDS["max_abs_difference"]).all()),
        "post_treatment_sign_agreement": bool((paper["post_treatment_sign_agreement"] >= THRESHOLDS["post_treatment_sign_agreement"]).all()),
    }
    gate = {
        "version": VERSION,
        "status": "passed" if all(checks.values()) else "failed",
        "comparison": "same raw outcomes/sample; package full policy versus independent paper-compatible policy",
        "checks": checks,
        "thresholds": THRESHOLDS,
        "legal_calendar_diagnostic_only": True,
        "independent_legal_release_gate": False,
    }
    write_metadata_json(root(config) / "pooled_policy_v4_gate.json", gate)
    figures = _plot(config, frames)
    manifest = {
        "version": VERSION,
        "status": "complete",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "expected_fits": 24,
        "completed_fits": len(fit_records),
        "fits": fit_records,
        "figures": figures,
        "paper_policy_gate": gate,
    }
    write_metadata_json(root(config) / "pooled_policy_v4_regression_manifest.json", manifest)
    return manifest


def _plot_line_with_confidence(
    axis: Any,
    frame: pd.DataFrame,
    *,
    horizon: str,
    color: str,
    label: str,
    linestyle: str,
    marker: str,
) -> None:
    """Draw a coefficient path and its 95-percent confidence band."""
    line = frame.sort_values(horizon)
    x = pd.to_numeric(line[horizon], errors="raise").to_numpy(dtype=float)
    estimate = pd.to_numeric(line["estimate"], errors="raise").to_numpy(dtype=float)
    conf_low = pd.to_numeric(line["conf_low"], errors="coerce").to_numpy(dtype=float)
    conf_high = pd.to_numeric(line["conf_high"], errors="coerce").to_numpy(dtype=float)
    axis.fill_between(x, conf_low, conf_high, color=color, alpha=0.14, linewidth=0, zorder=1)
    axis.plot(
        x,
        estimate,
        color=color,
        linestyle=linestyle,
        marker=marker,
        markersize=3,
        linewidth=1.8,
        label=label,
        zorder=3,
    )


def _plot(config: PipelineConfig, frames: dict[tuple[str, str, str], pd.DataFrame]) -> dict[str, str]:
    figure_root = root(config) / "figures"
    figure_root.mkdir(parents=True, exist_ok=True)
    package_root = config.verification_dir / "trade_regressions" / "package_benchmark_v5"
    outputs: dict[str, str] = {}
    for spec in SPECS:
        horizon = "event_time" if spec == "event" else "horizon"
        package = read_table(package_root / f"package_full_{spec}_coefficients.parquet")
        fig, axes = plt.subplots(2, 2, figsize=(13, 8.5), sharex=True)
        for axis, outcome in zip(axes.flat, OUTCOMES):
            package_line = package.loc[(package["spec"] == spec) & (package["outcome"] == outcome)]
            for mode, color, label, linestyle, marker in FINAL_CHART_SERIES:
                line = package_line if mode == "original" else frames[(mode, spec, outcome)]
                _plot_line_with_confidence(
                    axis,
                    line,
                    horizon=horizon,
                    color=color,
                    label=label,
                    linestyle=linestyle,
                    marker=marker,
                )
            axis.axhline(0, color="0.25", linewidth=.7)
            axis.axvline(0, color="0.6", linewidth=.7, linestyle="--")
            axis.set_title(OUTCOME_LABELS[outcome])
            axis.grid(alpha=.2)
        handles, labels = axes[0, 0].get_legend_handles_labels()
        fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(.5, .985), ncol=3, frameon=False, fontsize=8)
        title = "Event study" if spec == "event" else "Dynamic response"
        fig.suptitle(f"Historical tariff replication: {title}", y=.925)
        fig.tight_layout(rect=(0, 0, 1, .87))
        path = figure_root / f"pooled_policy_v4_{spec}.png"
        fig.savefig(path, dpi=190)
        fig.savefig(path.with_suffix(".pdf"))
        plt.close(fig)
        outputs[spec] = _relative(config, path)
    return outputs


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-panels", action="store_true")
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--finalize-only", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--mode", choices=[*MODES, "all"], default="all")
    parser.add_argument("--spec", choices=[*SPECS, "all"], default="all")
    parser.add_argument("--outcome", choices=[*OUTCOMES, "all"], default="all")
    args = parser.parse_args()
    config = PipelineConfig.default()
    if args.build_panels:
        print(json.dumps(build_panels(config, overwrite=args.overwrite), indent=2))
    if args.run:
        modes = list(MODES) if args.mode == "all" else [args.mode]
        specs = list(SPECS) if args.spec == "all" else [args.spec]
        outcomes = list(OUTCOMES) if args.outcome == "all" else [args.outcome]
        print(json.dumps(run_fits(config, modes=modes, specs=specs, outcomes=outcomes, overwrite=args.overwrite), indent=2))
    if args.finalize_only:
        print(json.dumps(finalize(config), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
