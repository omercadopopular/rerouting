"""Same-sample pooled-policy regressions for the historical paper window.

The policy panels are independently constructed from local HTS/Chapter-99
sources.  Outcomes come from the frozen raw-outcome/common-sample artifact;
package policy columns are not used to construct the paper or legal policy
objects.  This module is deliberately separate from the Section-301-only
substitution runner.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from .config import PipelineConfig
from .io_utils import read_table, write_metadata_json, write_parquet, sha256_file
from .pooled_policy_replication_v2 import analysis_root, relative, root as verification_root
from .trade_regressions import (
    _prepare_dynamic,
    _prepare_event_study,
    _run_dynamic_one,
    _run_event_study_one,
)

VERSION = "pooled_policy_regressions_v2"
MODES = ("paper_compatible", "independent_legal")
SPECS = ("event", "dynamic")
OUTCOMES = ("val", "q1", "p", "pduty")


def regression_root(config: PipelineConfig) -> Path:
    path = verification_root(config) / "regressions"
    path.mkdir(parents=True, exist_ok=True)
    return path


def panel_paths(config: PipelineConfig) -> dict[str, Path]:
    return {mode: regression_root(config) / "panels" / f"{mode}.parquet" for mode in MODES}


def _sql(path: Path) -> str:
    return str(path).replace("'", "''")


def build_regression_panels(config: PipelineConfig, *, overwrite: bool = False) -> dict[str, Any]:
    """Join independent policy fields to the frozen raw-outcome common sample."""
    source = config.verification_dir / "trade_regressions" / "package_benchmark_v5" / "common_sample_v5_cif" / "raw_outcomes_package_policy_cif.parquet"
    policy = analysis_root(config)
    sources = {
        "paper_compatible": policy / "paper_compatible_full_trade_policy_panel.parquet",
        "independent_legal": policy / "independent_legal_full_trade_policy_panel.parquet",
    }
    if not source.exists() or not all(path.exists() for path in sources.values()):
        raise FileNotFoundError("Missing raw-outcome common sample or pooled policy panel")
    import duckdb

    result: dict[str, Any] = {"version": VERSION, "source_outcomes": relative(config, source), "panels": {}}
    for mode, destination in panel_paths(config).items():
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists() and not overwrite:
            continue
        policy_path = sources[mode]
        if mode == "paper_compatible":
            active = "independent_treated"
            active_two = "((coalesce(p.solar_201_hit,0)+coalesce(p.washer_201_hit,0)+coalesce(p.steel_232_hit,0)+coalesce(p.aluminum_232_hit,0)>0) OR (coalesce(p.china_301_hit,0)>0 AND p.cty_code=5700))"
            effective = "independent_paper_effective_month"
            tariff = "independent_paper_dayweighted_total_tariff"
            ever_partition = "p.hs10"
        else:
            active = "independent_treated"
            active_two = "p.independent_treated"
            effective = "independent_legal_effective_month"
            tariff = "independent_legal_dayweighted_total_tariff"
            ever_partition = "p.cty_code, p.hs10"
        temp = destination.with_name(f".{destination.name}.{VERSION}.tmp")
        temp.unlink(missing_ok=True)
        con = duckdb.connect()
        try:
            query = f"""
            SELECT r.* EXCLUDE(m_effective_mdate2, m_stattariff2, m_status2, m_ess),
                   CASE WHEN min(p.{effective}) OVER (PARTITION BY {ever_partition}) IS NOT NULL
                        THEN strptime(min(p.{effective}) OVER (PARTITION BY {ever_partition}) || '-01', '%Y-%m-%d') END AS m_effective_mdate2,
                   p.{tariff} AS m_stattariff2,
                   CASE WHEN {active_two} THEN 2
                        WHEN max(CASE WHEN p.{active} THEN 1 ELSE 0 END) OVER (PARTITION BY {ever_partition})=1 THEN 1
                        ELSE 0 END::TINYINT AS m_status2,
                   CASE WHEN {active_two} THEN 2
                        WHEN max(CASE WHEN p.{active} THEN 1 ELSE 0 END) OVER (PARTITION BY {ever_partition})=1 THEN 1
                        ELSE 0 END::TINYINT AS m_ess,
                   '{mode}' AS pooled_policy_mode,
                   '{relative(config, policy_path)}' AS pooled_policy_source
            FROM read_parquet('{_sql(source)}') r
            JOIN read_parquet('{_sql(policy_path)}') p USING (cty_code, hs10, year, month)
            """
            con.execute(f"COPY ({query}) TO '{_sql(temp)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            import pyarrow.parquet as pq

            pf = pq.ParquetFile(temp)
            if pf.metadata.num_rows <= 0 or any(
                pf.metadata.row_group(0).column(i).compression != "ZSTD"
                for i in range(pf.metadata.row_group(0).num_columns)
            ):
                raise RuntimeError(f"Invalid pooled regression panel: {destination}")
            del pf
            temp.replace(destination)
        finally:
            con.close()
            temp.unlink(missing_ok=True)
        result["panels"][mode] = {
            "path": relative(config, destination),
            "sha256": sha256_file(destination),
            "rows": int(read_table(destination, columns=["hs10"]).shape[0]),
            "source_policy": relative(config, policy_path),
        }
    write_metadata_json(regression_root(config) / "pooled_regression_panel_manifest.json", result)
    return result


def run_regressions(
    config: PipelineConfig,
    *,
    modes: list[str],
    specs: list[str],
    outcomes: list[str],
    overwrite: bool = False,
) -> dict[str, Any]:
    paths = panel_paths(config)
    output_root = regression_root(config) / "coefficients"
    output_root.mkdir(parents=True, exist_ok=True)
    records: list[pd.DataFrame] = []
    manifest: dict[str, Any] = {"version": VERSION, "created_at_utc": datetime.now(timezone.utc).isoformat(), "fits": []}
    for mode in modes:
        for spec in specs:
            frame = read_table(paths[mode])
            # The raw-outcome artifact intentionally contains reconstructed
            # outcomes, not the authors' precomputed lm_* fields.  The
            # package-only benchmark remains the lm_* reference; pooled raw
            # policy diagnostics reconstruct logs from the raw outcome fields.
            prepared = _prepare_event_study("imports", frame) if spec == "event" else _prepare_dynamic("imports", frame, package_logs=False)
            for outcome in outcomes:
                destination = output_root / mode / spec / f"{outcome}.parquet"
                destination.parent.mkdir(parents=True, exist_ok=True)
                if destination.exists() and not overwrite:
                    fit = read_table(destination)
                else:
                    result = (_run_event_study_one(config, "imports", outcome, prepared, mode, relative(config, paths[mode]))
                              if spec == "event" else
                              _run_dynamic_one(config, "imports", outcome, prepared, mode, relative(config, paths[mode])))
                    fit = result.frame
                    write_parquet(fit, destination, overwrite=True)
                records.append(fit)
                manifest["fits"].append({
                    "fit_id": f"{mode}|{spec}|{outcome}",
                    "path": relative(config, destination),
                    "rows": int(len(fit)),
                    "nobs": int(fit["nobs"].iloc[0]),
                    "source_mode": mode,
                })
    all_coefficients = pd.concat(records, ignore_index=True)
    write_parquet(all_coefficients, regression_root(config) / "pooled_policy_coefficients.parquet", overwrite=True)
    write_metadata_json(regression_root(config) / "pooled_policy_regression_manifest.json", manifest)
    _write_comparisons(config, all_coefficients)
    return manifest


def _write_comparisons(config: PipelineConfig, coefficients: pd.DataFrame) -> None:
    root = regression_root(config)
    package_root = config.verification_dir / "trade_regressions" / "package_benchmark_v5"
    rows: list[dict[str, Any]] = []
    for _, candidate in coefficients.groupby(["source_mode", "spec", "outcome"], dropna=False):
        mode, spec, outcome = candidate[["source_mode", "spec", "outcome"]].iloc[0]
        package_path = package_root / f"package_full_{'event' if spec == 'event' else 'dynamic'}_coefficients.parquet"
        package = read_table(package_path)
        package = package.loc[(package["spec"] == spec) & (package["outcome"] == outcome)].copy()
        horizon = "event_time" if spec == "event" else "horizon"
        candidate = candidate.rename(columns={horizon: "h"})
        package = package.rename(columns={horizon: "h"})
        merged = candidate.merge(package[["h", "estimate", "std_error"]].rename(columns={"estimate": "package_estimate", "std_error": "package_std_error"}), on="h", how="inner")
        diff = merged["estimate"] - merged["package_estimate"]
        rows.append({
            "source_mode": mode,
            "spec": spec,
            "outcome": outcome,
            "n_horizons": int(len(merged)),
            "correlation": float(merged["estimate"].corr(merged["package_estimate"])),
            "rmse": float(np.sqrt(np.mean(diff * diff))),
            "max_abs_difference": float(np.max(np.abs(diff))),
            "post_treatment_sign_agreement": float(np.mean(np.sign(merged.loc[merged.h >= 0, "estimate"]) == np.sign(merged.loc[merged.h >= 0, "package_estimate"]))),
        })
    summary = pd.DataFrame(rows)
    write_parquet(summary, root / "pooled_policy_curve_comparison.parquet", overwrite=True)
    summary.to_csv(root / "pooled_policy_curve_comparison.csv", index=False)
    paper = summary.loc[summary["source_mode"].eq("paper_compatible")]
    checks = {
        "all_13_horizons": bool((paper["n_horizons"] == 13).all()) if not paper.empty else False,
        "correlation": bool((paper["correlation"] >= 0.95).all()) if not paper.empty else False,
        "rmse": bool((paper["rmse"] <= 1.25).all()) if not paper.empty else False,
        "max_abs_difference": bool((paper["max_abs_difference"] <= 2.50).all()) if not paper.empty else False,
        "post_treatment_sign_agreement": bool((paper["post_treatment_sign_agreement"] >= 0.50).all()) if not paper.empty else False,
    }
    write_metadata_json(root / "pooled_policy_regression_gate.json", {
        "version": VERSION,
        "status": "passed" if all(checks.values()) else "failed",
        "source_mode": "paper_compatible",
        "checks": checks,
        "thresholds": {"correlation": 0.95, "rmse": 1.25, "max_abs_difference": 2.50, "post_treatment_sign_agreement": 0.50},
        "legal_calendar_is_diagnostic_only": True,
        "section301_legal_release_gate": False,
    })
    _plot(config, coefficients)


def _plot(config: PipelineConfig, coefficients: pd.DataFrame) -> None:
    root = regression_root(config) / "figures"
    root.mkdir(parents=True, exist_ok=True)
    for spec in SPECS:
        horizon = "event_time" if spec == "event" else "horizon"
        fig, axes = plt.subplots(2, 2, figsize=(12, 8), sharex=True)
        for ax, outcome in zip(axes.ravel(), OUTCOMES):
            package_path = config.verification_dir / "trade_regressions" / "package_benchmark_v5" / f"package_full_{'event' if spec == 'event' else 'dynamic'}_coefficients.parquet"
            if package_path.exists():
                package = read_table(package_path)
                package = package.loc[(package["spec"] == spec) & (package["outcome"] == outcome)].sort_values(horizon)
                if not package.empty:
                    ax.plot(package[horizon], package["estimate"], color="#111111", linestyle="--", linewidth=1.3, label="Package-only benchmark")
            for mode, color, label in (("paper_compatible", "#2b8c6b", "Independent raw policy, paper calendar"), ("independent_legal", "#d95f02", "Independent raw policy, legal calendar")):
                line = coefficients.loc[(coefficients.source_mode == mode) & (coefficients.spec == spec) & (coefficients.outcome == outcome)].copy()
                if not line.empty:
                    line = line.sort_values(horizon)
                    ax.plot(line[horizon], line.estimate, marker="o", linewidth=1.5, color=color, label=label)
            ax.set_title(outcome)
            ax.axhline(0, color="black", linewidth=.6)
            ax.grid(alpha=.2)
        axes[0, 0].legend(fontsize=8)
        fig.suptitle(f"Pooled independent policy regression: {spec}")
        fig.tight_layout()
        fig.savefig(root / f"pooled_{spec}_policy_comparison.png", dpi=160)
        fig.savefig(root / f"pooled_{spec}_policy_comparison.pdf")
        plt.close(fig)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-panels", action="store_true")
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--mode", choices=[*MODES, "all"], default="all")
    parser.add_argument("--spec", choices=[*SPECS, "all"], default="all")
    parser.add_argument("--outcome", choices=[*OUTCOMES, "all"], default="all")
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()
    config = PipelineConfig.default()
    if args.build_panels:
        print(build_regression_panels(config, overwrite=args.overwrite))
    if args.run:
        modes = list(MODES) if args.mode == "all" else [args.mode]
        specs = list(SPECS) if args.spec == "all" else [args.spec]
        outcomes = list(OUTCOMES) if args.outcome == "all" else [args.outcome]
        print(run_regressions(config, modes=modes, specs=specs, outcomes=outcomes, overwrite=args.overwrite))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
