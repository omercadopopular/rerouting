"""Same-sample policy substitutions for the v3 policy objects."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import duckdb

from .config import PipelineConfig
from .io_utils import read_table, sha256_file, write_metadata_json, write_parquet
from .pooled_policy_replication_v3 import analysis_root, relative, verification_root
from .trade_regressions import (
    _prepare_dynamic,
    _prepare_event_study,
    _run_dynamic_one,
    _run_event_study_one,
)

VERSION = "pooled_policy_regressions_v3"
MODES = ("raw_outcomes_paper_policy", "raw_outcomes_legal_policy")
SPECS = ("event", "dynamic")
OUTCOMES = ("val", "q1", "p", "pduty")
SOURCE = "data/verification/passthru_data/trade_regressions/package_benchmark_v5/common_sample_v5_cif/raw_outcomes_package_policy_cif.parquet"
ANCHOR = "data/verification/passthru_data/raw_replication_imports/policy_replication_v2/regressions/section301_policy_coefficients.parquet"
PACKAGE_EVENT = "data/verification/passthru_data/trade_regressions/package_benchmark_v5/package_full_event_coefficients.parquet"
PACKAGE_DYNAMIC = "data/verification/passthru_data/trade_regressions/package_benchmark_v5/package_full_dynamic_coefficients.parquet"


def root(config: PipelineConfig) -> Path:
    path = verification_root(config) / "regressions"
    path.mkdir(parents=True, exist_ok=True)
    return path


def panel_paths(config: PipelineConfig) -> dict[str, Path]:
    return {mode: root(config) / "panels" / f"{mode}.parquet" for mode in MODES}


def _sql(path: Path) -> str:
    return str(path).replace("'", "''")


def _validate(path: Path) -> dict[str, Any]:
    import pyarrow.parquet as pq
    pf = pq.ParquetFile(path)
    compression = sorted({pf.metadata.row_group(r).column(c).compression
                          for r in range(pf.metadata.num_row_groups)
                          for c in range(pf.metadata.row_group(r).num_columns)})
    if compression != ["ZSTD"]:
        raise RuntimeError(f"Expected ZSTD Parquet: {path} -> {compression}")
    return {"path": relative(PipelineConfig.default(), path), "rows": int(pf.metadata.num_rows), "compression": compression, "sha256": sha256_file(path)}


def build_regression_panels(config: PipelineConfig, *, overwrite: bool = False) -> dict[str, Any]:
    source = config.repo_root / SOURCE
    policy = analysis_root(config) / "pooled_policy_replication_v3_panel.parquet"
    if not source.exists() or not policy.exists():
        raise FileNotFoundError("Missing raw common sample or v3 policy panel")
    out: dict[str, Any] = {"version": VERSION, "source": SOURCE, "policy": relative(config, policy), "panels": {}}
    for mode, destination in panel_paths(config).items():
        if destination.exists() and not overwrite:
            out["panels"][mode] = _validate(destination)
            continue
        destination.parent.mkdir(parents=True, exist_ok=True)
        if mode == "raw_outcomes_paper_policy":
            tariff = "paper_dynamic_total_tariff"
            date = "paper_event_mdate"
            status = "paper_event_status"
        else:
            tariff = "legal_dynamic_total_tariff"
            date = "legal_event_month"
            status = "legal_event_status"
        temporary = destination.with_name(f".{destination.name}.{VERSION}.tmp")
        temporary.unlink(missing_ok=True)
        con = duckdb.connect()
        try:
            query = f"""
              SELECT r.* EXCLUDE(m_effective_mdate2, m_stattariff2, m_status2, m_ess),
                     CAST(p.{date} AS TIMESTAMP) AS m_effective_mdate2,
                     p.{tariff} AS m_stattariff2,
                     p.{status} AS m_status2,
                     p.{status} AS m_ess,
                     '{mode}' AS policy_source_mode,
                     '{relative(config, policy)}' AS policy_source_path,
                     '{VERSION}' AS policy_replication_version
              FROM read_parquet('{_sql(source)}') r
              JOIN read_parquet('{_sql(policy)}') p USING(cty_code, hs10, year, month)
            """
            con.execute(f"COPY ({query}) TO '{_sql(temporary)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        finally:
            con.close()
        _validate(temporary)
        temporary.replace(destination)
        out["panels"][mode] = _validate(destination)
    write_metadata_json(root(config) / "pooled_policy_v3_panel_manifest.json", out)
    return out


def _anchor_coefficients(config: PipelineConfig) -> pd.DataFrame:
    path = config.repo_root / ANCHOR
    if not path.exists():
        raise FileNotFoundError(path)
    frame = read_table(path)
    frame = frame.loc[frame["fit_id"].astype(str).str.startswith("raw_outcomes_package_section301_policy_anchor|")].copy()
    frame["fit_id"] = frame["fit_id"].str.replace("raw_outcomes_package_section301_policy_anchor", "raw_outcomes_package_policy_anchor", regex=False)
    return frame


def _package_full_coefficients(config: PipelineConfig) -> pd.DataFrame:
    """Load the verified package-only benchmark as plotting context only."""
    frames = []
    for spec, rel in (("event", PACKAGE_EVENT), ("dynamic", PACKAGE_DYNAMIC)):
        path = config.repo_root / rel
        if not path.exists():
            continue
        frame = read_table(path).copy()
        frame["fit_id"] = "package_full_benchmark|" + spec + "|" + frame["outcome"].astype(str)
        frame["source_mode"] = "package_full_benchmark"
        frames.append(frame)
    return pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()


def run_regressions(config: PipelineConfig, *, overwrite: bool = False) -> dict[str, Any]:
    paths = panel_paths(config)
    records = [_anchor_coefficients(config)]
    package_context = _package_full_coefficients(config)
    if not package_context.empty:
        records.append(package_context)
    fits: list[dict[str, Any]] = []
    for mode in MODES:
        frame = read_table(paths[mode])
        for spec in SPECS:
            prepared = _prepare_event_study("imports", frame) if spec == "event" else _prepare_dynamic("imports", frame, package_logs=False)
            for outcome in OUTCOMES:
                destination = root(config) / "coefficients" / mode / spec / f"{outcome}.parquet"
                destination.parent.mkdir(parents=True, exist_ok=True)
                if destination.exists() and not overwrite:
                    result = read_table(destination)
                else:
                    fit = (_run_event_study_one(config, "imports", outcome, prepared, mode, relative(config, paths[mode]))
                           if spec == "event" else
                           _run_dynamic_one(config, "imports", outcome, prepared, mode, relative(config, paths[mode])))
                    result = fit.frame
                    write_parquet(result, destination, overwrite=True)
                result = result.copy()
                result["fit_id"] = f"{mode}|{spec}|{outcome}"
                records.append(result)
                fits.append({"fit_id": result["fit_id"].iloc[0], "path": relative(config, destination), "rows": int(len(result)), "nobs": int(result["nobs"].iloc[0])})
            del prepared
        del frame
    coefficients = pd.concat(records, ignore_index=True, sort=False)
    write_parquet(coefficients, root(config) / "pooled_policy_v3_coefficients.parquet", overwrite=True)
    comparisons = _comparisons(config, coefficients)
    write_parquet(comparisons, root(config) / "pooled_policy_v3_same_sample_comparison.parquet", overwrite=True)
    comparisons.to_csv(root(config) / "pooled_policy_v3_same_sample_comparison.csv", index=False)
    package_comparisons = _package_comparisons(config, coefficients)
    write_parquet(package_comparisons, root(config) / "pooled_policy_v3_package_comparison.parquet", overwrite=True)
    package_comparisons.to_csv(root(config) / "pooled_policy_v3_package_comparison.csv", index=False)
    figures = _plots(config, coefficients)
    manifest = {"version": VERSION, "created_at_utc": datetime.now(timezone.utc).isoformat(), "fits": fits, "anchor_source": ANCHOR, "figures": figures}
    write_metadata_json(root(config) / "pooled_policy_v3_regression_manifest.json", manifest)
    return manifest


def finalize_regressions(config: PipelineConfig) -> dict[str, Any]:
    """Finalize already-materialized v3 fits without reopening 4.2m-row panels."""
    records = [_anchor_coefficients(config)]
    package_context = _package_full_coefficients(config)
    if not package_context.empty:
        records.append(package_context)
    fits: list[dict[str, Any]] = []
    for mode in MODES:
        for spec in SPECS:
            for outcome in OUTCOMES:
                destination = root(config) / "coefficients" / mode / spec / f"{outcome}.parquet"
                if not destination.exists():
                    raise FileNotFoundError(f"missing v3 fit: {relative(config, destination)}")
                result = read_table(destination).copy()
                if len(result) != 13:
                    raise ValueError(f"v3 fit is not 13 horizons: {destination}")
                result["fit_id"] = f"{mode}|{spec}|{outcome}"
                records.append(result)
                fits.append({"fit_id": result["fit_id"].iloc[0], "path": relative(config, destination), "rows": int(len(result)), "nobs": int(result["nobs"].iloc[0])})
    coefficients = pd.concat(records, ignore_index=True, sort=False)
    write_parquet(coefficients, root(config) / "pooled_policy_v3_coefficients.parquet", overwrite=True)
    comparisons = _comparisons(config, coefficients)
    write_parquet(comparisons, root(config) / "pooled_policy_v3_same_sample_comparison.parquet", overwrite=True)
    comparisons.to_csv(root(config) / "pooled_policy_v3_same_sample_comparison.csv", index=False)
    package_comparisons = _package_comparisons(config, coefficients)
    write_parquet(package_comparisons, root(config) / "pooled_policy_v3_package_comparison.parquet", overwrite=True)
    package_comparisons.to_csv(root(config) / "pooled_policy_v3_package_comparison.csv", index=False)
    figures = _plots(config, coefficients)
    manifest = {"version": VERSION, "created_at_utc": datetime.now(timezone.utc).isoformat(), "fits": fits, "anchor_source": ANCHOR, "figures": figures, "finalized_from_existing_checkpoints": True}
    write_metadata_json(root(config) / "pooled_policy_v3_regression_manifest.json", manifest)
    return manifest


def _metrics(left: pd.DataFrame, right: pd.DataFrame, horizon: str) -> dict[str, float]:
    left = left.rename(columns={horizon: "h"})
    right = right.rename(columns={horizon: "h"})
    merged = left.merge(right, on="h", suffixes=("_anchor", "_candidate"), validate="one_to_one")
    diff = merged["estimate_candidate"] - merged["estimate_anchor"]
    post = merged.loc[merged["h"] >= 0]
    return {
        "n_horizons": int(len(merged)),
        "correlation": float(merged["estimate_anchor"].corr(merged["estimate_candidate"])),
        "rmse": float(np.sqrt(np.mean(diff * diff))),
        "max_abs_difference": float(np.max(np.abs(diff))),
        "post_treatment_sign_agreement": float(np.mean(np.sign(post["estimate_anchor"]) == np.sign(post["estimate_candidate"]))),
    }


def _comparisons(config: PipelineConfig, coefficients: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for spec in SPECS:
        horizon = "event_time" if spec == "event" else "horizon"
        for outcome in OUTCOMES:
            anchor = coefficients.loc[coefficients["fit_id"].eq(f"raw_outcomes_package_policy_anchor|{spec}|{outcome}")]
            for mode in MODES:
                candidate = coefficients.loc[coefficients["fit_id"].eq(f"{mode}|{spec}|{outcome}")]
                rows.append({"comparison": "same_sample_package_policy_anchor_vs_reconstructed", "spec": spec, "outcome": outcome, "source_mode": mode, **_metrics(anchor, candidate, horizon)})
    return pd.DataFrame(rows)


def _package_comparisons(config: PipelineConfig, coefficients: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for spec in SPECS:
        horizon = "event_time" if spec == "event" else "horizon"
        for outcome in OUTCOMES:
            package = coefficients.loc[coefficients["fit_id"].eq(f"package_full_benchmark|{spec}|{outcome}")]
            if package.empty:
                continue
            for mode in MODES:
                candidate = coefficients.loc[coefficients["fit_id"].eq(f"{mode}|{spec}|{outcome}")]
                rows.append({"comparison": "package_full_benchmark_vs_reconstructed", "spec": spec, "outcome": outcome, "source_mode": mode, **_metrics(package, candidate, horizon)})
    return pd.DataFrame(rows)


def _plots(config: PipelineConfig, coefficients: pd.DataFrame) -> dict[str, str]:
    out = root(config) / "figures"
    out.mkdir(parents=True, exist_ok=True)
    result: dict[str, str] = {}
    labels = {
        "package_full_benchmark": ("#202020", "-", "Package-only benchmark (PDF anchor)"),
        "raw_outcomes_package_policy_anchor": ("#d35f00", "--", "Raw outcomes, package bilateral policy"),
        "raw_outcomes_paper_policy": ("#2b8c6b", ":", "Raw outcomes, paper event / bilateral tariff"),
        "raw_outcomes_legal_policy": ("#d95f02", "-", "Raw outcomes, legal applicability / bilateral tariff"),
    }
    for spec in SPECS:
        horizon = "event_time" if spec == "event" else "horizon"
        fig, axes = plt.subplots(2, 2, figsize=(12.8, 8.4), sharex=True)
        for axis, outcome in zip(axes.flat, OUTCOMES):
            for mode, (color, style, label) in labels.items():
                line = coefficients.loc[coefficients["fit_id"].eq(f"{mode}|{spec}|{outcome}")].sort_values(horizon)
                if line.empty:
                    continue
                axis.plot(line[horizon], line["estimate"], color=color, linestyle=style, marker="o" if "anchor" in mode else None, linewidth=1.8, label=label)
            axis.axhline(0, color="0.3", linewidth=.7)
            axis.axvline(0, color="0.55", linewidth=.8, linestyle="--")
            axis.set_title(outcome)
            axis.grid(axis="y", color="0.9", linewidth=.6)
        handles, labels_text = axes[0, 0].get_legend_handles_labels()
        fig.legend(handles, labels_text, loc="upper center", ncol=2, frameon=False, fontsize=8)
        fig.suptitle(f"V3 same-sample policy comparison: {spec}")
        fig.tight_layout(rect=(.02, .04, .98, .90))
        path = out / f"pooled_policy_v3_same_sample_{spec}.png"
        fig.savefig(path, dpi=200)
        fig.savefig(path.with_suffix(".pdf"))
        plt.close(fig)
        result[spec] = relative(config, path)
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-panels", action="store_true")
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--finalize-only", action="store_true")
    args = parser.parse_args()
    config = PipelineConfig.default()
    if args.build_panels:
        print(json.dumps(build_regression_panels(config, overwrite=args.overwrite), indent=2, default=str))
    if args.run:
        print(json.dumps(run_regressions(config, overwrite=args.overwrite), indent=2, default=str))
    if args.finalize_only:
        print(json.dumps(finalize_regressions(config), indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
