"""Projection-based diagnostics for the package/raw outcome bridge."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import json

import numpy as np
import pandas as pd

from .config import PipelineConfig
from .io_utils import read_table, write_metadata_json, write_parquet


OUTCOMES = ("val", "q1", "p", "pduty")
PACKAGE_TO_RAW_SCALE = {"val": 1_000_000.0, "q1": 1_000_000.0, "p": 1.0, "pduty": 1.0}


def ci_overlap(low_left: float, high_left: float, low_right: float, high_right: float, *, baseline: bool = False) -> float | None:
    """Intersection-over-union interval overlap; normalized event baseline is excluded."""
    if baseline:
        return None
    denominator = max(high_left, high_right) - min(low_left, low_right)
    if denominator <= 0:
        return None
    return max(0.0, min(high_left, high_right) - max(low_left, low_right)) / denominator


def curve_metrics(merged: pd.DataFrame, *, exclude_baseline: bool) -> dict[str, float | int]:
    """Compute horizon-level bridge metrics with an explicit baseline policy."""
    evaluated = merged.loc[merged["horizon"] != -6].copy() if exclude_baseline else merged.copy()
    valid = evaluated["estimate_package"].notna() & evaluated["estimate_raw"].notna()
    differences = evaluated.loc[valid, "estimate_package"] - evaluated.loc[valid, "estimate_raw"]
    overlaps = [
        ci_overlap(row.conf_low_package, row.conf_high_package, row.conf_low_raw, row.conf_high_raw, baseline=bool(row.horizon == -6))
        for row in evaluated.itertuples()
    ]
    return {
        "n_points": int(valid.sum()),
        "correlation": float(evaluated.loc[valid, "estimate_package"].corr(evaluated.loc[valid, "estimate_raw"])),
        "rmse": float(np.sqrt(np.mean(differences**2))),
        "max_abs_difference": float(differences.abs().max()),
        "ci_overlap": float(np.nanmean([value for value in overlaps if value is not None])),
    }


def _root(config: PipelineConfig) -> Path:
    return config.verification_dir / "trade_regressions" / "package_benchmark_v5" / "common_sample" / "bridge" / "diagnosis"


def _relative(config: PipelineConfig, path: Path) -> str:
    try:
        return path.resolve().relative_to(config.repo_root.resolve()).as_posix()
    except ValueError:
        return path.name


def run_bridge_diagnostics(config: PipelineConfig) -> dict[str, object]:
    import duckdb

    base = config.verification_dir / "trade_regressions" / "package_benchmark_v5"
    package = base / "cache" / "package_full_panel_hs10fixed.parquet"
    raw = base / "common_sample" / "raw_outcomes_package_policy_hs10fixed.parquet"
    if not package.exists() or not raw.exists():
        raise FileNotFoundError("Corrected package and raw bridge panels are required")
    out = _root(config)
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(database=":memory:")
    aggregate: list[dict[str, object]] = []
    grouped: list[dict[str, object]] = []
    try:
        for outcome in OUTCOMES:
            pcol = f"p.m_{outcome}"
            rcol = f"r.m_{outcome}"
            query = f"""
                SELECT '{outcome}' AS outcome, 'all' AS breakdown, 'all' AS group_value,
                    count(*) AS common_rows,
                    count({pcol}) AS package_nonmissing, count({rcol}) AS raw_nonmissing,
                    count(*) FILTER (WHERE {pcol} IS NOT NULL AND {rcol} IS NOT NULL) AS both_nonmissing,
                    count(*) FILTER (WHERE {pcol} > 0) AS package_positive,
                    count(*) FILTER (WHERE {rcol} > 0) AS raw_positive,
                    count(*) FILTER (WHERE {pcol} > 0 AND {rcol} > 0) AS both_positive,
                    count(*) FILTER (WHERE {pcol} = {rcol}) AS exact_equal,
                    avg(abs(log(nullif({pcol} * {PACKAGE_TO_RAW_SCALE[outcome]}, 0)) - log(nullif({rcol}, 0)))) FILTER (WHERE {pcol} > 0 AND {rcol} > 0) AS mean_abs_log_diff,
                    quantile_cont(abs(log(nullif({pcol} * {PACKAGE_TO_RAW_SCALE[outcome]}, 0)) - log(nullif({rcol}, 0))), 0.50) FILTER (WHERE {pcol} > 0 AND {rcol} > 0) AS p50_abs_log_diff,
                    quantile_cont(abs(log(nullif({pcol} * {PACKAGE_TO_RAW_SCALE[outcome]}, 0)) - log(nullif({rcol}, 0))), 0.90) FILTER (WHERE {pcol} > 0 AND {rcol} > 0) AS p90_abs_log_diff,
                    corr({pcol}, {rcol}) FILTER (WHERE {pcol} > 0 AND {rcol} > 0) AS level_corr,
                    corr(log(nullif({pcol} * {PACKAGE_TO_RAW_SCALE[outcome]}, 0)), log(nullif({rcol}, 0))) FILTER (WHERE {pcol} > 0 AND {rcol} > 0) AS log_corr
                FROM read_parquet(?) p INNER JOIN read_parquet(?) r USING (cty_code, hs10, year, month)
            """
            aggregate.extend(con.execute(query, [str(package), str(raw)]).fetchdf().to_dict(orient="records"))
            for breakdown, expression in (("month", "concat(cast(p.year as varchar), '-', lpad(cast(p.month as varchar), 2, '0'))"), ("country", "cast(p.cty_code as varchar)"), ("hs2", "substr(cast(p.hs10 as varchar), 1, 2)"), ("hs4", "substr(cast(p.hs10 as varchar), 1, 4)")):
                grouped_query = f"""
                    SELECT '{outcome}' AS outcome, '{breakdown}' AS breakdown, {expression} AS group_value,
                        count(*) AS common_rows,
                        count(*) FILTER (WHERE {pcol} > 0 AND {rcol} > 0) AS both_positive,
                        avg(abs(log(nullif({pcol} * {PACKAGE_TO_RAW_SCALE[outcome]}, 0)) - log(nullif({rcol}, 0)))) FILTER (WHERE {pcol} > 0 AND {rcol} > 0) AS mean_abs_log_diff,
                        quantile_cont(abs(log(nullif({pcol} * {PACKAGE_TO_RAW_SCALE[outcome]}, 0)) - log(nullif({rcol}, 0))), 0.90) FILTER (WHERE {pcol} > 0 AND {rcol} > 0) AS p90_abs_log_diff,
                        corr(log(nullif({pcol} * {PACKAGE_TO_RAW_SCALE[outcome]}, 0)), log(nullif({rcol}, 0))) FILTER (WHERE {pcol} > 0 AND {rcol} > 0) AS log_corr
                    FROM read_parquet(?) p INNER JOIN read_parquet(?) r USING (cty_code, hs10, year, month)
                    GROUP BY 1, 2, 3
                """
                grouped.extend(con.execute(grouped_query, [str(package), str(raw)]).fetchdf().to_dict(orient="records"))
    finally:
        con.close()
    equivalence = pd.DataFrame(aggregate + grouped)
    write_parquet(equivalence, out / "bridge_outcome_equivalence.parquet", overwrite=True)
    equivalence.loc[equivalence["breakdown"] == "all"].to_csv(out / "bridge_outcome_equivalence_summary.csv", index=False)

    loss_rows: list[dict[str, object]] = []
    loss_json = base / "common_sample" / "package_common_sample_loss_audit.json"
    if loss_json.exists():
        payload = json.loads(loss_json.read_text(encoding="utf-8"))
        for row in payload.get("stages", []):
            loss_rows.append({"source_mode": "package_common_sample_anchor", **row})
    for mode in ("package_common_sample_anchor", "raw_outcomes_package_policy"):
        for spec in ("event", "dynamic"):
            for outcome in OUTCOMES:
                path = base / "common_sample" / "bridge" / mode / spec / f"{outcome}.parquet"
                if path.exists():
                    frame = read_table(path, columns=["nobs"])
                    loss_rows.append({"source_mode": mode, "stage": f"effective_{spec}_{outcome}", "rows": int(frame["nobs"].iloc[0]) if not frame.empty else 0, "products": None, "countries": None, "key_hash": None})
    write_parquet(pd.DataFrame(loss_rows), out / "bridge_sample_loss_audit.parquet", overwrite=True)
    pd.DataFrame(loss_rows).groupby(["source_mode", "stage"], dropna=False).agg(rows=("rows", "sum")).reset_index().to_csv(out / "bridge_sample_loss_summary.csv", index=False)

    ci_rows: list[dict[str, object]] = []
    bridge_root = base / "common_sample" / "bridge"
    for spec in ("event", "dynamic"):
        for outcome in OUTCOMES:
            left = read_table(bridge_root / "package_common_sample_anchor" / spec / f"{outcome}.parquet").rename(columns={"event_time": "horizon"})
            right = read_table(bridge_root / "raw_outcomes_package_policy" / spec / f"{outcome}.parquet").rename(columns={"event_time": "horizon"})
            merged = left.merge(right, on=["flow", "spec", "outcome", "horizon"], suffixes=("_package", "_raw"), validate="one_to_one")
            for _, row in merged.iterrows():
                overlap = ci_overlap(row["conf_low_package"], row["conf_high_package"], row["conf_low_raw"], row["conf_high_raw"], baseline=bool(row["horizon"] == -6))
                ci_rows.append({"spec": spec, "outcome": outcome, "horizon": int(row["horizon"]), "baseline": int(row["horizon"] == -6), "ci_overlap": overlap})
    ci = pd.DataFrame(ci_rows)
    write_parquet(ci, out / "bridge_ci_overlap_audit.parquet", overwrite=True)
    sensitivity: list[dict[str, object]] = []
    for spec in ("event", "dynamic"):
        for outcome in OUTCOMES:
            left = read_table(bridge_root / "package_common_sample_anchor" / spec / f"{outcome}.parquet").rename(columns={"event_time": "horizon"})
            right = read_table(bridge_root / "raw_outcomes_package_policy" / spec / f"{outcome}.parquet").rename(columns={"event_time": "horizon"})
            merged = left.merge(right, on=["flow", "spec", "outcome", "horizon"], suffixes=("_package", "_raw"), validate="one_to_one")
            for exclude_baseline in (False, True):
                sensitivity.append({
                    "comparison": "package_common_vs_raw_outcomes_package_policy",
                    "spec": spec,
                    "outcome": outcome,
                    "exclude_baseline": exclude_baseline,
                    **curve_metrics(merged, exclude_baseline=exclude_baseline),
                })
    pd.DataFrame(sensitivity).to_csv(out / "bridge_metric_sensitivity.csv", index=False)
    manifest = {"version": "v5", "created_at_utc": datetime.now(timezone.utc).isoformat(), "package_path": _relative(config, package), "raw_path": _relative(config, raw), "equivalence_path": _relative(config, out / "bridge_outcome_equivalence.parquet"), "status": "diagnostic", "v5_ready": False}
    write_metadata_json(out / "bridge_diagnosis_manifest.json", manifest)
    (out / "bridge_diagnosis_report.md").write_text("# Bridge diagnosis\n\nThis report diagnoses outcome and sample differences. It does not alter policy semantics or release v5.\n", encoding="utf-8")
    return manifest
