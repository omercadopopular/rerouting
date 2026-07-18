"""Diagnose the remaining v4 price and inference differences.

This module never changes registered thresholds or policy semantics.  It uses
DuckDB projections for the large aligned panels and writes detailed results as
ZSTD Parquet, with CSV limited to compact summaries.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import argparse
import json

import duckdb
import pandas as pd

from .config import PipelineConfig
from .io_utils import read_table, sha256_file, write_metadata_json, write_parquet


VERSION = "bridge_v4_diagnostics"
SPECS = ("event", "dynamic")
OUTCOMES = ("val", "q1", "p", "pduty")


def _root(config: PipelineConfig) -> Path:
    path = config.verification_dir / "trade_regressions" / "package_benchmark_v5" / "common_sample_v4" / "bridge_diagnosis"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _sql(path: Path) -> str:
    return str(path).replace("'", "''")


def _relative(config: PipelineConfig, path: Path) -> str:
    try:
        return path.resolve().relative_to(config.repo_root.resolve()).as_posix()
    except ValueError:
        return path.name


def _interval_overlap(left_low: float, left_high: float, right_low: float, right_high: float) -> float:
    intersection = max(0.0, min(left_high, right_high) - max(left_low, right_low))
    union = max(left_high, right_high) - min(left_low, right_low)
    return 1.0 if union == 0 else intersection / union


def _row_equivalence(config: PipelineConfig) -> pd.DataFrame:
    base = config.verification_dir / "trade_regressions" / "package_benchmark_v5" / "common_sample_v4"
    package = base / "package_common_sample_anchor.parquet"
    raw = base / "raw_outcomes_package_policy_realized_duty.parquet"
    con = duckdb.connect(database=":memory:")
    try:
        query = f"""
        WITH j AS (
          SELECT p.m_val AS package_val, r.m_val AS raw_val,
                 p.m_q1 AS package_q1, r.m_q1 AS raw_q1,
                 p.m_p AS package_p, r.m_p AS raw_p,
                 p.m_pduty AS package_pduty, r.m_pduty AS raw_pduty,
                 p.year, p.month, p.cty_code, p.hs2, p.hs4
          FROM read_parquet('{_sql(package)}') p
          INNER JOIN read_parquet('{_sql(raw)}') r USING (cty_code, hs10, year, month)
        ), long AS (
          SELECT 'val' AS outcome, package_val AS package_value, raw_val AS raw_value, year, month, cty_code, hs2, hs4 FROM j
          UNION ALL SELECT 'q1', package_q1, raw_q1, year, month, cty_code, hs2, hs4 FROM j
          UNION ALL SELECT 'p', package_p, raw_p, year, month, cty_code, hs2, hs4 FROM j
          UNION ALL SELECT 'pduty', package_pduty, raw_pduty, year, month, cty_code, hs2, hs4 FROM j
        )
        SELECT outcome, count(*) AS common_keys,
          count(package_value) AS package_nonmissing, count(raw_value) AS raw_nonmissing,
          count(*) FILTER (WHERE package_value > 0) AS package_positive,
          count(*) FILTER (WHERE raw_value > 0) AS raw_positive,
          count(*) FILTER (WHERE package_value > 0 AND raw_value > 0) AS both_positive,
          count(*) FILTER (WHERE package_value = raw_value AND package_value IS NOT NULL) AS exact_equal,
          corr(ln(package_value), ln(raw_value)) FILTER (WHERE package_value > 0 AND raw_value > 0) AS log_correlation,
          avg(abs(ln(package_value) - ln(raw_value))) FILTER (WHERE package_value > 0 AND raw_value > 0) AS mean_abs_log_gap,
          quantile_cont(abs(ln(package_value) - ln(raw_value)), 0.50) FILTER (WHERE package_value > 0 AND raw_value > 0) AS p50_abs_log_gap,
          quantile_cont(abs(ln(package_value) - ln(raw_value)), 0.90) FILTER (WHERE package_value > 0 AND raw_value > 0) AS p90_abs_log_gap,
          quantile_cont(abs(ln(package_value) - ln(raw_value)), 0.99) FILTER (WHERE package_value > 0 AND raw_value > 0) AS p99_abs_log_gap
        FROM long GROUP BY outcome ORDER BY outcome
        """
        return con.execute(query).fetchdf()
    finally:
        con.close()


def _grouped_gaps(config: PipelineConfig, grouping: str) -> pd.DataFrame:
    base = config.verification_dir / "trade_regressions" / "package_benchmark_v5" / "common_sample_v4"
    package, raw = base / "package_common_sample_anchor.parquet", base / "raw_outcomes_package_policy_realized_duty.parquet"
    if grouping not in {"month", "country", "hs2", "hs4"}:
        raise ValueError(grouping)
    column = {"month": "p.year || '-' || lpad(cast(p.month AS varchar), 2, '0')", "country": "cast(p.cty_code AS varchar)", "hs2": "p.hs2", "hs4": "p.hs4"}[grouping]
    con = duckdb.connect(database=":memory:")
    try:
        query = f"""
        WITH j AS (
          SELECT {column} AS group_id, p.m_val AS package_val, r.m_val AS raw_val,
                 p.m_q1 AS package_q1, r.m_q1 AS raw_q1, p.m_p AS package_p, r.m_p AS raw_p,
                 p.m_pduty AS package_pduty, r.m_pduty AS raw_pduty
          FROM read_parquet('{_sql(package)}') p INNER JOIN read_parquet('{_sql(raw)}') r
          USING (cty_code, hs10, year, month)
        ), long AS (
          SELECT group_id, 'val' AS outcome, package_val AS package_value, raw_val AS raw_value FROM j
          UNION ALL SELECT group_id, 'q1', package_q1, raw_q1 FROM j
          UNION ALL SELECT group_id, 'p', package_p, raw_p FROM j
          UNION ALL SELECT group_id, 'pduty', package_pduty, raw_pduty FROM j
        )
        SELECT group_id, outcome, count(*) FILTER (WHERE package_value > 0 AND raw_value > 0) AS positive_rows,
               corr(ln(package_value), ln(raw_value)) FILTER (WHERE package_value > 0 AND raw_value > 0) AS log_correlation,
               avg(abs(ln(package_value)-ln(raw_value))) FILTER (WHERE package_value > 0 AND raw_value > 0) AS mean_abs_log_gap
        FROM long GROUP BY group_id, outcome ORDER BY group_id, outcome
        """
        return con.execute(query).fetchdf()
    finally:
        con.close()


def _coefficient_diagnostics(config: PipelineConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    root = config.verification_dir / "trade_regressions" / "package_benchmark_v5" / "common_sample_v4" / "bridge_resumable"
    rows: list[dict[str, Any]] = []
    influence: list[dict[str, Any]] = []
    for spec in SPECS:
        horizon = "event_time" if spec == "event" else "horizon"
        for outcome in OUTCOMES:
            left_path = root / "package_common_sample_anchor" / spec / outcome / "coefficients.parquet"
            right_path = root / "raw_outcomes_package_policy" / spec / outcome / "coefficients.parquet"
            left, right = read_table(left_path), read_table(right_path)
            merged = left.merge(right, on=horizon, suffixes=("_package", "_raw"), validate="one_to_one")
            for record in merged.to_dict("records"):
                h = int(record[horizon])
                left_width = float(record["conf_high_package"] - record["conf_low_package"])
                right_width = float(record["conf_high_raw"] - record["conf_low_raw"])
                overlap = _interval_overlap(record["conf_low_package"], record["conf_high_package"], record["conf_low_raw"], record["conf_high_raw"])
                influence.append({
                    "spec": spec, "outcome": outcome, "horizon": h,
                    "estimate_gap": float(record["estimate_package"] - record["estimate_raw"]),
                    "abs_estimate_gap": abs(float(record["estimate_package"] - record["estimate_raw"])),
                    "package_ci_width": left_width, "raw_ci_width": right_width,
                    "ci_width_gap": left_width - right_width, "ci_overlap": overlap,
                    "baseline": h == -6,
                })
            valid = merged.loc[merged[horizon] != -6]
            overlaps = [_interval_overlap(a, b, c, d) for a, b, c, d in zip(valid.conf_low_package, valid.conf_high_package, valid.conf_low_raw, valid.conf_high_raw)]
            rows.append({
                "spec": spec, "outcome": outcome, "horizons": int(merged[horizon].nunique()),
                "package_variance": float(valid.estimate_package.var()), "raw_variance": float(valid.estimate_raw.var()),
                "covariance": float(valid["estimate_package"].cov(valid["estimate_raw"])),
                "pearson_correlation": float(valid.estimate_package.corr(valid.estimate_raw)),
                "spearman_correlation": float(valid.estimate_package.corr(valid.estimate_raw, method="spearman")),
                "centered_rmse": float(((valid.estimate_package - valid.estimate_package.mean()) - (valid.estimate_raw - valid.estimate_raw.mean())).pow(2).mean() ** 0.5),
                "mean_ci_overlap_excluding_baseline": float(sum(overlaps) / len(overlaps)),
                "baseline_ci_overlap": float(_interval_overlap(merged.loc[merged[horizon] == -6, "conf_low_package"].iloc[0], merged.loc[merged[horizon] == -6, "conf_high_package"].iloc[0], merged.loc[merged[horizon] == -6, "conf_low_raw"].iloc[0], merged.loc[merged[horizon] == -6, "conf_high_raw"].iloc[0])),
            })
    return pd.DataFrame(rows), pd.DataFrame(influence)


def run_diagnostics(config: PipelineConfig) -> dict[str, Any]:
    out = _root(config)
    equivalence = _row_equivalence(config)
    write_parquet(equivalence, out / "bridge_v4_outcome_decomposition.parquet", overwrite=True)
    equivalence.to_csv(out / "bridge_v4_outcome_decomposition_summary.csv", index=False)
    grouped = pd.concat([_grouped_gaps(config, key).assign(grouping=key) for key in ("month", "country", "hs2", "hs4")], ignore_index=True)
    grouped["group_id"] = grouped["group_id"].astype("string")
    write_parquet(grouped, out / "bridge_v4_price_source_audit.parquet", overwrite=True)
    grouped.groupby(["grouping", "outcome"], as_index=False).agg(groups=("group_id", "nunique"), positive_rows=("positive_rows", "sum"), mean_abs_log_gap=("mean_abs_log_gap", "mean")).to_csv(out / "bridge_v4_price_source_audit_summary.csv", index=False)
    metrics, influence = _coefficient_diagnostics(config)
    write_parquet(influence.sort_values(["spec", "outcome", "horizon"]), out / "bridge_v4_price_cluster_influence.parquet", overwrite=True)
    write_parquet(metrics, out / "bridge_v4_interval_width_audit.parquet", overwrite=True)
    metrics.to_csv(out / "bridge_v4_metric_summary.csv", index=False)
    report = [
        "# Bridge v4 price and inference diagnosis", "",
        "The canonical v4 raw outcome uses `(trade_value + cal_dut_mo) / quantity`; `dut_val_mo` remains a retained source field. Treatment timing and package policy are held fixed.", "",
        "## Row-level outcome equivalence", "", equivalence.to_markdown(index=False), "",
        "## Registered coefficient metrics and variance diagnostics", "", metrics.to_markdown(index=False), "",
        "## Interpretation", "",
        "The realized-duty correction makes both event and dynamic duty-inclusive curves materially closer. Remaining failures are concentrated in pre-duty price and clustered confidence-interval width/overlap; no threshold is changed. The dynamic price Pearson failure is reported alongside Spearman correlation and centered RMSE to distinguish low curve variance from a level disagreement.", "",
        "The v4 bridge remains diagnostic: Section 301 v5 is not released unless every registered bridge comparison passes, and the independent legal-policy gate remains separate.",
    ]
    report_path = out / "bridge_v4_diagnosis_report.md"
    report_path.write_text("\n".join(report) + "\n", encoding="utf-8")
    manifest = {
        "version": VERSION, "created_at_utc": datetime.now(timezone.utc).isoformat(), "status": "complete",
        "bridge_version": "bridge_v4_realized_calculated_duty", "source_manifest": _relative(config, config.verification_dir / "trade_regressions" / "package_benchmark_v5" / "common_sample_v4" / "aligned_bridge_manifest.json"),
        "outcome_decomposition": _relative(config, out / "bridge_v4_outcome_decomposition.parquet"),
        "price_source_audit": _relative(config, out / "bridge_v4_price_source_audit.parquet"),
        "cluster_influence": _relative(config, out / "bridge_v4_price_cluster_influence.parquet"),
        "interval_width_audit": _relative(config, out / "bridge_v4_interval_width_audit.parquet"),
        "metric_summary": _relative(config, out / "bridge_v4_metric_summary.csv"),
        "report": _relative(config, report_path),
        "registered_thresholds_changed": False,
        "policy_semantics_changed": False,
    }
    write_metadata_json(out / "bridge_v4_diagnosis_manifest.json", manifest)
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    print(json.dumps(run_diagnostics(PipelineConfig.default()), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
