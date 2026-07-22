"""Projected forensic diagnostics for the aligned package/raw outcome bridge.

The bridge is deliberately diagnosed on one import-only universe.  The
key-level differences are written as compressed Parquet; all grouped reports
are compact summaries.  No policy definition is changed by this module.
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
from .io_utils import sha256_file, write_metadata_json, write_parquet


VERSION = "bridge_forensics_v1"
OUTCOMES = ("val", "q1", "p", "pduty")


def forensics_root(config: PipelineConfig) -> Path:
    path = config.verification_dir / "trade_regressions" / "package_benchmark_v5" / "common_sample_v3" / "bridge_forensics"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _relative(config: PipelineConfig, path: Path) -> str:
    try:
        return path.resolve().relative_to(config.repo_root.resolve()).as_posix()
    except ValueError:
        return path.name


def _paths(config: PipelineConfig) -> tuple[Path, Path]:
    base = config.verification_dir / "trade_regressions" / "package_benchmark_v5" / "common_sample_v3"
    package = base / "package_common_sample_anchor.parquet"
    raw = base / "raw_outcomes_package_policy.parquet"
    if not package.exists() or not raw.exists():
        # v2 remains a historical fallback for users inspecting the old
        # diagnostic namespace; new runs always prefer source-separated v3.
        base = config.verification_dir / "trade_regressions" / "package_benchmark_v5" / "common_sample_v2"
        package = base / "package_common_sample_aligned.parquet"
        raw = base / "raw_outcomes_package_policy_aligned.parquet"
    if not package.exists() or not raw.exists():
        raise FileNotFoundError(f"Missing aligned bridge panels: {package}, {raw}")
    return package, raw


def _escaped(path: Path) -> str:
    return str(path).replace("'", "''")


def _copy_parquet(con: duckdb.DuckDBPyConnection, query: str, destination: Path) -> int:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.tmp")
    temporary.unlink(missing_ok=True)
    con.execute(f"COPY ({query}) TO '{_escaped(temporary)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    count = int(con.execute(f"SELECT count(*) FROM read_parquet('{_escaped(temporary)}')").fetchone()[0])
    temporary.replace(destination)
    return count


def run_forensics(config: PipelineConfig) -> dict[str, Any]:
    package, raw = _paths(config)
    out = forensics_root(config)
    p, r = _escaped(package), _escaped(raw)
    con = duckdb.connect(database=":memory:")
    summary_rows: list[dict[str, Any]] = []
    grouped_rows: list[pd.DataFrame] = []
    lm_equivalence_rows: list[dict[str, Any]] = []
    try:
        # The Stata dynamic program first-differences lm_* variables.  The
        # Python estimator uses log(m_* * 1e6); the scale is a constant and
        # must disappear after first differencing.  Prove that equivalence on
        # the actual package panel before interpreting dynamic bridge output.
        for outcome in OUTCOMES:
            mcol, lmcol = f"m_{outcome}", f"lm_{outcome}"
            row = con.execute(f"""
                WITH x AS (
                    SELECT id, mdate, {mcol} AS m_value, {lmcol} AS lm_value,
                           lag({mcol}) OVER (PARTITION BY id ORDER BY mdate) AS lag_m_value,
                           lag({lmcol}) OVER (PARTITION BY id ORDER BY mdate) AS lag_lm_value
                    FROM read_parquet('{p}')
                ), d AS (
                    SELECT (lm_value - lag_lm_value) - (ln(m_value * 1000000.0) - ln(lag_m_value * 1000000.0)) AS difference
                    FROM x WHERE m_value > 0 AND lag_m_value > 0 AND lm_value IS NOT NULL AND lag_lm_value IS NOT NULL
                )
                SELECT count(*), max(abs(difference)), avg(abs(difference)) FROM d
            """).fetchone()
            lm_equivalence_rows.append({"outcome": outcome, "rows": int(row[0] or 0), "max_abs_first_difference_gap": float(row[1] or 0.0), "mean_abs_first_difference_gap": float(row[2] or 0.0), "status": "passed" if float(row[1] or 0.0) <= 1e-5 else "failed"})
        write_parquet(pd.DataFrame(lm_equivalence_rows), out / "package_lm_equivalence.parquet", overwrite=True)
        for outcome in OUTCOMES:
            col = f"m_{outcome}"
            # This is a row-level diagnostic and therefore cannot be CSV.
            delta_query = f"""
                WITH p AS (
                    SELECT cty_code, hs10, year, month, id, naics_str,
                           {col} AS package_value,
                           max(m_status2) OVER (PARTITION BY id) AS treatment
                    FROM read_parquet('{p}')
                ), r AS (
                    SELECT cty_code, hs10, year, month, {col} AS raw_value
                    FROM read_parquet('{r}')
                )
                SELECT p.cty_code, p.hs10, p.year, p.month, p.id, p.naics_str,
                       p.treatment,
                       p.package_value, r.raw_value,
                       100.0 * (ln(r.raw_value) - ln(p.package_value)) AS log_difference
                FROM p INNER JOIN r USING (cty_code, hs10, year, month)
                WHERE p.package_value > 0 AND r.raw_value > 0
            """
            key_path = out / "aligned_outcome_difference.parquet"
            if outcome == OUTCOMES[0]:
                # Build the complete file once with a stable outcome column.
                union = " UNION ALL ".join(
                    f"SELECT '{name}' AS outcome, q.* FROM ({delta_query.replace(col, f'm_{name}')}) q"
                    for name in OUTCOMES
                )
                total = _copy_parquet(con, union, key_path)
            stats = con.execute(f"""
                SELECT count(*) AS n,
                       avg(abs(log_difference)) AS mean_abs,
                       median(abs(log_difference)) AS median_abs,
                       quantile_cont(abs(log_difference), 0.95) AS p95_abs,
                       quantile_cont(abs(log_difference), 0.99) AS p99_abs,
                       max(abs(log_difference)) AS max_abs,
                       avg(log_difference) FILTER (WHERE treatment = 2) AS treated_mean,
                       avg(log_difference) FILTER (WHERE coalesce(treatment, 0) <> 2) AS untreated_mean
                FROM read_parquet('{_escaped(key_path)}') WHERE outcome = '{outcome}'
            """).fetchone()
            summary_rows.append({
                "outcome": outcome,
                "rows": int(stats[0] or 0),
                "mean_abs_log_difference_x100": float(stats[1]) if stats[1] is not None else None,
                "median_abs_log_difference_x100": float(stats[2]) if stats[2] is not None else None,
                "p95_abs_log_difference_x100": float(stats[3]) if stats[3] is not None else None,
                "p99_abs_log_difference_x100": float(stats[4]) if stats[4] is not None else None,
                "max_abs_log_difference_x100": float(stats[5]) if stats[5] is not None else None,
                "treated_mean_difference_x100": float(stats[6]) if stats[6] is not None else None,
                "untreated_mean_difference_x100": float(stats[7]) if stats[7] is not None else None,
            })
            grouped_rows.append(pd.DataFrame(con.execute(f"""
                SELECT '{outcome}' AS outcome, year, month,
                       count(*) AS rows, avg(log_difference) AS mean_log_difference,
                       avg(abs(log_difference)) AS mean_abs_log_difference,
                       quantile_cont(abs(log_difference), 0.95) AS p95_abs_log_difference
                FROM read_parquet('{_escaped(key_path)}') WHERE outcome = '{outcome}'
                GROUP BY year, month
            """).fetchdf()))
        summary = pd.DataFrame(summary_rows)
        write_parquet(summary, out / "aligned_outcome_difference_summary.parquet", overwrite=True)
        summary.to_csv(out / "aligned_outcome_difference_summary.csv", index=False)
        grouped = pd.concat(grouped_rows, ignore_index=True)
        write_parquet(grouped, out / "difference_by_month.parquet", overwrite=True)
        for dimension, expression in {
            "treatment": "treatment",
            "country": "cty_code",
            "hs2": "left(hs10, 2)",
            "hs4": "left(hs10, 4)",
        }.items():
            frame = con.execute(f"""
                SELECT outcome, cast({expression} AS varchar) AS group_value,
                       count(*) AS rows, avg(log_difference) AS mean_log_difference,
                       avg(abs(log_difference)) AS mean_abs_log_difference,
                       quantile_cont(abs(log_difference), 0.99) AS p99_abs_log_difference
                FROM read_parquet('{_escaped(key_path)}')
                GROUP BY outcome, cast({expression} AS varchar)
            """).fetchdf()
            write_parquet(frame, out / f"difference_by_{dimension}.parquet", overwrite=True)
        tail = con.execute(f"""
            SELECT outcome,
                   count(*) FILTER (WHERE abs(log_difference) >= 1) AS abs_ge_1,
                   count(*) FILTER (WHERE abs(log_difference) >= 5) AS abs_ge_5,
                   count(*) FILTER (WHERE abs(log_difference) >= 10) AS abs_ge_10,
                   count(*) FILTER (WHERE abs(log_difference) >= 25) AS abs_ge_25,
                   count(*) AS rows
            FROM read_parquet('{_escaped(key_path)}') GROUP BY outcome
        """).fetchdf()
        write_parquet(tail, out / "difference_tail_audit.parquet", overwrite=True)
        tail.to_csv(out / "difference_influence_summary.csv", index=False)
        # The coefficient-difference table is explicitly a comparison record;
        # it is not mislabeled as a separately estimated regression.
        coefficient_rows: list[pd.DataFrame] = []
        bridge = config.verification_dir / "trade_regressions" / "package_benchmark_v5" / "common_sample_v3" / "bridge_resumable"
        if not bridge.exists():
            bridge = config.verification_dir / "trade_regressions" / "package_benchmark_v5" / "common_sample_v2" / "bridge_resumable"
        for mode in ("package_common_sample_anchor", "raw_outcomes_package_policy"):
            for spec in ("event", "dynamic"):
                for outcome in OUTCOMES:
                    coefficient = pd.read_parquet(bridge / mode / spec / outcome / "coefficients.parquet")
                    coefficient["source_mode"] = mode
                    coefficient["spec"] = spec
                    coefficient["outcome"] = outcome
                    coefficient["record_type"] = "checkpoint_coefficient_comparison_not_delta_regression"
                    coefficient_rows.append(coefficient)
        coefficients = pd.concat(coefficient_rows, ignore_index=True)
        write_parquet(coefficients, out / "difference_regression_coefficients.parquet", overwrite=True)
    finally:
        con.close()
    manifest = {
        "version": VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "package_source": _relative(config, package),
        "raw_source": _relative(config, raw),
        "package_source_sha256": sha256_file(package),
        "raw_source_sha256": sha256_file(raw),
        "key_difference_path": _relative(config, out / "aligned_outcome_difference.parquet"),
        "summary_path": _relative(config, out / "aligned_outcome_difference_summary.parquet"),
        "summary_rows": summary_rows,
        "package_lm_equivalence": lm_equivalence_rows,
        "status": "diagnostic",
        "registered_bridge_gate_changed": False,
    }
    write_metadata_json(out / "bridge_forensics_manifest.json", manifest)
    (out / "bridge_forensics_report.md").write_text(
        "# Aligned bridge forensics\n\n"
        "This report diagnoses package/raw outcome differences on identical import keys. "
        "It does not alter the registered bridge thresholds or policy mappings.\n\n"
        + "\n".join(f"- **{row['outcome']}**: {row['rows']} positive paired rows; "
                     f"mean absolute log difference ×100 = {row['mean_abs_log_difference_x100']:.6f}; "
                     f"p99 = {row['p99_abs_log_difference_x100']:.6f}." for row in summary_rows)
        + "\n",
        encoding="utf-8",
    )
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args(argv)
    print(run_forensics(PipelineConfig.default()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
