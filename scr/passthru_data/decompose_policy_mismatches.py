"""Decompose existing Section 301 discrepancies without changing mappings."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import argparse
import json

import duckdb
import pandas as pd

from .config import PipelineConfig
from .io_utils import sha256_file, write_metadata_json, write_parquet


VERSION = "china_301_policy_mismatch_decomposition_v1"


def run(config: PipelineConfig) -> dict[str, object]:
    source = config.verification_dir / "raw_replication_imports" / "raw_replication_discrepancies_china_301_semantics_corrected.parquet"
    if not source.exists():
        raise FileNotFoundError(source)
    out = config.verification_dir / "raw_replication_imports" / "policy_mismatch_decomposition_v1"
    out.mkdir(parents=True, exist_ok=True)
    s = str(source).replace("'", "''")
    con = duckdb.connect(database=":memory:")
    try:
        detail = con.execute(f"""
        WITH base AS (SELECT * FROM read_parquet('{s}'))
        SELECT *,
          CASE
            WHEN ref_treated = 0 AND raw_treated = 1 THEN 'raw_only_treatment'
            WHEN ref_treated = 1 AND raw_treated = 0 THEN 'reference_only_treatment'
            WHEN ref_active = false AND raw_active = true THEN 'raw_only_active_scope'
            WHEN ref_active = true AND raw_active = false THEN 'reference_only_active_scope'
            WHEN discrepancy_type = 'statutory_rate_mismatch' THEN 'statutory_rate_precision_or_definition'
            WHEN discrepancy_type = 'day_weighted_rate_mismatch' THEN 'day_weighted_calendar_or_timing'
            WHEN discrepancy_type = 'non_ad_valorem_or_sentinel' THEN 'non_ad_valorem_or_sentinel'
            WHEN discrepancy_type = 'missing_reference_key' THEN 'missing_reference_key'
            WHEN discrepancy_type IN ('missing_raw_key','missing_raw_policy_scope') THEN 'missing_raw_source'
            WHEN discrepancy_type = 'extra_raw_policy_scope' THEN 'extra_raw_scope'
            ELSE 'other_or_unresolved'
          END AS mismatch_category,
          CASE
            WHEN raw_tw_rule_code_raw IS NULL AND raw_m_policy_source IS NULL THEN 'no_raw_rule_metadata'
            WHEN raw_tw_rule_code_raw IS NULL THEN 'source_without_rule_code'
            ELSE raw_tw_rule_code_raw
          END AS raw_rule_class
        FROM base
        """).fetchdf()
        summary = con.execute(f"""
        WITH base AS (SELECT * FROM read_parquet('{s}')),
        classified AS (
          SELECT CASE
            WHEN ref_treated = 0 AND raw_treated = 1 THEN 'raw_only_treatment'
            WHEN ref_treated = 1 AND raw_treated = 0 THEN 'reference_only_treatment'
            WHEN ref_active = false AND raw_active = true THEN 'raw_only_active_scope'
            WHEN ref_active = true AND raw_active = false THEN 'reference_only_active_scope'
            WHEN discrepancy_type = 'statutory_rate_mismatch' THEN 'statutory_rate_precision_or_definition'
            WHEN discrepancy_type = 'day_weighted_rate_mismatch' THEN 'day_weighted_calendar_or_timing'
            WHEN discrepancy_type = 'non_ad_valorem_or_sentinel' THEN 'non_ad_valorem_or_sentinel'
            WHEN discrepancy_type = 'missing_reference_key' THEN 'missing_reference_key'
            WHEN discrepancy_type IN ('missing_raw_key','missing_raw_policy_scope') THEN 'missing_raw_source'
            WHEN discrepancy_type = 'extra_raw_policy_scope' THEN 'extra_raw_scope'
            ELSE 'other_or_unresolved' END AS mismatch_category,
            year, month, cty_code, hs10, substr(hs10, 1, 2) AS hs2, substr(hs10, 1, 4) AS hs4, ref_treated, raw_treated,
            rate2_abs_diff, raw_m_policy_source AS raw_policy_source
          FROM base
        )
        SELECT mismatch_category, count(*) AS rows, count(DISTINCT (cty_code,hs10,year,month)) AS distinct_keys,
               count(DISTINCT cty_code) AS countries, count(DISTINCT hs2) AS hs2_codes,
               avg(rate2_abs_diff) AS mean_rate2_abs_diff
        FROM classified GROUP BY mismatch_category ORDER BY rows DESC
        """).fetchdf()
        by_month = con.execute(f"""
          SELECT year, month, discrepancy_type, count(*) AS rows
          FROM read_parquet('{s}') GROUP BY year, month, discrepancy_type ORDER BY year, month, discrepancy_type
        """).fetchdf()
        by_rule = con.execute(f"""
          SELECT coalesce(raw_tw_rule_code_raw, 'missing') AS raw_rule_code,
                 coalesce(raw_m_policy_source, 'missing') AS raw_policy_source,
                 discrepancy_type, count(*) AS rows
          FROM read_parquet('{s}') GROUP BY 1,2,3 ORDER BY rows DESC
        """).fetchdf()
    finally:
        con.close()
    write_parquet(detail, out / "policy_mismatch_detail.parquet", overwrite=True)
    write_parquet(by_month, out / "policy_mismatch_by_month.parquet", overwrite=True)
    write_parquet(by_rule, out / "policy_mismatch_by_rule.parquet", overwrite=True)
    summary.to_csv(out / "policy_mismatch_summary.csv", index=False)
    report = [
        "# Section 301 policy mismatch decomposition", "",
        "This is a diagnostic decomposition of the existing semantics-corrected comparison. It does not alter legal mappings, fill unresolved rates, or change any release flag.", "",
        summary.to_markdown(index=False), "",
        "Treatment mismatches are separated from active-scope mismatches and rate/calendar mismatches. Raw rule codes and source labels are retained for review; `missing` is not interpreted as zero.",
    ]
    report_path = out / "policy_mismatch_decomposition_report.md"
    report_path.write_text("\n".join(report) + "\n", encoding="utf-8")
    manifest = {
        "version": VERSION, "created_at_utc": datetime.now(timezone.utc).isoformat(), "status": "diagnostic_complete",
        "source_path": str(source.relative_to(config.repo_root)).replace('\\', '/'), "source_sha256": sha256_file(source),
        "detail_path": str((out / 'policy_mismatch_detail.parquet').relative_to(config.repo_root)).replace('\\', '/'),
        "summary_path": str((out / 'policy_mismatch_summary.csv').relative_to(config.repo_root)).replace('\\', '/'),
        "report_path": str(report_path.relative_to(config.repo_root)).replace('\\', '/'),
        "legal_mapping_changed": False, "independent_policy_gate": "failed", "section301_v5_ready": False,
    }
    write_metadata_json(out / "policy_mismatch_decomposition_manifest.json", manifest)
    return manifest


def main() -> int:
    argparse.ArgumentParser(description=__doc__).parse_args()
    print(json.dumps(run(PipelineConfig.default()), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
