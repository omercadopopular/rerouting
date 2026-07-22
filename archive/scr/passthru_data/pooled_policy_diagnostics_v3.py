"""Projection-based diagnostics for the v3 policy clock and tariff path.

This module deliberately produces aggregate Parquet/CSV evidence only.  It does
not rewrite policy mappings or export row-level traces.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

from .config import PipelineConfig
from .io_utils import write_metadata_json, write_parquet
from .pooled_policy_replication_v3 import analysis_root, relative, verification_root


def _sql(path: Path) -> str:
    return str(path).replace("'", "''")


def run_diagnostics(config: PipelineConfig) -> dict:
    panel = analysis_root(config) / "pooled_policy_replication_v3_panel.parquet"
    package = config.verification_dir / "trade_regressions" / "package_benchmark_v5" / "cache" / "package_full_panel_hs10fixed.parquet"
    out = verification_root(config) / "diagnostics"
    out.mkdir(parents=True, exist_ok=True)
    if not panel.exists() or not package.exists():
        raise FileNotFoundError(f"required panel/cache missing: {panel}, {package}")
    con = duckdb.connect()
    try:
        p, k = _sql(panel), _sql(package)
        join = f"read_parquet('{p}') v JOIN read_parquet('{k}') k USING(cty_code, hs10, year, month)"
        path_detail = con.execute(f"""
          SELECT v.cty_code, v.year, v.month,
                 count(*) AS rows,
                 sum(CASE WHEN v.bilateral_dayweighted_additional_rate > 0 THEN 1 ELSE 0 END) AS v3_positive_rows,
                 sum(CASE WHEN k.m_stattariff2 > 0 THEN 1 ELSE 0 END) AS package_positive_rows,
                 avg(abs(v.paper_dynamic_total_tariff - k.m_stattariff2)) AS paper_abs_tariff_diff,
                 avg(abs(v.legal_dynamic_total_tariff - k.m_stattariff2)) AS legal_abs_tariff_diff,
                 corr(v.paper_dynamic_total_tariff, k.m_stattariff2) AS paper_tariff_corr,
                 corr(v.legal_dynamic_total_tariff, k.m_stattariff2) AS legal_tariff_corr
          FROM {join}
          GROUP BY 1,2,3 ORDER BY 1,2,3
        """).fetchdf()
        event_detail = con.execute(f"""
          SELECT v.cty_code, v.year, v.month, count(*) AS rows,
                 sum(CASE WHEN v.paper_event_status = k.m_status2 THEN 1 ELSE 0 END) AS paper_status_matches,
                 sum(CASE WHEN v.legal_event_status = k.m_status2 THEN 1 ELSE 0 END) AS legal_status_matches,
                 sum(CASE WHEN v.paper_event_mdate = k.m_effective_mdate2 THEN 1 ELSE 0 END) AS paper_date_matches,
                 sum(CASE WHEN v.legal_event_month = k.m_effective_mdate2 THEN 1 ELSE 0 END) AS legal_date_matches
          FROM {join}
          GROUP BY 1,2,3 ORDER BY 1,2,3
        """).fetchdf()
        exemption = con.execute(f"""
          SELECT cty_code,
                 count(DISTINCT hs10) AS hs10_products,
                 count(DISTINCT CASE WHEN bilateral_dayweighted_additional_rate > 0 THEN hs10 END) AS targeted_hs10_products,
                 count(DISTINCT CASE WHEN paper_event_status = 1 THEN hs10 END) AS product_clock_hs10,
                 count(DISTINCT CASE WHEN paper_event_status = 2 THEN hs10 END) AS partner_clock_hs10,
                 sum(CASE WHEN bilateral_dayweighted_additional_rate > 0 THEN 1 ELSE 0 END) AS targeted_rows,
                 sum(CASE WHEN bilateral_dayweighted_additional_rate = 0 THEN 1 ELSE 0 END) AS exempt_or_untargeted_rows
          FROM read_parquet('{p}')
          GROUP BY 1 ORDER BY 1
        """).fetchdf()
    finally:
        con.close()
    write_parquet(path_detail, out / "pooled_policy_v3_tariff_path_diagnostics.parquet", overwrite=True)
    write_parquet(event_detail, out / "pooled_policy_v3_event_clock_diagnostics.parquet", overwrite=True)
    write_parquet(exemption, out / "pooled_policy_v3_exemption_audit.parquet", overwrite=True)
    summary = pd.DataFrame([
        {"diagnostic": "tariff_path", "rows": int(path_detail.rows.sum()), "paper_abs_mean": float(path_detail.paper_abs_tariff_diff.mean()), "legal_abs_mean": float(path_detail.legal_abs_tariff_diff.mean()), "paper_corr_mean": float(path_detail.paper_tariff_corr.mean()), "legal_corr_mean": float(path_detail.legal_tariff_corr.mean())},
        {"diagnostic": "event_clock", "rows": int(event_detail.rows.sum()), "paper_status_match": float(event_detail.paper_status_matches.sum() / event_detail.rows.sum()), "legal_status_match": float(event_detail.legal_status_matches.sum() / event_detail.rows.sum()), "paper_date_match": float(event_detail.paper_date_matches.sum() / event_detail.rows.sum()), "legal_date_match": float(event_detail.legal_date_matches.sum() / event_detail.rows.sum())},
        {"diagnostic": "exemption", "rows": int(exemption.targeted_rows.sum() + exemption.exempt_or_untargeted_rows.sum()), "targeted_hs10": int(exemption.targeted_hs10_products.sum()), "product_clock_hs10": int(exemption.product_clock_hs10.sum()), "partner_clock_hs10": int(exemption.partner_clock_hs10.sum())},
    ])
    summary.to_csv(out / "pooled_policy_v3_diagnostic_summary.csv", index=False)
    manifest = {
        "version": "pooled_policy_replication_v3_diagnostics",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "panel": relative(config, panel), "package_cache": relative(config, package),
        "outputs": [relative(config, out / f) for f in ("pooled_policy_v3_tariff_path_diagnostics.parquet", "pooled_policy_v3_event_clock_diagnostics.parquet", "pooled_policy_v3_exemption_audit.parquet", "pooled_policy_v3_diagnostic_summary.csv")],
        "interpretation": "Paper event dates and legal first-applicability dates are separate objects; dynamic comparisons use bilateral tariff paths.",
    }
    write_metadata_json(out / "pooled_policy_v3_diagnostics_manifest.json", manifest)
    (out / "pooled_policy_v3_diagnostics_report.md").write_text(
        "# Pooled policy v3 diagnostics\n\n"
        "The tariff-path table compares partner-specific v3 rates with the package rate on identical keys. "
        "The event-clock table compares paper-compatible and legal first-applicability dates separately. "
        "The exemption audit is aggregate and preserves native partner/product scope.\n",
        encoding="utf-8",
    )
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", action="store_true")
    args = parser.parse_args()
    if args.run:
        print(json.dumps(run_diagnostics(PipelineConfig.default()), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
