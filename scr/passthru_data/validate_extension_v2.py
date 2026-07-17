"""Compare archive-native extension v2 partitions with raw-only staging.

This is a validation comparison, not a replacement for archive parsing.  The
archive-native partitions remain the canonical v2 source; staging is used only
to expose key/value/quantity discrepancies.
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
from .io_utils import iter_months, write_metadata_json, write_parquet


VERSION = "extension_v2_staging_comparison_v1"


def validate_extension_v2(config: PipelineConfig, *, start_period: str = "2013-01", end_period: str = "2025-12", flows: tuple[str, ...] = ("imports", "exports")) -> dict[str, Any]:
    analysis = config.analysis_dir / "extension_v2"
    verification = config.verification_dir / "extension_v2"
    verification.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    con = duckdb.connect(database=":memory:")
    try:
        for flow in flows:
            staging = config.staging_dir / "passthru_data" / f"{flow}_trade_staging.parquet"
            if not staging.exists():
                staging = config.staging_dir / f"{flow}_trade_staging.parquet"
            for period in iter_months(start_period, end_period):
                partition = analysis / f"flow={flow}" / f"year={period[:4]}" / f"month={period[5:7]}" / "part.parquet"
                if not partition.exists() or not staging.exists():
                    rows.append({"flow": flow, "period": period, "status": "missing_input"})
                    continue
                p = str(partition).replace("'", "''")
                s = str(staging).replace("'", "''")
                result = con.execute(
                    f"""
                    WITH v AS (SELECT partner_code, hs10, year, month, trade_value, quantity FROM read_parquet('{p}')),
                         s AS (SELECT partner_code, hs10, year, month, trade_value, quantity FROM read_parquet('{s}') WHERE period='{period}')
                    SELECT (SELECT count(*) FROM v), (SELECT count(*) FROM s),
                           (SELECT count(DISTINCT (partner_code,hs10,year,month)) FROM v),
                           (SELECT count(DISTINCT (partner_code,hs10,year,month)) FROM s),
                           (SELECT coalesce(sum(trade_value),0) FROM v), (SELECT coalesce(sum(trade_value),0) FROM s),
                           (SELECT count(*) FROM (SELECT partner_code,hs10,year,month FROM v EXCEPT SELECT partner_code,hs10,year,month FROM s)),
                           (SELECT count(*) FROM (SELECT partner_code,hs10,year,month FROM s EXCEPT SELECT partner_code,hs10,year,month FROM v))
                    """
                ).fetchone()
                output_rows, staging_rows, output_keys, staging_keys, output_total, staging_total, only_v2, only_staging = result
                tolerance = max(1.0, 1e-8 * abs(float(staging_total)))
                rows.append({"flow": flow, "period": period, "output_rows": int(output_rows), "staging_rows": int(staging_rows), "output_keys": int(output_keys), "staging_keys": int(staging_keys), "output_trade_value": float(output_total), "staging_trade_value": float(staging_total), "trade_value_difference": float(output_total-staging_total), "trade_value_tolerance": tolerance, "only_v2_keys": int(only_v2), "only_staging_keys": int(only_staging), "status": "passed" if abs(float(output_total-staging_total)) <= tolerance and int(only_v2) == 0 and int(only_staging) == 0 else "failed"})
    finally:
        con.close()
    frame = pd.DataFrame(rows)
    write_parquet(frame, verification / "extension_v2_staging_comparison.parquet", overwrite=True)
    frame.groupby(["flow", "status"], dropna=False).size().reset_index(name="months").to_csv(verification / "extension_v2_staging_comparison.csv", index=False)
    manifest = {"version": VERSION, "created_at_utc": datetime.now(timezone.utc).isoformat(), "rows": int(len(frame)), "passed": int((frame["status"] == "passed").sum()) if not frame.empty else 0, "failed": int((frame["status"] != "passed").sum()) if not frame.empty else 0, "status": "passed" if not frame.empty and bool((frame["status"] == "passed").all()) else "pending_or_failed"}
    write_metadata_json(verification / "extension_v2_staging_comparison_manifest.json", manifest)
    return manifest


def main() -> int:
    print(validate_extension_v2(PipelineConfig.default()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
