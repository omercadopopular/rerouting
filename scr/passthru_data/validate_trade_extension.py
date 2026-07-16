"""Streaming validation of extension partitions against local Census archives.

This module deliberately does not use package-policy fields.  It validates the
staging-projection extension against the downloaded ZIP detail members and
records source-level quantities, periods, duty fields, and value totals.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import argparse
import json

import pandas as pd
import duckdb

from .build_trade_extension import _archive_path, _repo_relative
from .config import PipelineConfig
from .download_trade import FLOW_SPECS, _iter_fixed_width_chunks
from .io_utils import iter_months, normalize_hs_code, normalize_period, sha256_file, write_metadata_json, write_parquet


VERSION = "extension_archive_validation_v1"


def _partition(config: PipelineConfig, flow: str, period: str) -> Path:
    return config.analysis_dir / "extension_v1" / f"flow={flow}" / f"year={period[:4]}" / f"month={period[5:7]}" / "part.parquet"


def _validate_one(config: PipelineConfig, flow: str, period: str) -> dict[str, Any]:
    archive = _archive_path(config, flow, period)
    partition = _partition(config, flow, period)
    if not archive.exists():
        return {"flow": flow, "period": period, "status": "missing_archive", "archive": _repo_relative(config, archive)}
    if not partition.exists():
        return {"flow": flow, "period": period, "status": "missing_partition", "partition": _repo_relative(config, partition)}
    spec = FLOW_SPECS[flow]
    expected_year, expected_month = int(period[:4]), int(period[5:7])
    source_rows = period_mismatch = hs_missing = country_missing = quantity_missing = quantity_zero = quantity_positive = 0
    trade_total = 0.0
    duty_total = 0.0
    duty_rows = 0
    for chunk in _iter_fixed_width_chunks(archive, spec["detail_member"], spec["detail_colspecs"], spec["detail_names"], chunksize=250_000):
        source_rows += len(chunk)
        years = pd.to_numeric(chunk["year"], errors="coerce")
        months = pd.to_numeric(chunk["month"], errors="coerce")
        period_mismatch += int(((years != expected_year) | (months != expected_month)).fillna(True).sum())
        hs = chunk["hs10"].map(lambda value: normalize_hs_code(value, 10))
        cty = chunk["cty_code"].astype("string").str.strip()
        hs_missing += int(hs.isna().sum())
        country_missing += int((cty.isna() | cty.eq("")).sum())
        trade = pd.to_numeric(chunk["trade_value"], errors="coerce")
        quantity = pd.to_numeric(chunk["quantity"], errors="coerce")
        trade_total += float(trade.sum(min_count=1) or 0.0)
        quantity_missing += int(quantity.isna().sum())
        quantity_zero += int(quantity.eq(0).sum())
        quantity_positive += int(quantity.gt(0).sum())
        for column in ("dut_val_mo", "cal_dut_mo"):
            if column in chunk.columns:
                duty = pd.to_numeric(chunk[column], errors="coerce")
                duty_total += float(duty.sum(min_count=1) or 0.0)
                duty_rows += int(duty.notna().sum())
    con = duckdb.connect(database=":memory:")
    try:
        output_rows, output_total, output_missing, output_zero, output_dups = con.execute(
            """SELECT count(*), sum(trade_value), count(*) FILTER (WHERE quantity_missing),
                      count(*) FILTER (WHERE quantity_zero),
                      count(*) - count(DISTINCT (partner_code, hs10, year, month))
               FROM read_parquet(?)""", [str(partition)]
        ).fetchone()
    finally:
        con.close()
    tolerance = max(1.0, 1e-8 * abs(trade_total))
    return {
        "flow": flow, "period": period, "status": "passed" if period_mismatch == 0 and abs(float(output_total or 0) - trade_total) <= tolerance else "failed",
        "archive": _repo_relative(config, archive), "partition": _repo_relative(config, partition),
        "archive_sha256": sha256_file(archive), "source_member": spec["detail_member"],
        "source_rows": source_rows, "output_rows": int(output_rows),
        "period_mismatch_rows": period_mismatch, "hs_missing_rows": hs_missing, "country_missing_rows": country_missing,
        "source_trade_value": trade_total, "output_trade_value": float(output_total or 0),
        "trade_value_difference": float(output_total or 0) - trade_total, "trade_value_tolerance": tolerance,
        "source_quantity_missing_rows": quantity_missing, "source_quantity_zero_rows": quantity_zero,
        "source_quantity_positive_rows": quantity_positive, "output_quantity_missing_rows": int(output_missing),
        "output_quantity_zero_rows": int(output_zero), "output_duplicate_keys": int(output_dups),
        "source_duty_rows": duty_rows, "source_duty_total": duty_total,
        "value_reconciliation_pass": abs(float(output_total or 0) - trade_total) <= tolerance,
    }


def validate_trade_extension(config: PipelineConfig, *, start_period: str = "2013-01", end_period: str = "2025-12", flows: tuple[str, ...] = ("imports", "exports"), periods: tuple[str, ...] | None = None) -> dict[str, Any]:
    verification = config.verification_dir / "extension_v1" / "archive_validation"
    verification.mkdir(parents=True, exist_ok=True)
    selected_periods = tuple(periods) if periods else tuple(iter_months(start_period, end_period))
    rows = [_validate_one(config, flow, period) for flow in flows for period in selected_periods]
    frame = pd.DataFrame(rows)
    write_parquet(frame, verification / "extension_archive_validation.parquet", overwrite=True)
    frame.groupby(["flow", "status"], dropna=False).size().reset_index(name="months").to_csv(verification / "extension_archive_validation_summary.csv", index=False)
    manifest = {
        "version": VERSION, "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "requested_start_period": normalize_period(start_period), "requested_end_period": normalize_period(end_period), "selected_periods": list(selected_periods),
        "flows": list(flows), "months": len(rows), "passed_months": int((frame["status"] == "passed").sum()),
        "failed_months": int((frame["status"] != "passed").sum()),
        "value_reconciliation_failures": int((~frame["value_reconciliation_pass"].fillna(False)).sum()),
        "output_path": _repo_relative(config, verification / "extension_archive_validation.parquet"),
        "status": "passed" if not frame.empty and bool((frame["status"] == "passed").all()) else "failed",
    }
    write_metadata_json(verification / "extension_archive_validation_manifest.json", manifest)
    (verification / "extension_archive_validation_report.md").write_text(
        "# Archive-level extension validation\n\n"
        f"Validated {len(rows)} flow-months against local ZIP detail members.\n\n"
        f"Status: **{manifest['status']}**. Value reconciliation failures: {manifest['value_reconciliation_failures']}.\n",
        encoding="utf-8",
    )
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", default="2013-01")
    parser.add_argument("--end", default="2025-12")
    parser.add_argument("--flow", choices=("imports", "exports", "all"), default="all")
    parser.add_argument("--period", action="append", dest="periods", help="Validate selected periods instead of the full range; may be repeated.")
    args = parser.parse_args(argv)
    flows = ("imports", "exports") if args.flow == "all" else (args.flow,)
    print(validate_trade_extension(PipelineConfig.default(), start_period=args.start, end_period=args.end, flows=flows, periods=tuple(args.periods) if args.periods else None))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
