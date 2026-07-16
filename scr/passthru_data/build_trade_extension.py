"""Local-only monthly raw Census trade extension through the available period.

This module deliberately parses the already-downloaded Census archives and never
attaches package tariff treatments.  It writes one ZSTD Parquet partition per
flow/year/month so an interrupted build can resume without loading all months.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import argparse
import hashlib
import json
import re

import pandas as pd

from .config import PipelineConfig
from .download_trade import FLOW_SPECS, _iter_fixed_width_chunks, _load_country_lookup, _resolve_member_name
from .io_utils import add_hierarchy_codes, iter_months, normalize_hs_code, normalize_period, sha256_file, write_metadata_json, write_parquet


VERSION = "extension_v1"
FLOW_PREFIX = {"imports": "IMDB", "exports": "EXDB"}


def _repo_relative(config: PipelineConfig, path: Path) -> str:
    try:
        return path.resolve().relative_to(config.repo_root.resolve()).as_posix()
    except ValueError:
        # Existing staging metadata may contain an old machine-local absolute
        # path. Canonical manifests must remain portable; recover the known
        # repository-relative trade path from the archive basename.
        name = path.name
        for flow in ("imports", "exports"):
            if name.upper().startswith(FLOW_PREFIX[flow]):
                return (config.raw_dir / "trade" / flow / name).resolve().relative_to(config.repo_root.resolve()).as_posix()
        return path.name


def _archive_path(config: PipelineConfig, flow: str, period: str) -> Path:
    return config.raw_dir / "trade" / flow / f"{FLOW_PREFIX[flow]}{period[2:4]}{period[5:7]}.ZIP"


def _source_member(flow: str) -> str:
    return FLOW_SPECS[flow]["detail_member"]


def _parse_archive(config: PipelineConfig, flow: str, period: str, archive: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    spec = FLOW_SPECS[flow]
    expected_year, expected_month = int(period[:4]), int(period[5:7])
    chunks: list[pd.DataFrame] = []
    source_rows = 0
    discarded_rows = 0
    for chunk in _iter_fixed_width_chunks(archive, spec["detail_member"], spec["detail_colspecs"], spec["detail_names"], chunksize=250_000):
        source_rows += len(chunk)
        chunk["hs10"] = chunk["hs10"].map(lambda value: normalize_hs_code(value, 10))
        chunk["cty_code"] = chunk["cty_code"].astype(str).str.strip().str.zfill(4)
        chunk["year"] = pd.to_numeric(chunk["year"], errors="coerce")
        chunk["month"] = pd.to_numeric(chunk["month"], errors="coerce")
        chunk["trade_value"] = pd.to_numeric(chunk["trade_value"], errors="coerce")
        chunk["quantity"] = pd.to_numeric(chunk["quantity"], errors="coerce")
        for column in ("dut_val_mo", "cal_dut_mo"):
            if column in chunk.columns:
                chunk[column] = pd.to_numeric(chunk[column], errors="coerce")
        before = len(chunk)
        chunk = chunk.loc[(chunk["year"] == expected_year) & (chunk["month"] == expected_month)].copy()
        discarded_rows += before - len(chunk)
        chunk = chunk.dropna(subset=["hs10", "cty_code", "year", "month"])
        if chunk.empty:
            continue
        chunk["quantity_missing"] = chunk["quantity"].isna()
        chunk["quantity_zero"] = chunk["quantity"].eq(0)
        chunks.append(chunk)
    if not chunks:
        raise RuntimeError(f"Archive {archive} produced no rows for expected period {period}")
    raw = pd.concat(chunks, ignore_index=True)
    aggregations: dict[str, Any] = {
        "trade_value": lambda values: values.sum(min_count=1),
        "quantity": lambda values: values.sum(min_count=1),
        "quantity_missing": "max",
        "quantity_zero": "max",
    }
    for column in ("dut_val_mo", "cal_dut_mo"):
        if column in raw.columns:
            aggregations[column] = lambda values: values.sum(min_count=1)
    grouped = raw.groupby(["cty_code", "hs10", "year", "month"], as_index=False).agg(aggregations)
    grouped["period"] = period
    grouped["flow"] = flow
    grouped["hs10"] = grouped["hs10"].map(lambda value: normalize_hs_code(value, 10))
    grouped = add_hierarchy_codes(grouped, "hs10")
    country = _load_country_lookup(archive, flow).rename(columns={"cty_name": "partner_name"})
    country["cty_code"] = country["cty_code"].astype(str).str.strip().str.zfill(4)
    grouped = grouped.merge(country[["cty_code", "partner_name"]], on="cty_code", how="left")
    grouped = grouped.rename(columns={"cty_code": "partner_code"})
    grouped["source_archive"] = _repo_relative(config, archive)
    grouped["source_member"] = spec["detail_member"]
    grouped["source_sha256"] = sha256_file(archive)
    grouped["parser_version"] = VERSION
    grouped["unit_value"] = grouped["trade_value"] / grouped["quantity"].where(grouped["quantity"] > 0)
    columns = [
        "flow", "partner_code", "partner_name", "hs10", "hs8", "hs6", "hs4", "hs2",
        "year", "month", "period", "trade_value", "quantity", "quantity_missing", "quantity_zero",
        "unit_value", "source_archive", "source_member", "source_sha256", "parser_version",
    ]
    for column in ("dut_val_mo", "cal_dut_mo"):
        if column in grouped.columns:
            columns.insert(columns.index("unit_value"), column)
    grouped = grouped[columns].sort_values(["partner_code", "hs10"]).reset_index(drop=True)
    audit = {
        "flow": flow,
        "period": period,
        "archive": _repo_relative(config, archive),
        "source_member": spec["detail_member"],
        "source_sha256": grouped["source_sha256"].iloc[0],
        "source_rows": int(source_rows),
        "discarded_period_rows": int(discarded_rows),
        "parsed_rows": int(len(raw)),
        "output_rows": int(len(grouped)),
        "duplicate_keys_before_aggregation": int(raw.duplicated(["cty_code", "hs10", "year", "month"]).sum()),
        "duplicate_keys_after_aggregation": int(grouped.duplicated(["partner_code", "hs10", "year", "month"]).sum()),
        "source_trade_value": float(raw["trade_value"].sum(min_count=1)),
        "output_trade_value": float(grouped["trade_value"].sum(min_count=1)),
        "quantity_missing_rows": int(raw["quantity"].isna().sum()),
        "quantity_zero_rows": int(raw["quantity"].eq(0).sum()),
    }
    audit["trade_value_difference"] = audit["output_trade_value"] - audit["source_trade_value"]
    audit["trade_value_tolerance"] = max(1.0, 1e-8 * abs(audit["source_trade_value"]))
    audit["reconciliation_pass"] = abs(audit["trade_value_difference"]) <= audit["trade_value_tolerance"]
    return grouped, audit


def build_trade_extension(config: PipelineConfig, *, start_period: str = "2013-01", end_period: str = "2025-12", flows: tuple[str, ...] = ("imports", "exports"), overwrite: bool = False) -> dict[str, Any]:
    out_root = config.analysis_dir / VERSION
    verification_root = config.verification_dir / VERSION
    out_root.mkdir(parents=True, exist_ok=True)
    verification_root.mkdir(parents=True, exist_ok=True)
    audits: list[dict[str, Any]] = []
    missing: list[dict[str, str]] = []
    for flow in flows:
        for period in iter_months(start_period, end_period):
            archive = _archive_path(config, flow, period)
            if not archive.exists():
                missing.append({"flow": flow, "period": period, "expected_archive": _repo_relative(config, archive)})
                continue
            partition = out_root / f"flow={flow}" / f"year={period[:4]}" / f"month={period[5:7]}" / "part.parquet"
            if partition.exists() and not overwrite:
                audit_path = verification_root / "audits" / f"{flow}_{period}.json"
                if audit_path.exists():
                    audits.append(json.loads(audit_path.read_text(encoding="utf-8")))
                continue
            frame, audit = _parse_archive(config, flow, period, archive)
            write_parquet(frame, partition, overwrite=True)
            audit["partition"] = _repo_relative(config, partition)
            audit_path = verification_root / "audits" / f"{flow}_{period}.json"
            write_metadata_json(audit_path, audit)
            audits.append(audit)
    audit_frame = pd.DataFrame(audits)
    write_parquet(audit_frame, verification_root / "extension_monthly_reconciliation.parquet", overwrite=True)
    audit_frame.to_csv(verification_root / "extension_monthly_reconciliation.csv", index=False)
    write_metadata_json(verification_root / "extension_missing_sources.json", {"version": VERSION, "missing": missing, "status": "complete" if not missing else "blocked_missing_data"})
    manifest = {
        "version": VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "flows": list(flows),
        "requested_start_period": normalize_period(start_period),
        "requested_end_period": normalize_period(end_period),
        "partitions": int(len(audits)),
        "missing_partitions": int(len(missing)),
        "reconciliation_failures": int((~audit_frame["reconciliation_pass"].astype(bool)).sum()) if not audit_frame.empty else None,
        "policy_columns_present": False,
        "status": "complete" if not missing and not audit_frame.empty and bool(audit_frame["reconciliation_pass"].all()) else "diagnostic_or_incomplete",
    }
    write_metadata_json(verification_root / "extension_build_manifest.json", manifest)
    return manifest


def build_trade_extension_from_raw_staging(config: PipelineConfig, *, start_period: str = "2013-01", end_period: str = "2025-12", flows: tuple[str, ...] = ("imports", "exports"), overwrite: bool = False) -> dict[str, Any]:
    """Materialize the already parsed raw-only staging panels by projection.

    The staging panels are explicitly marked ``build_mode=raw_only`` and contain
    no policy variables.  This path avoids re-reading hundreds of compressed
    archives while retaining the archive hashes from the staging metadata.
    Import duty detail is not present in that staging schema and is therefore
    represented as null, never as zero.
    """
    import duckdb

    out_root = config.analysis_dir / VERSION
    verification_root = config.verification_dir / VERSION
    out_root.mkdir(parents=True, exist_ok=True)
    verification_root.mkdir(parents=True, exist_ok=True)
    audits: list[dict[str, Any]] = []
    missing: list[dict[str, str]] = []
    con = duckdb.connect(database=":memory:")
    try:
        for flow in flows:
            staging = config.staging_dir / "passthru_data" / f"{flow}_trade_staging.parquet"
            if not staging.exists():
                staging = config.staging_dir / f"{flow}_trade_staging.parquet"
            metadata_path = staging.with_suffix(".metadata.json")
            if not staging.exists() or not metadata_path.exists():
                missing.append({"flow": flow, "period": "all", "expected_staging": _repo_relative(config, staging)})
                continue
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            source_by_period = {str(item["period"]): {"source_archive": _repo_relative(config, Path(item["path"])), "source_sha256": item.get("sha256")} for item in metadata.get("source_files", [])}
            for period in iter_months(start_period, end_period):
                if period not in source_by_period:
                    missing.append({"flow": flow, "period": period, "expected_archive": _repo_relative(config, _archive_path(config, flow, period))})
                    continue
                source = source_by_period[period]
                partition = out_root / f"flow={flow}" / f"year={period[:4]}" / f"month={period[5:7]}" / "part.parquet"
                if partition.exists() and not overwrite:
                    continue
                partition.parent.mkdir(parents=True, exist_ok=True)
                escaped_staging = str(staging).replace("'", "''")
                escaped_archive = source["source_archive"].replace("'", "''")
                source_hash = source.get("source_sha256") or "missing"
                query = f"""
                    SELECT flow, partner_code, partner_name, hs10, hs8, hs6, hs4, hs2,
                           year, month, period, mdate, trade_value, quantity,
                           quantity IS NULL AS quantity_missing,
                           quantity = 0 AS quantity_zero,
                           trade_value / NULLIF(quantity, 0) AS unit_value,
                           CAST(NULL AS DOUBLE) AS dut_val_mo,
                           CAST(NULL AS DOUBLE) AS cal_dut_mo,
                           '{escaped_archive}' AS source_archive,
                           '{source_hash}' AS source_sha256,
                           'extension_v1_raw_only_staging' AS parser_version
                    FROM read_parquet('{escaped_staging}')
                    WHERE period = '{period}'
                """
                con.execute(f"COPY ({query}) TO '{str(partition).replace("'", "''")}' (FORMAT PARQUET, COMPRESSION ZSTD)")
                audit = con.execute(f"""
                    SELECT '{flow}' AS flow, '{period}' AS period,
                           count(*) AS output_rows,
                           count(*) FILTER (WHERE quantity IS NULL) AS quantity_missing_rows,
                           count(*) FILTER (WHERE quantity = 0) AS quantity_zero_rows,
                           count(*) - count(DISTINCT (partner_code, hs10, year, month)) AS duplicate_keys_after_aggregation,
                           sum(trade_value) AS output_trade_value
                    FROM read_parquet('{str(partition).replace("'", "''")}')
                """).fetchone()
                source_total = con.execute(f"SELECT sum(trade_value), count(*) FROM read_parquet('{escaped_staging}') WHERE period = '{period}'").fetchone()
                record = {"flow": flow, "period": period, "partition": _repo_relative(config, partition), "output_rows": int(audit[2]), "quantity_missing_rows": int(audit[3]), "quantity_zero_rows": int(audit[4]), "duplicate_keys_after_aggregation": int(audit[5]), "output_trade_value": float(audit[6] or 0), "source_trade_value": float(source_total[0] or 0), "source_rows": int(source_total[1]), "source_archive": source["source_archive"], "source_sha256": source_hash, "duty_fields_available": False}
                record["trade_value_difference"] = record["output_trade_value"] - record["source_trade_value"]
                record["trade_value_tolerance"] = max(1.0, 1e-8 * abs(record["source_trade_value"]))
                record["reconciliation_pass"] = abs(record["trade_value_difference"]) <= record["trade_value_tolerance"]
                audits.append(record)
    finally:
        con.close()
    audit_frame = pd.DataFrame(audits)
    write_parquet(audit_frame, verification_root / "extension_monthly_reconciliation.parquet", overwrite=True)
    audit_frame.to_csv(verification_root / "extension_monthly_reconciliation.csv", index=False)
    # Release-grade audit companions. These remain machine-readable Parquet;
    # only the compact monthly reconciliation is CSV.
    partition_rows = []
    for record in audits:
        partition_path = config.repo_root / record["partition"]
        partition_rows.append({
            "flow": record["flow"], "period": record["period"],
            "partition": record["partition"],
            "partition_sha256": sha256_file(partition_path) if partition_path.exists() else None,
            "rows": record["output_rows"], "source_archive": record["source_archive"],
            "source_sha256": record["source_sha256"],
            "parser_version": VERSION + "_raw_only_staging",
        })
    write_parquet(pd.DataFrame(partition_rows), verification_root / "extension_partition_manifest.parquet", overwrite=True)
    write_parquet(audit_frame[["flow", "period", "partition", "duplicate_keys_after_aggregation"]], verification_root / "extension_duplicate_audit.parquet", overwrite=True)
    write_parquet(audit_frame[["flow", "period", "partition", "quantity_missing_rows", "quantity_zero_rows"]], verification_root / "extension_quantity_audit.parquet", overwrite=True)
    concordance = audit_frame[["flow", "period", "partition"]].copy()
    concordance["concordance_status"] = "not_applied_raw_hs10"
    concordance["one_to_many_count"] = 0
    concordance["many_to_one_count"] = 0
    concordance["unmatched_count"] = 0
    write_parquet(concordance, verification_root / "extension_concordance_audit.parquet", overwrite=True)
    inventory = {"version": VERSION, "created_at_utc": datetime.now(timezone.utc).isoformat(), "requested_start_period": normalize_period(start_period), "requested_end_period": normalize_period(end_period), "flows": {}}
    for flow in flows:
        staging = config.staging_dir / "passthru_data" / f"{flow}_trade_staging.parquet"
        if not staging.exists():
            staging = config.staging_dir / f"{flow}_trade_staging.parquet"
        metadata_path = staging.with_suffix(".metadata.json")
        metadata = json.loads(metadata_path.read_text(encoding="utf-8")) if metadata_path.exists() else {}
        files = metadata.get("source_files", [])
        archives = [item for item in files if str(item.get("path", "")).upper().endswith(".ZIP")]
        auxiliary = config.staging_dir / "passthru_data" / f"{flow}_concord.parquet"
        inventory["flows"][flow] = {"archive_count": len(archives), "auxiliary_concordance_count": 1 if auxiliary.exists() else max(0, len(files) - len(archives)), "auxiliary_concordance": _repo_relative(config, auxiliary) if auxiliary.exists() else None, "periods": sorted(str(item.get("period")) for item in archives), "source_files": [{"period": item.get("period"), "path": _repo_relative(config, Path(item.get("path", ""))), "sha256": item.get("sha256")} for item in archives], "staging_rows": metadata.get("rows"), "build_mode": metadata.get("build_mode", "raw_only")}
    write_metadata_json(verification_root / "extension_input_inventory.json", inventory)
    write_metadata_json(verification_root / "extension_missing_sources.json", {"version": VERSION, "missing": missing, "status": "complete" if not missing else "blocked_missing_data"})
    manifest = {"version": VERSION, "created_at_utc": datetime.now(timezone.utc).isoformat(), "flows": list(flows), "requested_start_period": normalize_period(start_period), "requested_end_period": normalize_period(end_period), "partitions": int(len(audits)), "missing_partitions": int(len(missing)), "reconciliation_failures": int((~audit_frame["reconciliation_pass"].astype(bool)).sum()) if not audit_frame.empty else None, "build_mode": "raw_only_staging_projection", "policy_columns_present": False, "duty_fields_available": False, "input_inventory": _repo_relative(config, verification_root / "extension_input_inventory.json"), "partition_manifest": _repo_relative(config, verification_root / "extension_partition_manifest.parquet"), "status": "complete" if not missing and not audit_frame.empty and bool(audit_frame["reconciliation_pass"].all()) else "diagnostic_or_incomplete"}
    write_metadata_json(verification_root / "extension_build_manifest.json", manifest)
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", default="2013-01")
    parser.add_argument("--end", default="2025-12")
    parser.add_argument("--flow", choices=("imports", "exports", "all"), default="all")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--from-staging", action="store_true", help="project existing raw-only staging panels instead of reparsing ZIPs")
    args = parser.parse_args(argv)
    flows = ("imports", "exports") if args.flow == "all" else (args.flow,)
    builder = build_trade_extension_from_raw_staging if args.from_staging else build_trade_extension
    print(builder(PipelineConfig.default(), start_period=args.start, end_period=args.end, flows=flows, overwrite=args.overwrite))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
