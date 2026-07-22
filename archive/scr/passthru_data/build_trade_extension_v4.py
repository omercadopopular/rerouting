"""Archive-native, local-only trade extension through the available months.

Unlike the staging projection, this builder reads each downloaded ZIP detail
member, preserves raw duty fields when present, and writes independent CIF-preserving partitions
partitions.  It never attaches package policy or treatment variables.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import argparse
import json

import pandas as pd

from .build_trade_extension import _archive_path, _repo_relative
from .download_trade import FLOW_SPECS, _iter_fixed_width_chunks, _load_country_lookup
from .config import PipelineConfig
from .io_utils import add_hierarchy_codes, iter_months, normalize_hs_code, normalize_period, sha256_file, write_metadata_json, write_parquet


VERSION = "extension_v4_archive_native_cif"


def _parse_archive_cif(config: PipelineConfig, flow: str, period: str, archive: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    spec = FLOW_SPECS[flow]
    expected_year, expected_month = int(period[:4]), int(period[5:7])
    chunks = []
    source_rows = 0
    discarded_rows = 0
    for chunk in _iter_fixed_width_chunks(archive, spec["detail_member"], spec["detail_colspecs"], spec["detail_names"], chunksize=250_000):
        source_rows += len(chunk)
        chunk["hs10"] = chunk["hs10"].map(lambda value: normalize_hs_code(value, 10))
        chunk["cty_code"] = chunk["cty_code"].astype(str).str.strip().str.zfill(4)
        for col in ("year", "month", "gen_qy1_mo", "gen_val_mo", "gen_cif_mo", "dut_val_mo", "cal_dut_mo", "quantity", "trade_value"):
            if col in chunk.columns:
                chunk[col] = pd.to_numeric(chunk[col], errors="coerce")
        before = len(chunk)
        chunk = chunk.loc[(chunk["year"] == expected_year) & (chunk["month"] == expected_month)].copy()
        discarded_rows += before - len(chunk)
        chunk = chunk.dropna(subset=["hs10", "cty_code", "year", "month"])
        if chunk.empty:
            continue
        if flow == "imports":
            chunk["trade_value"] = chunk["gen_val_mo"]
            chunk["quantity"] = chunk["gen_qy1_mo"]
        chunks.append(chunk)
    if not chunks:
        raise RuntimeError(f"Archive {archive} produced no rows for expected period {period}")
    raw = pd.concat(chunks, ignore_index=True)
    aggregations = {}
    for col in ("gen_val_mo", "gen_cif_mo", "gen_qy1_mo", "dut_val_mo", "cal_dut_mo", "trade_value", "quantity"):
        if col in raw.columns:
            aggregations[col] = lambda values: values.sum(min_count=1)
    grouped = raw.groupby(["cty_code", "hs10", "year", "month"], as_index=False).agg(aggregations)
    grouped["period"] = period
    grouped["flow"] = flow
    grouped["hs10"] = grouped["hs10"].map(lambda value: normalize_hs_code(value, 10))
    grouped = add_hierarchy_codes(grouped, "hs10")
    country = _load_country_lookup(archive, flow).rename(columns={"cty_name": "partner_name"})
    country["cty_code"] = country["cty_code"].astype(str).str.strip().str.zfill(4)
    grouped = grouped.merge(country[["cty_code", "partner_name"]], on="cty_code", how="left").rename(columns={"cty_code": "partner_code"})
    if flow == "imports":
        grouped["quantity_missing"] = grouped["gen_qy1_mo"].isna()
        grouped["quantity_zero"] = grouped["gen_qy1_mo"].eq(0)
        grouped["unit_value"] = grouped["gen_cif_mo"] / grouped["gen_qy1_mo"].where(grouped["gen_qy1_mo"] > 0)
        grouped["cif_duty_unit_value"] = (grouped["gen_cif_mo"] + grouped["cal_dut_mo"]) / grouped["gen_qy1_mo"].where(grouped["gen_qy1_mo"] > 0)
        grouped["trade_value"] = grouped["gen_val_mo"]
        grouped["quantity"] = grouped["gen_qy1_mo"]
    else:
        grouped["quantity_missing"] = grouped["quantity"].isna()
        grouped["quantity_zero"] = grouped["quantity"].eq(0)
        grouped["unit_value"] = grouped["trade_value"] / grouped["quantity"].where(grouped["quantity"] > 0)
    grouped["source_archive"] = _repo_relative(config, archive)
    grouped["source_member"] = spec["detail_member"]
    grouped["source_sha256"] = sha256_file(archive)
    grouped["parser_version"] = VERSION
    base = ["flow", "partner_code", "partner_name", "hs10", "hs8", "hs6", "hs4", "hs2", "year", "month", "period"]
    fields = [c for c in ("gen_val_mo", "gen_cif_mo", "gen_qy1_mo", "trade_value", "quantity", "quantity_missing", "quantity_zero", "unit_value", "cif_duty_unit_value", "dut_val_mo", "cal_dut_mo", "source_archive", "source_member", "source_sha256", "parser_version") if c in grouped.columns]
    grouped = grouped[base + fields].sort_values(["partner_code", "hs10"]).reset_index(drop=True)
    source_field = "gen_cif_mo" if flow == "imports" else "trade_value"
    output_field = "gen_cif_mo" if flow == "imports" else "trade_value"
    source_total = float(raw[source_field].sum(min_count=1))
    output_total = float(grouped[output_field].sum(min_count=1))
    diff = output_total - source_total
    tol = max(1.0, 1e-8 * abs(source_total))
    audit = {
        "flow": flow, "period": period, "archive": _repo_relative(config, archive), "source_member": spec["detail_member"],
        "source_sha256": grouped["source_sha256"].iloc[0], "source_rows": int(source_rows), "discarded_period_rows": int(discarded_rows),
        "parsed_rows": int(len(raw)), "output_rows": int(len(grouped)),
        "duplicate_keys_before_aggregation": int(raw.duplicated(["cty_code", "hs10", "year", "month"]).sum()),
        "duplicate_keys_after_aggregation": int(grouped.duplicated(["partner_code", "hs10", "year", "month"]).sum()),
        "source_trade_value": source_total, "output_trade_value": output_total, "source_cif_value": float(raw["gen_cif_mo"].sum(min_count=1)) if "gen_cif_mo" in raw else None,
        "output_cif_value": float(grouped["gen_cif_mo"].sum(min_count=1)) if "gen_cif_mo" in grouped else None,
        "quantity_missing_rows": int(raw["gen_qy1_mo"].isna().sum()) if "gen_qy1_mo" in raw else int(raw["quantity"].isna().sum()),
        "quantity_zero_rows": int(raw["gen_qy1_mo"].eq(0).sum()) if "gen_qy1_mo" in raw else int(raw["quantity"].eq(0).sum()),
        "dut_val_mo_rows": int(raw["dut_val_mo"].notna().sum()) if "dut_val_mo" in raw else 0,
        "cal_dut_mo_rows": int(raw["cal_dut_mo"].notna().sum()) if "cal_dut_mo" in raw else 0,
        "trade_value_difference": diff, "trade_value_tolerance": tol, "reconciliation_pass": abs(diff) <= tol,
        "source_mode": "archive_native_cif_preserving",
    }
    return grouped, audit
def build_extension_v4(config: PipelineConfig, *, start_period: str = "2013-01", end_period: str = "2025-12", flows: tuple[str, ...] = ("imports", "exports"), overwrite: bool = False) -> dict[str, Any]:
    analysis = config.analysis_dir / "extension_v4_cif"
    verification = config.verification_dir / "extension_v4_cif"
    analysis.mkdir(parents=True, exist_ok=True)
    verification.mkdir(parents=True, exist_ok=True)
    progress_path = verification / "extension_progress.json" if len(flows) > 1 else verification / f"extension_progress_{flows[0]}.json"
    missing_path = verification / "extension_missing_sources.json" if len(flows) > 1 else verification / f"extension_missing_sources_{flows[0]}.json"
    manifest_path = verification / "extension_build_manifest.json" if len(flows) > 1 else verification / f"extension_build_manifest_{flows[0]}.json"
    audits: list[dict[str, Any]] = []
    missing: list[dict[str, str]] = []
    periods = iter_months(start_period, end_period)
    for flow in flows:
        for period in periods:
            archive = _archive_path(config, flow, period)
            partition = analysis / f"flow={flow}" / f"year={period[:4]}" / f"month={period[5:7]}" / "part.parquet"
            audit_path = verification / "audits" / f"{flow}_{period}.json"
            if not archive.exists():
                missing.append({"flow": flow, "period": period, "archive": _repo_relative(config, archive)})
                continue
            if partition.exists() and audit_path.exists() and not overwrite:
                prior = json.loads(audit_path.read_text(encoding="utf-8"))
                prior.setdefault("status", "passed" if bool(prior.get("reconciliation_pass")) else "failed")
                # Older v2 audit JSON used the source-facing names directly.
                # Normalize them here so a no-overwrite consolidation cannot
                # silently emit a quantity audit with only partition IDs.
                prior.setdefault("source_quantity_missing_rows", prior.get("quantity_missing_rows"))
                prior.setdefault("source_quantity_zero_rows", prior.get("quantity_zero_rows"))
                prior.setdefault("output_quantity_missing_rows", prior.get("quantity_missing_rows"))
                prior.setdefault("output_quantity_zero_rows", prior.get("quantity_zero_rows"))
                prior.setdefault("output_quantity_positive_rows", prior.get("quantity_positive_rows"))
                audits.append(prior)
                continue
            frame, audit = _parse_archive_cif(config, flow, period, archive)
            frame["parser_version"] = VERSION
            write_parquet(frame, partition, overwrite=True)
            source_missing = int(audit.get("quantity_missing_rows", 0))
            source_zero = int(audit.get("quantity_zero_rows", 0))
            audit.update({
                "version": VERSION,
                "partition": _repo_relative(config, partition),
                "partition_sha256": sha256_file(partition),
                "duty_fields_available": {column: column in frame.columns for column in ("dut_val_mo", "cal_dut_mo")},
                "dut_val_mo_rows": int(frame["dut_val_mo"].notna().sum()) if "dut_val_mo" in frame else 0,
                "dut_val_mo_total": float(frame["dut_val_mo"].sum(min_count=1) or 0.0) if "dut_val_mo" in frame else None,
                "cal_dut_mo_rows": int(frame["cal_dut_mo"].notna().sum()) if "cal_dut_mo" in frame else 0,
                "cal_dut_mo_total": float(frame["cal_dut_mo"].sum(min_count=1) or 0.0) if "cal_dut_mo" in frame else None,
                "source_quantity_missing_rows": source_missing,
                "source_quantity_zero_rows": source_zero,
                "output_quantity_missing_rows": int(frame["quantity"].isna().sum()),
                "output_quantity_zero_rows": int(frame["quantity"].eq(0).sum()),
                "output_quantity_positive_rows": int((frame["quantity"] > 0).sum()),
                "status": "passed" if bool(audit.get("reconciliation_pass")) else "failed",
            })
            write_metadata_json(audit_path, audit)
            audits.append(audit)
            # A resumable run exposes exact completed flow-month IDs without
            # pretending that the final extension gate has passed.
            write_metadata_json(progress_path, {
                "version": VERSION,
                "completed_partitions": len(audits),
                "expected_partitions": len(periods) * len(flows),
                "completed_ids": [f"{row.get('flow')}|{row.get('period')}" for row in audits],
                "status": "partial",
            })
    frame = pd.DataFrame(audits)
    if not frame.empty:
        write_parquet(frame, verification / "extension_monthly_reconciliation.parquet", overwrite=True)
        frame.groupby(["flow", "status"], dropna=False).size().reset_index(name="months").to_csv(verification / "extension_monthly_reconciliation.csv", index=False)
        partition_columns = ["flow", "period", "partition", "partition_sha256", "output_rows", "archive", "source_sha256"]
        write_parquet(frame[[column for column in partition_columns if column in frame.columns]].rename(columns={"archive": "source_archive"}), verification / "extension_partition_manifest.parquet", overwrite=True)
        write_parquet(frame[["flow", "period", "partition", "duplicate_keys_before_aggregation", "duplicate_keys_after_aggregation"]], verification / "extension_duplicate_audit.parquet", overwrite=True)
        quantity_columns = ["flow", "period", "partition", "source_quantity_missing_rows", "source_quantity_zero_rows", "output_quantity_missing_rows", "output_quantity_zero_rows", "output_quantity_positive_rows"]
        write_parquet(frame[[column for column in quantity_columns if column in frame.columns]], verification / "extension_quantity_audit.parquet", overwrite=True)
        duty_cols = [column for column in ("flow", "period", "partition", "dut_val_mo_rows", "dut_val_mo_total", "cal_dut_mo_rows", "cal_dut_mo_total") if column in frame]
        write_parquet(frame[duty_cols], verification / "extension_duty_audit.parquet", overwrite=True)
    status = "complete" if not missing and len(audits) == len(periods) * len(flows) and bool(frame.get("reconciliation_pass", pd.Series(dtype=bool)).all()) else "blocked_or_failed"
    # Reconcile the resumable marker with the consolidated audit set.  A
    # worker can be interrupted after writing a partition, and a later
    # no-overwrite consolidation must not leave a stale partial marker behind.
    write_metadata_json(progress_path, {
        "version": VERSION,
        "completed_partitions": len(audits),
        "expected_partitions": len(periods) * len(flows),
        "completed_ids": sorted({f"{row.get('flow')}|{row.get('period')}" for row in audits}),
        "missing_ids": sorted({f"{flow}|{period}" for flow in flows for period in periods
                                if not any(row.get("flow") == flow and row.get("period") == period for row in audits)}),
        "status": "complete" if status == "complete" else "partial",
    })
    write_metadata_json(missing_path, {"version": VERSION, "missing": missing, "status": "complete" if not missing else "blocked_missing_data"})
    manifest = {
        "version": VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "flows": list(flows),
        "requested_start_period": normalize_period(start_period),
        "requested_end_period": normalize_period(end_period),
        "archive_count": int(len(audits)),
        "missing_count": int(len(missing)),
        "partition_count": int(len(audits)),
        "reconciliation_failures": int((~frame["reconciliation_pass"].fillna(False)).sum()) if not frame.empty else None,
        "policy_columns_present": False,
        "source_mode": "archive_native_local_only",
        "status": status,
        "archive_validation_gate": "passed" if status == "complete" else "failed",
        "concordance_gate": "pending_native_vintage_audit",
        "cpi_real_value_gate": "not_run_nominal_canonical",
    }
    write_metadata_json(manifest_path, manifest)
    (verification / "extension_validation_report.md").write_text(
        "# Archive-native extension v4 (CIF-preserving)\n\n"
        f"Archive-native partitions: **{len(audits)}**; missing archives: **{len(missing)}**.\n\n"
        f"Status: **{status}**. Policy columns are explicitly excluded.\n",
        encoding="utf-8",
    )
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", default="2013-01")
    parser.add_argument("--end", default="2025-12")
    parser.add_argument("--flow", choices=("imports", "exports", "all"), default="all")
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args(argv)
    flows = ("imports", "exports") if args.flow == "all" else (args.flow,)
    print(build_extension_v4(PipelineConfig.default(), start_period=args.start, end_period=args.end, flows=flows, overwrite=args.overwrite))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
