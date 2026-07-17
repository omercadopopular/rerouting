"""Archive-native, local-only trade extension through the available months.

Unlike the staging projection, this builder reads each downloaded ZIP detail
member, preserves raw duty fields when present, and writes independent v2
partitions.  It never attaches package policy or treatment variables.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import argparse
import json

import pandas as pd

from .build_trade_extension import _archive_path, _parse_archive, _repo_relative
from .config import PipelineConfig
from .io_utils import iter_months, normalize_period, sha256_file, write_metadata_json, write_parquet


VERSION = "extension_v2_archive_native"


def build_extension_v2(config: PipelineConfig, *, start_period: str = "2013-01", end_period: str = "2025-12", flows: tuple[str, ...] = ("imports", "exports"), overwrite: bool = False) -> dict[str, Any]:
    analysis = config.analysis_dir / "extension_v2"
    verification = config.verification_dir / "extension_v2"
    analysis.mkdir(parents=True, exist_ok=True)
    verification.mkdir(parents=True, exist_ok=True)
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
                audits.append(prior)
                continue
            frame, audit = _parse_archive(config, flow, period, archive)
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
            write_metadata_json(verification / "extension_progress.json", {
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
    write_metadata_json(verification / "extension_progress.json", {
        "version": VERSION,
        "completed_partitions": len(audits),
        "expected_partitions": len(periods) * len(flows),
        "completed_ids": sorted({f"{row.get('flow')}|{row.get('period')}" for row in audits}),
        "missing_ids": sorted({f"{flow}|{period}" for flow in flows for period in periods
                                if not any(row.get("flow") == flow and row.get("period") == period for row in audits)}),
        "status": "complete" if status == "complete" else "partial",
    })
    write_metadata_json(verification / "extension_missing_sources.json", {"version": VERSION, "missing": missing, "status": "complete" if not missing else "blocked_missing_data"})
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
    write_metadata_json(verification / "extension_build_manifest.json", manifest)
    (verification / "extension_validation_report.md").write_text(
        "# Archive-native extension v2\n\n"
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
    print(build_extension_v2(PipelineConfig.default(), start_period=args.start, end_period=args.end, flows=flows, overwrite=args.overwrite))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
