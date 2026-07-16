"""Archive-native raw trade extension builder.

This path is intentionally separate from extension_v1, which is a projection of
raw-only staging.  It parses local ZIP detail members and preserves import duty
fields when those fields are present in the Census layout.  No package policy is
attached.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import argparse
import pandas as pd

from .build_trade_extension import _archive_path, _parse_archive, _repo_relative
from .config import PipelineConfig
from .io_utils import iter_months, normalize_period, sha256_file, write_metadata_json, write_parquet


VERSION = "extension_v2"


def build_trade_extension_v2(config: PipelineConfig, *, start_period: str = "2013-01", end_period: str = "2025-12", flows: tuple[str, ...] = ("imports", "exports"), periods: tuple[str, ...] | None = None, overwrite: bool = False) -> dict[str, Any]:
    output_root = config.analysis_dir / VERSION
    verification_root = config.verification_dir / VERSION
    output_root.mkdir(parents=True, exist_ok=True)
    verification_root.mkdir(parents=True, exist_ok=True)
    selected = tuple(periods) if periods else tuple(iter_months(start_period, end_period))
    audits: list[dict[str, Any]] = []
    missing: list[dict[str, str]] = []
    for flow in flows:
        for period in selected:
            archive = _archive_path(config, flow, period)
            partition = output_root / f"flow={flow}" / f"year={period[:4]}" / f"month={period[5:7]}" / "part.parquet"
            if not archive.exists():
                missing.append({"flow": flow, "period": period, "archive": _repo_relative(config, archive)})
                continue
            if partition.exists() and not overwrite:
                prior = verification_root / "audits" / f"{flow}_{period}.json"
                if prior.exists():
                    audits.append(__import__("json").loads(prior.read_text(encoding="utf-8")))
                    continue
            frame, audit = _parse_archive(config, flow, period, archive)
            write_parquet(frame, partition, overwrite=True)
            audit.update({"partition": _repo_relative(config, partition), "partition_sha256": sha256_file(partition), "build_mode": "archive_native", "policy_columns_present": False})
            write_metadata_json(verification_root / "audits" / f"{flow}_{period}.json", audit)
            audits.append(audit)
    audit_frame = pd.DataFrame(audits)
    write_parquet(audit_frame, verification_root / "monthly_reconciliation.parquet", overwrite=True)
    audit_frame.to_csv(verification_root / "monthly_reconciliation.csv", index=False)
    manifest = {
        "version": VERSION, "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "flows": list(flows), "selected_periods": list(selected), "partitions": len(audits),
        "missing_partitions": len(missing), "reconciliation_failures": int((~audit_frame["reconciliation_pass"].astype(bool)).sum()) if not audit_frame.empty else None,
        "build_mode": "archive_native", "policy_columns_present": False,
        "duty_fields_available": bool(not audit_frame.empty and (audit_frame.get("duty_rows", pd.Series(dtype=int)).fillna(0).sum() > 0)),
        "status": "complete" if not missing and not audit_frame.empty and bool(audit_frame["reconciliation_pass"].all()) else "incomplete",
    }
    write_metadata_json(verification_root / "build_manifest.json", manifest)
    write_metadata_json(verification_root / "missing_sources.json", {"version": VERSION, "missing": missing})
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", default="2013-01")
    parser.add_argument("--end", default="2025-12")
    parser.add_argument("--flow", choices=("imports", "exports", "all"), default="all")
    parser.add_argument("--period", action="append", dest="periods")
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args(argv)
    flows = ("imports", "exports") if args.flow == "all" else (args.flow,)
    print(build_trade_extension_v2(PipelineConfig.default(), start_period=args.start, end_period=args.end, flows=flows, periods=tuple(args.periods) if args.periods else None, overwrite=args.overwrite))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
