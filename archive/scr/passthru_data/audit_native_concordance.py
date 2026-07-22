"""Audit native monthly HTS descriptions/concordances from local ZIP archives."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import argparse
import hashlib
import zipfile

import pandas as pd

from .build_trade_extension import _archive_path, _repo_relative
from .config import PipelineConfig
from .download_trade import FLOW_SPECS, _load_concord, _resolve_member_name
from .io_utils import iter_months, sha256_file, write_metadata_json, write_parquet


VERSION = "extension_native_concordance_audit_v1"


def _member_sha256(archive: Path, member: str) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with zipfile.ZipFile(archive) as zf:
        with zf.open(_resolve_member_name(zf, member)) as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                size += len(chunk)
                digest.update(chunk)
    return digest.hexdigest(), size


def audit_native_concordances(config: PipelineConfig, *, start_period: str = "2013-01", end_period: str = "2025-12") -> dict[str, Any]:
    out = config.verification_dir / "extension_v3"
    out.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    previous: dict[str, dict[str, str]] = {"imports": {}, "exports": {}}
    for flow in ("imports", "exports"):
        for period in iter_months(start_period, end_period):
            archive = _archive_path(config, flow, period)
            if not archive.exists():
                rows.append({"flow": flow, "period": period, "archive": _repo_relative(config, archive), "status": "missing_archive"})
                continue
            concord_member = FLOW_SPECS[flow]["concord_member"]
            try:
                frame = _load_concord(archive, flow)
                member_hash, member_bytes = _member_sha256(archive, concord_member)
                current = dict(zip(frame["hs10"].astype(str), frame["hs10_desc"].astype(str)))
                prior = previous[flow]
                current_codes, prior_codes = set(current), set(prior)
                added, removed = current_codes - prior_codes, prior_codes - current_codes
                changed = sum(1 for code in current_codes & prior_codes if current[code] != prior[code])
                with zipfile.ZipFile(archive) as archive_handle:
                    obsolete_members = [name for name in archive_handle.namelist() if "obsolete" in name.lower() or "concord" in name.lower()]
                rows.append({
                    "flow": flow,
                    "period": period,
                    "archive": _repo_relative(config, archive),
                    "archive_sha256": sha256_file(archive),
                    "concordance_member": concord_member,
                    "concordance_member_sha256": member_hash,
                    "concordance_member_bytes": member_bytes,
                    "native_code_count": len(current),
                    "codes_added_from_prior": len(added) if prior else None,
                    "codes_removed_from_prior": len(removed) if prior else None,
                    "description_changes_from_prior": changed if prior else None,
                    "obsolete_mapping_members": obsolete_members,
                    "one_to_many_count": None,
                    "many_to_one_count": None,
                    "unmatched_count": None,
                    "mapping_status": "pending_obsolete_mapping_parse" if obsolete_members else "native_description_audit_only",
                    "status": "passed",
                })
                previous[flow] = current
            except Exception as exc:
                rows.append({"flow": flow, "period": period, "archive": _repo_relative(config, archive), "status": "failed", "error_type": type(exc).__name__, "error_message": str(exc)})
    frame = pd.DataFrame(rows)
    write_parquet(frame, out / "extension_native_concordance_audit.parquet", overwrite=True)
    summary = frame.groupby(["flow", "status"], dropna=False).size().reset_index(name="months")
    summary.to_csv(out / "extension_native_concordance_summary.csv", index=False)
    manifest = {
        "version": VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "rows": int(len(frame)),
        "passed": int((frame["status"] == "passed").sum()) if not frame.empty else 0,
        "failed": int((frame["status"] != "passed").sum()) if not frame.empty else 0,
        "native_description_gate": "passed" if not frame.empty and bool((frame["status"] == "passed").all()) else "failed",
        "mapping_gate": "pending_obsolete_mapping_parse",
        "mapping_counts_are_null_until_obsolete_mapping_parse": True,
        "source_mode": "archive_native_local_only",
    }
    write_metadata_json(out / "extension_native_concordance_manifest.json", manifest)
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", default="2013-01")
    parser.add_argument("--end", default="2025-12")
    args = parser.parse_args(argv)
    print(audit_native_concordances(PipelineConfig.default(), start_period=args.start, end_period=args.end))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
