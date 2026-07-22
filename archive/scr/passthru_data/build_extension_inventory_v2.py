"""Build a truthful local-only inventory of Census trade archives and auxiliaries."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import json

from .config import PipelineConfig
from .build_trade_extension import _archive_path, _repo_relative
from .io_utils import iter_months, sha256_file, write_metadata_json

VERSION = "extension_input_inventory_v2"


def _write_progress(path: Path, payload: dict[str, Any]) -> None:
    """Persist progress, tolerating a transient OneDrive replacement lock."""
    import time

    for attempt in range(6):
        try:
            write_metadata_json(path, payload)
            return
        except PermissionError:
            if attempt == 5:
                raise
            time.sleep(0.5 * (attempt + 1))


def build_inventory(config: PipelineConfig, *, start_period: str = "2013-01", end_period: str = "2025-12") -> dict[str, Any]:
    root = config.verification_dir / "extension_v4_cif"
    root.mkdir(parents=True, exist_ok=True)
    progress_path = root / "inventory_hash_progress.json"
    prior = json.loads(progress_path.read_text(encoding="utf-8")) if progress_path.exists() else {"files": {}}
    files = prior.setdefault("files", {})
    payload: dict[str, Any] = {"version": VERSION, "created_at_utc": datetime.now(timezone.utc).isoformat(), "requested_start_period": start_period, "requested_end_period": end_period, "flows": {}}
    for flow in ("imports", "exports"):
        archives = []
        for period in iter_months(start_period, end_period):
            path = _archive_path(config, flow, period)
            record = {"period": period, "path": _repo_relative(config, path), "bytes": path.stat().st_size if path.exists() else None}
            if path.exists():
                key = str(path.resolve())
                cached = files.get(key)
                digest = cached.get("sha256") if cached and cached.get("bytes") == record["bytes"] else None
                if not digest:
                    audit_path = root / "audits" / f"{flow}_{period}.json"
                    audit = json.loads(audit_path.read_text(encoding="utf-8")) if audit_path.exists() else {}
                    digest = audit.get("source_sha256") or sha256_file(path)
                record["sha256"] = digest
                record["status"] = "present"
                files[key] = {"period": period, "bytes": record["bytes"], "sha256": digest}
            else:
                record["sha256"] = None
                record["status"] = "missing"
            archives.append(record)
            if len(archives) == 1 or len(archives) % 12 == 0 or period == end_period:
                _write_progress(progress_path, {"version": VERSION, "status": "partial", "files": files, "last_flow": flow, "last_period": period})
        auxiliary = config.raw_dir / "trade" / flow / "CONCORD.PARQUET"
        if not auxiliary.exists():
            candidates = sorted((config.raw_dir / "trade" / flow).glob("*concord*.parquet"))
            auxiliary = candidates[0] if candidates else None
        payload["flows"][flow] = {
            "archive_count": len(archives),
            "auxiliary_concordance_count": 1 if auxiliary and auxiliary.exists() else 0,
            "auxiliary_concordance": _repo_relative(config, auxiliary) if auxiliary and auxiliary.exists() else None,
            "periods": [row["period"] for row in archives if row["status"] == "present"],
            "missing_periods": [row["period"] for row in archives if row["status"] == "missing"],
            "source_files": archives,
        }
    payload["status"] = "complete" if all(not row["missing_periods"] for row in payload["flows"].values()) else "blocked_missing_data"
    payload["hashes_complete"] = all(row["sha256"] for flow in payload["flows"].values() for row in flow["source_files"])
    payload["archive_count_total"] = sum(flow["archive_count"] for flow in payload["flows"].values())
    payload["auxiliary_concordance_count_total"] = sum(flow["auxiliary_concordance_count"] for flow in payload["flows"].values())
    write_metadata_json(root / "extension_input_inventory.json", payload)
    _write_progress(progress_path, {"version": VERSION, "status": "complete" if payload["status"] == "complete" and payload["hashes_complete"] else "partial", "files": files})
    return payload


if __name__ == "__main__":
    print(build_inventory(PipelineConfig.default()))
