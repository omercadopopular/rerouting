"""Inventory local HTS/concordance inputs without inventing mapping losses."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import json

import pandas as pd

from .config import PipelineConfig
from .io_utils import sha256_file, write_metadata_json, write_parquet


VERSION = "extension_concordance_inventory_v1"


def audit_concordances(config: PipelineConfig) -> dict[str, Any]:
    out = config.verification_dir / "extension_v2"
    out.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    for flow in ("imports", "exports"):
        candidates = list((config.staging_dir / "passthru_data").glob(f"{flow}*concord*.parquet"))
        if not candidates:
            candidates = list(config.staging_dir.glob(f"{flow}*concord*.parquet"))
        if candidates:
            for path in candidates:
                rows.append({"flow": flow, "path": path.resolve().relative_to(config.repo_root.resolve()).as_posix(), "sha256": sha256_file(path), "status": "source_present_mapping_not_applied", "native_code_count": None, "one_to_many_count": None, "many_to_one_count": None, "unmatched_count": None})
        else:
            rows.append({"flow": flow, "path": None, "sha256": None, "status": "missing_concordance_source", "native_code_count": None, "one_to_many_count": None, "many_to_one_count": None, "unmatched_count": None})
    frame = pd.DataFrame(rows)
    write_parquet(frame, out / "extension_concordance_audit.parquet", overwrite=True)
    manifest = {"version": VERSION, "created_at_utc": datetime.now(timezone.utc).isoformat(), "rows": int(len(frame)), "status": "pending_mapping_comparison", "mapping_counts_are_null_until_native_vintage_comparison": True}
    write_metadata_json(out / "extension_concordance_manifest.json", manifest)
    return manifest


if __name__ == "__main__":
    print(audit_concordances(PipelineConfig.default()))
