"""Audit package benchmark checkpoints without modifying estimator outputs."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import json

from .config import PipelineConfig
from .io_utils import read_table, sha256_file, write_metadata_json
from .package_benchmark import EXPECTED_FIT_IDS, estimator_fingerprint, specification_fingerprint


def audit_package_checkpoints(config: PipelineConfig) -> dict[str, object]:
    root = config.verification_dir / "trade_regressions" / "package_benchmark_v5"
    cache = root / "cache" / "package_full_panel_hs10fixed.parquet"
    current_estimator = estimator_fingerprint(config)
    current_specification = specification_fingerprint(config)
    rows: list[dict[str, object]] = []
    for fit_id in sorted(EXPECTED_FIT_IDS):
        _, spec, outcome = fit_id.split("|")
        directory = root / "checkpoints" / spec / outcome
        manifest_path = directory / "manifest.json"
        coefficient_path = directory / "coefficients.parquet"
        audit_path = directory / "sample_audit.parquet"
        record: dict[str, object] = {"fit_id": fit_id, "valid": False, "reason": None}
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            coefficient = read_table(coefficient_path)
            audit = read_table(audit_path)
            horizon_column = "event_time" if "event_time" in coefficient.columns else "horizon"
            checks = {
                "manifest_exists": manifest_path.exists(),
                "coefficient_exists": coefficient_path.exists(),
                "audit_exists": audit_path.exists(),
                "fit_id_match": manifest.get("fit_id") == fit_id,
                "source_hash": manifest.get("source_fingerprint") == sha256_file(cache),
                "estimator_hash": manifest.get("estimator_fingerprint") == current_estimator,
                "specification_hash": manifest.get("specification_fingerprint") == current_specification,
                "horizons": coefficient[horizon_column].nunique() == 13,
                "observation_count": int(manifest.get("observation_count")) == int(coefficient["nobs"].iloc[0]) == int(audit["nobs"].iloc[0]),
            }
            record.update(checks)
            record["valid"] = all(checks.values())
            if not record["valid"]:
                record["reason"] = ",".join(key for key, value in checks.items() if not value)
        except Exception as exc:  # diagnostic audit must report, not conceal, malformed checkpoints
            record["reason"] = f"{type(exc).__name__}: {exc}"
        rows.append(record)
    valid_ids = sorted(row["fit_id"] for row in rows if row["valid"])
    payload = {
        "version": "v5",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "expected_fit_count": len(EXPECTED_FIT_IDS),
        "valid_fit_count": len(valid_ids),
        "valid_fit_ids": valid_ids,
        "status": "passed" if len(valid_ids) == len(EXPECTED_FIT_IDS) else "failed",
        "checks": rows,
    }
    write_metadata_json(root / "package_checkpoint_integrity_audit.json", payload)
    return payload


if __name__ == "__main__":
    print(audit_package_checkpoints(PipelineConfig.default()))
