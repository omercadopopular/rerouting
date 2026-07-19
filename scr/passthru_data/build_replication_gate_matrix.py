"""Generate the replication readiness matrix from canonical manifests."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import argparse
import json

import pandas as pd

from .config import PipelineConfig
from .io_utils import write_metadata_json, write_parquet


VERSION = "replication_gate_matrix_v2"


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def build_gate_matrix(config: PipelineConfig) -> dict[str, Any]:
    package_root = config.verification_dir / "trade_regressions" / "package_benchmark_v5"
    package = _load(package_root / "package_full_manifest.json")
    pdf_path = package_root / "package_pdf_comparison.parquet"
    # v3 separates the package anchor from the raw-outcome source.  Retain v2
    # only as a historical fallback when the v3 run has not been materialized.
    bridge_root = package_root / "common_sample_v5_cif" / "bridge_resumable_v5"
    if not (bridge_root / "bridge_gate.json").exists():
        bridge_root = package_root / "common_sample_v2" / "bridge_resumable"
    bridge = _load(bridge_root / "bridge_gate.json")
    aligned = _load(package_root / "common_sample_v5_cif" / "aligned_bridge_manifest.json")
    if not aligned:
        aligned = _load(package_root / "common_sample_v2" / "aligned_bridge_manifest.json")
    extension = _load(config.verification_dir / "extension_v4_cif" / "extension_build_manifest.json")
    staging = _load(config.verification_dir / "extension_v4_cif" / "extension_build_manifest.json")
    native = _load(config.verification_dir / "extension_v4_cif" / "extension_concordance_audit.json")
    quantity_tokens = _load(config.verification_dir / "extension_v4_cif" / "extension_quantity_audit.json")
    policy = _load(config.verification_dir / "policy_2025_preflight" / "policy_2025_preflight_manifest.json")

    rows: list[dict[str, Any]] = []
    if package.get("status") == "complete" and package.get("completed_fit_count") == 8 and pdf_path.exists():
        rows.append({"specification": "package_import_pdf", "status": "passed", "detail": "8 fits and frozen PDF comparison are complete"})
    if aligned.get("status") == "complete":
        rows.append({"specification": "package_common_sample", "status": "complete", "detail": f"{aligned.get('package_rows')} aligned import rows"})
    if bridge.get("registered_numeric_gate") is True:
        bridge_status = "passed"
    else:
        bridge_status = "failed"
    rows.append({"specification": "raw_outcome_bridge", "status": bridge_status, "detail": "registered thresholds are unchanged"})
    staging_gate = "passed" if staging.get("status") == "complete" and int(staging.get("reconciliation_failures", 0)) == 0 else staging.get("status", "pending")
    rows.extend([
        {"specification": "raw_trade_archive_ingestion", "status": extension.get("archive_validation_gate", "pending"), "detail": f"{extension.get('archive_count', 0)} archives"},
        {"specification": "raw_trade_staging_reconciliation", "status": staging_gate, "detail": f"{staging.get('partition_count', 0)} archive-native partitions; {staging.get('reconciliation_failures', 0)} failures"},
        {"specification": "raw_trade_quantity_semantics", "status": quantity_tokens.get("quantity_token_gate", "pending"), "detail": "source fixed-width quantity token audit"},
        {"specification": "raw_trade_duty_preservation", "status": "pending", "detail": "duty fields are present; units and source semantics require review"},
        {"specification": "raw_trade_concordance", "status": "pending" if native.get("mapping_gate") != "passed" else "passed", "detail": native.get("mapping_gate", "native audit absent")},
        {"specification": "raw_trade_real_values", "status": "pending", "detail": "nominal extension is canonical; CPI real-value build not run"},
        {"specification": "independent_policy", "status": policy.get("independent_policy_gate", "failed"), "detail": "2025 ledger sources remain incomplete"},
    ])
    package_gate = "passed" if package.get("status") == "complete" else "failed"
    archive_gate = extension.get("archive_validation_gate", "pending")
    payload: dict[str, Any] = {
        "version": VERSION,
        "created_at": datetime.now(timezone.utc).date().isoformat(),
        "package_import_pdf_gate": package_gate,
        "package_provenance_gate": "passed" if package.get("completed_fit_count") == 8 else "failed",
        "package_full_vs_common_gate": "passed" if aligned.get("status") == "complete" else "pending",
        "raw_trade_archive_ingestion_gate": archive_gate,
        "raw_trade_staging_reconciliation_gate": staging_gate,
        "raw_trade_quantity_semantics_gate": quantity_tokens.get("quantity_token_gate", "pending"),
        "raw_trade_duty_preservation_gate": "pending",
        "raw_trade_concordance_gate": "pending" if native.get("mapping_gate") != "passed" else "passed",
        "raw_trade_real_value_gate": "pending",
        "raw_outcome_bridge_gate": bridge_status,
        "independent_policy_gate": policy.get("independent_policy_gate", "failed"),
        "section301_v5_ready": False,
        "event_2025_ready": False,
        "aligned_bridge_import_universe": {"rows": aligned.get("package_rows"), "keys_identical": aligned.get("package_distinct_keys") == aligned.get("raw_distinct_keys"), "policy_semantics_changed": aligned.get("policy_semantics_changed")},
        "items": {
            "figure_2_import_event": "gate_passed" if package_gate == "passed" else "gate_failed",
            "figure_4a_import_dynamic": "gate_passed" if package_gate == "passed" else "gate_failed",
            "figure_4b_export_dynamic": "blocked_missing_data",
            "common_sample_bridge": "gate_passed" if bridge_status == "passed" else "gate_failed",
            "independent_section301_policy": "gate_failed",
            "raw_trade_extension_2025": "validated_nominal_archive_native" if archive_gate == "passed" and staging_gate == "passed" else "ran_no_gate",
            "event_2025": "blocked_missing_data",
        },
        "rows": rows,
    }
    docs = config.repo_root / "scr" / "docs"
    write_metadata_json(docs / "replication_coverage_matrix.json", payload)
    lines = ["# Replication coverage and readiness", "", f"Generated from canonical manifests on {payload['created_at']}.", "", "| Track | Status | Evidence |", "|---|---|---|"]
    for row in rows:
        lines.append(f"| {row['specification']} | `{row['status']}` | {row.get('detail', '')} |")
    lines.extend(["", "Package import replication covers Figures 2 and 4a only. Figure 4b exports and other tables remain outside the package gate.", "", "Section 301 v5 and the 2025 event remain blocked until the raw-outcome and independent-policy gates pass."])
    (docs / "replication_coverage_matrix.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return payload


def main() -> int:
    print(build_gate_matrix(PipelineConfig.default()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
