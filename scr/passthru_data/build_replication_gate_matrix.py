"""Generate the replication readiness matrix from canonical manifests."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import argparse
import json

from .config import PipelineConfig
from .io_utils import write_metadata_json


VERSION = "replication_gate_matrix_v3_policy_separated"


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
    policy_2025 = _load(config.verification_dir / "policy_2025_preflight" / "policy_2025_preflight_manifest.json")
    historical_policy_root = config.verification_dir / "raw_replication_imports" / "policy_replication_v2"
    paper_variable = _load(historical_policy_root / "paper_compatibility_variable_gate.json")
    legal_variable = _load(historical_policy_root / "section301_variable_gate.json")
    policy_curves = _load(historical_policy_root / "regressions" / "section301_policy_curve_gate.json")

    rows: list[dict[str, Any]] = []
    if package.get("status") == "complete" and package.get("completed_fit_count") == 8 and pdf_path.exists():
        rows.append({"specification": "package_import_pdf", "status": "passed", "detail": "8 fits and frozen PDF comparison are complete"})
    if aligned.get("status") == "complete":
        rows.append({"specification": "package_common_sample", "status": "complete", "detail": f"{aligned.get('package_rows')} aligned import rows"})
    failed_bridge_metrics = {
        metric
        for fit in bridge.get("failed_fit_metrics", [])
        for metric in fit.get("failed_metrics", [])
    }
    point_failures = failed_bridge_metrics - {"ci_overlap"}
    raw_point_gate = "passed" if not point_failures and bool(bridge) else "failed"
    raw_inference_diagnostic = "passed" if bridge.get("registered_numeric_gate") is True else "failed"
    rows.append({"specification": "raw_outcome_point_estimates", "status": raw_point_gate, "detail": "correlation, RMSE, maximum gap, and sign agreement"})
    rows.append({"specification": "raw_outcome_inference_diagnostic", "status": raw_inference_diagnostic, "detail": "event duty-inclusive-price CI overlap remains below 0.80"})
    staging_gate = "passed" if staging.get("status") == "complete" and int(staging.get("reconciliation_failures", 0)) == 0 else staging.get("status", "pending")
    rows.extend([
        {"specification": "raw_trade_archive_ingestion", "status": extension.get("archive_validation_gate", "pending"), "detail": f"{extension.get('archive_count', 0)} archives"},
        {"specification": "raw_trade_staging_reconciliation", "status": staging_gate, "detail": f"{staging.get('partition_count', 0)} archive-native partitions; {staging.get('reconciliation_failures', 0)} failures"},
        {"specification": "raw_trade_quantity_semantics", "status": quantity_tokens.get("quantity_token_gate", "pending"), "detail": "source fixed-width quantity token audit"},
        {"specification": "raw_trade_duty_preservation", "status": "pending", "detail": "duty fields are present; units and source semantics require review"},
        {"specification": "raw_trade_concordance", "status": "pending" if native.get("mapping_gate") != "passed" else "passed", "detail": native.get("mapping_gate", "native audit absent")},
        {"specification": "raw_trade_real_values", "status": "not_required_for_replication", "detail": "nominal values are canonical; local CPI data are preserved for future analysis"},
        {"specification": "historical_paper_policy", "status": "passed" if policy_curves.get("historical_policy_methodology_locked") else "pending", "detail": "paper-compatible source-vintage assignment and point-estimate substitution curves"},
        {"specification": "independent_2018_final_legal_policy", "status": legal_variable.get("status", "pending"), "detail": "final legal scope/date/rate variable diagnostic; legal-calendar curves are not expected to match paper timing"},
        {"specification": "forward_2025_policy_ledger", "status": policy_2025.get("independent_policy_gate", "failed"), "detail": "2025 official ledger sources remain incomplete"},
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
        "raw_trade_real_value_gate": "not_required_for_replication",
        "cpi_data_preserved_for_future_use": True,
        "raw_outcome_point_estimate_gate": raw_point_gate,
        "raw_outcome_inference_diagnostic": raw_inference_diagnostic,
        "historical_paper_policy_variable_gate": paper_variable.get("status", "pending"),
        "historical_paper_policy_curve_gate": "passed" if policy_curves.get("paper_compatible_point_estimate_curve_gate_passed") else "pending",
        "historical_replication_methodology_lock": bool(policy_curves.get("historical_policy_methodology_locked") and raw_point_gate == "passed" and package_gate == "passed"),
        "independent_2018_final_legal_variable_gate": legal_variable.get("status", "pending"),
        "forward_2025_policy_ledger_gate": policy_2025.get("independent_policy_gate", "failed"),
        "section301_v5_ready": False,
        "event_2025_ready": False,
        "aligned_bridge_import_universe": {"rows": aligned.get("package_rows"), "keys_identical": aligned.get("package_distinct_keys") == aligned.get("raw_distinct_keys"), "policy_semantics_changed": aligned.get("policy_semantics_changed")},
        "items": {
            "figure_2_import_event": "gate_passed" if package_gate == "passed" else "gate_failed",
            "figure_4a_import_dynamic": "gate_passed" if package_gate == "passed" else "gate_failed",
            "figure_4b_export_dynamic": "blocked_missing_data",
            "common_sample_bridge_point_estimates": "gate_passed" if raw_point_gate == "passed" else "gate_failed",
            "common_sample_bridge_inference": "gate_passed" if raw_inference_diagnostic == "passed" else "gate_failed",
            "historical_paper_compatible_section301_policy": "gate_passed" if policy_curves.get("historical_policy_methodology_locked") else "ran_no_gate",
            "independent_2018_final_legal_policy": "gate_passed" if legal_variable.get("status") == "passed" else "gate_failed",
            "forward_2025_policy_ledger": "blocked_missing_data",
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
    lines.extend(["", "Package import replication covers Figures 2 and 4a only. Figure 4b exports and other tables remain outside the package gate.", "", "The historical paper-compatible policy gate is distinct from the final-legal diagnostic and from the missing 2025 policy ledger. CPI inputs remain in place for future work but are not required for the original nominal replication."])
    (docs / "replication_coverage_matrix.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return payload


def main() -> int:
    print(build_gate_matrix(PipelineConfig.default()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
