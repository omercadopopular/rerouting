"""Build the versioned historical-replication lock from canonical manifests."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import json

from .config import PipelineConfig
from .io_utils import write_metadata_json


VERSION = "replication_methodology_lock_v2_policy_separated"


def _load(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def build_methodology_lock(config: PipelineConfig) -> dict[str, Any]:
    """Materialize the lock only from versioned empirical evidence.

    The historical paper-compatible object, the independent 2018 final-legal
    diagnostic, and the future 2025 ledger intentionally have separate gates.
    """
    benchmark = config.verification_dir / "trade_regressions" / "package_benchmark_v5"
    package = _load(benchmark / "package_full_manifest.json")
    package_pdf = _load(benchmark / "package_pdf_comparison_manifest.json")
    bridge_root = benchmark / "common_sample_v5_cif" / "bridge_resumable_v5"
    bridge = _load(bridge_root / "bridge_gate.json")

    policy_root = config.verification_dir / "raw_replication_imports" / "policy_replication_v2"
    paper_variable = _load(policy_root / "paper_compatibility_variable_gate.json")
    paper_encoding = _load(policy_root / "paper_compatibility_event_encoding_gate.json")
    legal_variable = _load(policy_root / "section301_variable_gate.json")
    policy_curve = _load(policy_root / "regressions" / "section301_policy_curve_gate.json")
    pooled_policy_root = config.verification_dir / "raw_replication_imports" / "pooled_policy_replication_v1"
    pooled_policy = _load(pooled_policy_root / "pooled_policy_replication_gate.json")
    missing_policy_sources = _load(policy_root / "paper_compatibility_missing_sources.json")
    policy_2025 = _load(
        config.verification_dir
        / "policy_2025_preflight"
        / "policy_2025_preflight_manifest.json"
    )

    failed_bridge_metrics = {
        metric
        for fit in bridge.get("failed_fit_metrics", [])
        for metric in fit.get("failed_metrics", [])
    }
    bridge_point_gate = bool(bridge) and not (failed_bridge_metrics - {"ci_overlap"})
    bridge_inference_gate = bool(bridge.get("registered_numeric_gate"))
    package_gate = bool(
        package.get("status") == "complete"
        and package.get("completed_fit_count") == 8
        and package.get("package_pdf_gate") == "passed"
    )
    historical_policy_gate = bool(
        policy_curve.get("historical_policy_methodology_locked")
        and pooled_policy.get("paper_compatible_gate") is True
    )
    historical_lock = bool(package_gate and bridge_point_gate and historical_policy_gate)
    forward_policy_gate = policy_2025.get("independent_policy_gate", "failed")

    gates = {
        "package_import_pdf_gate": "passed" if package_gate else "failed",
        "package_provenance_gate": "passed" if package.get("completed_fit_count") == 8 else "failed",
        "raw_outcome_point_estimate_gate": "passed" if bridge_point_gate else "failed",
        "raw_outcome_inference_diagnostic": "passed" if bridge_inference_gate else "failed",
        "paper_compatible_policy_variable_gate": paper_variable.get("status", "pending"),
        "paper_compatible_event_encoding_gate": paper_encoding.get("status", "pending"),
        "paper_compatible_policy_curve_gate": "passed" if policy_curve.get("paper_compatible_point_estimate_curve_gate_passed") else "failed",
        "historical_pooled_policy_gate": "passed" if pooled_policy.get("paper_compatible_gate") is True else "failed",
        "historical_replication_methodology_lock": "passed" if historical_lock else "failed",
        "independent_2018_final_legal_variable_gate": legal_variable.get("status", "pending"),
        "forward_2025_policy_ledger_gate": forward_policy_gate,
        "cpi_real_values_for_historical_replication": "not_required_for_replication",
        "section301_v5_ready": False,
        "event_2025_ready": False,
    }
    payload: dict[str, Any] = {
        "version": VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": {
            "included": ["Figure 2 imports", "Figure 4a imports"],
            "excluded": ["Figure 4b exports", "other manuscript tables", "2025 tariff event study"],
        },
        "gates": gates,
        "historical_methodology_locked": historical_lock,
        "outcomes": {
            "value": "GEN_CIF_MO / 1,000,000",
            "quantity": "GEN_QY1_MO / 1,000,000 with missing distinct from zero",
            "price": "GEN_CIF_MO / GEN_QY1_MO",
            "pduty": "(GEN_CIF_MO + CAL_DUT_MO) / GEN_QY1_MO",
        },
        "package_evidence": {
            "completed_fit_count": package.get("completed_fit_count"),
            "expected_fit_count": package.get("expected_fit_count"),
            "max_pdf_abs_difference": package_pdf.get("max_abs_difference"),
            "threshold": package_pdf.get("required_threshold"),
            "missing_export_figure_4b": package_pdf.get("missing_export_fig_04b", True),
        },
        "raw_outcome_evidence": {
            "point_estimate_gate": gates["raw_outcome_point_estimate_gate"],
            "inference_diagnostic": gates["raw_outcome_inference_diagnostic"],
            "failed_registered_metrics": sorted(failed_bridge_metrics),
            "bridge_gate_manifest": str((bridge_root / "bridge_gate.json").relative_to(config.repo_root)).replace("\\", "/"),
        },
        "policy_evidence": {
            "paper_compatible_variable_gate": paper_variable,
            "paper_compatible_event_encoding_gate": paper_encoding,
            "historical_curve_gate_manifest": str((policy_root / "regressions" / "section301_policy_curve_gate.json").relative_to(config.repo_root)).replace("\\", "/"),
            "pooled_policy_gate_manifest": str((pooled_policy_root / "pooled_policy_replication_gate.json").relative_to(config.repo_root)).replace("\\", "/"),
            "pooled_policy_gate": pooled_policy,
            "independent_2018_final_legal_variable_gate": legal_variable,
            "paper_compatible_uses_validation_derived_reconciliation": True,
            "paper_compatible_is_independent_legal_evidence": False,
            "missing_source_status": missing_policy_sources.get("status", "unknown"),
            "forward_2025_ledger_ready": False,
        },
        "figures": policy_curve.get("figures", {}),
        "cpi": {
            "data_preserved": True,
            "historical_replication_role": "not_required_for_replication",
            "nominal_values_are_canonical": True,
        },
    }

    docs = config.repo_root / "scr" / "docs"
    json_path = docs / "replication_methodology_lock_v2.json"
    md_path = docs / "replication_methodology_lock_v2.md"
    write_metadata_json(json_path, payload)
    lines = [
        "# Historical replication methodology lock v2",
        "",
        f"Generated from canonical manifests on {payload['created_at_utc']}.",
        "",
        f"Overall historical lock: **{'passed' if historical_lock else 'failed'}**.",
        "",
        "| Gate | Status |",
        "|---|---|",
    ]
    for name, status in gates.items():
        lines.append(f"| `{name}` | `{status}` |")
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "The lock covers the original-period U.S. import results in Figures 2 and 4a. It requires the package estimator, raw Census outcome point estimates, the reconstructed paper-compatible Section 301 assignment, and the independently sourced pooled 201/232/301 policy comparison to pass their separate gates.",
            "",
            "The paper-compatible schedule is a transparent historical-reproduction object. It uses official archived sources plus frozen, row-level validation-derived reconciliations for missing proposal-era annexes, the historical exclusion parser behavior, and the 2018 HTS transition. It is not labeled independent final-legal evidence.",
            "",
            "The independent 2018 final-legal schedule is retained as a separate diagnostic, and the forward 2025 ledger remains unready. Nothing in this lock authorizes reuse of the historical reconciliations in 2025.",
            "",
            "Confidence-interval overlap is retained as a secondary inference diagnostic. The accepted raw-outcome replication criterion here concerns point estimates; the registered CI diagnostic remains reported without changing its threshold.",
            "",
            "CPI files are preserved for future work. Real values are not required for the original nominal replication and nominal source fields remain canonical.",
        ]
    )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return payload


def main() -> int:
    payload = build_methodology_lock(PipelineConfig.default())
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
