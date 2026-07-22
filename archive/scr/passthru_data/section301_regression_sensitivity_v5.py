"""Resumable Section 301 sensitivity grid (v5).

The estimator remains in v4 for historical comparison; v5 owns the artifact
grid, checkpoint lifecycle, and finalization contract.  This separation makes
fit execution counts auditable when cloned variants reuse an estimator result.
"""

from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any, Iterable
import hashlib
import json

import pandas as pd

from .config import PipelineConfig
from .io_utils import write_metadata_json, write_parquet
from .section301_regression_sensitivity_v4 import BRIDGES, OUTCOMES, VARIANTS, WINDOWS, _duplicate_source_variant


VERSION = "v5"


def artifact_dir(config: PipelineConfig) -> Path:
    path = config.verification_dir / "raw_replication_imports" / VERSION
    path.mkdir(parents=True, exist_ok=True)
    return path


def expected_grid() -> tuple[set[str], set[str]]:
    fits: set[str] = set()
    artifacts: set[str] = set()
    for bridge in BRIDGES:
        variants = tuple(v for v in VARIANTS if bridge in v.bridges)
        for window in WINDOWS:
            for outcome in OUTCOMES:
                for variant in variants:
                    artifact_id = f"{bridge}|{window}|{outcome}|{variant.code}"
                    artifacts.add(artifact_id)
                    if _duplicate_source_variant(variant, outcome) is None:
                        fits.add(artifact_id)
    return fits, artifacts


def expected_fit_ids() -> set[str]:
    return expected_grid()[0]


def expected_artifact_ids() -> set[str]:
    return expected_grid()[1]


def grid_counts() -> dict[str, int]:
    fits, artifacts = expected_grid()
    return {"expected_fits": len(fits), "expected_artifacts": len(artifacts)}


def _hash_payload(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()


def checkpoint_is_valid(manifest: dict[str, Any], *, source_hash: str, code_hash: str, spec_hash: str) -> bool:
    return (
        manifest.get("version") == VERSION
        and manifest.get("source_hash") == source_hash
        and manifest.get("code_hash") == code_hash
        and manifest.get("spec_hash") == spec_hash
        and manifest.get("status") in {"fit", "clone"}
    )


def write_current_fit(config: PipelineConfig, *, fit_id: str, rows: int, formula: str, fixed_effects: str, clusters: str, started_at_utc: str) -> Path:
    """Write the crash-recovery marker immediately before estimation."""
    path = artifact_dir(config) / "current_fit.json"
    write_metadata_json(path, {"version": VERSION, "fit_id": fit_id, "rows": int(rows), "formula": formula, "fixed_effects": fixed_effects, "clusters": clusters, "started_at_utc": started_at_utc})
    return path


def clear_current_fit(config: PipelineConfig) -> None:
    path = artifact_dir(config) / "current_fit.json"
    if path.exists():
        path.unlink()


def write_failure_manifest(config: PipelineConfig, *, fit_id: str, exc: BaseException) -> Path:
    path = artifact_dir(config) / "failure_manifest.json"
    write_metadata_json(path, {"version": VERSION, "fit_id": fit_id, "exception_type": type(exc).__name__, "exception_message": str(exc)})
    return path


def finalize_v5(config: PipelineConfig, records: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Materialize final outputs only after the complete artifact grid exists."""
    records = list(records)
    expected_fits, expected_artifacts = expected_grid()
    completed_artifacts = {str(r["artifact_id"]) for r in records}
    if completed_artifacts != expected_artifacts:
        missing = sorted(expected_artifacts - completed_artifacts)
        extra = sorted(completed_artifacts - expected_artifacts)
        raise RuntimeError(f"v5 artifact grid incomplete; missing={missing[:5]}, extra={extra[:5]}")
    out = artifact_dir(config)
    coeff = pd.DataFrame(records)
    # Final artifacts have separate scientific schemas.  A single generic
    # record table cannot truthfully serve as coefficients, sample audit, and
    # provenance simultaneously.
    coefficient_columns = [c for c in (
        "artifact_id", "fit_id", "analysis", "source_mode", "window", "outcome",
        "variant", "horizon", "estimate", "std_error", "ci_low", "ci_high",
    ) if c in coeff.columns]
    audit_columns = [c for c in (
        "artifact_id", "fit_id", "source_mode", "window", "outcome", "variant",
        "nobs", "treated_products", "sample_hash", "treatment_hash", "loss_stage",
    ) if c in coeff.columns]
    provenance_columns = [c for c in (
        "artifact_id", "fit_id", "source_mode", "source_path", "source_kind",
        "source_hash", "code_hash", "spec_hash", "sample_hash", "treatment_hash",
    ) if c in coeff.columns]
    write_parquet(coeff[coefficient_columns] if coefficient_columns else pd.DataFrame(), out / "section301_sensitivity_coefficients.parquet", overwrite=True)
    write_parquet(coeff[coefficient_columns] if coefficient_columns else pd.DataFrame(), out / "section301_sensitivity_comparison.parquet", overwrite=True)
    write_parquet(coeff[audit_columns] if audit_columns else pd.DataFrame(), out / "section301_sample_audit.parquet", overwrite=True)
    write_parquet(coeff[provenance_columns] if provenance_columns else pd.DataFrame(), out / "section301_source_provenance.parquet", overwrite=True)
    def _summary(columns: list[str], preferred: tuple[str, ...]) -> pd.DataFrame:
        if not columns:
            return pd.DataFrame({"rows": [len(coeff)]})
        keys = [c for c in preferred if c in coeff.columns]
        if not keys:
            return pd.DataFrame({"rows": [len(coeff)]})
        return coeff[columns].groupby(keys, dropna=False).size().reset_index(name="rows")

    _summary(coefficient_columns, ("analysis", "outcome", "variant")).to_csv(out / "section301_sensitivity_comparison.csv", index=False)
    _summary(audit_columns, ("source_mode", "window", "outcome")).to_csv(out / "section301_sample_audit_summary.csv", index=False)
    _summary(provenance_columns, ("source_kind", "source_mode")).to_csv(out / "section301_source_provenance_summary.csv", index=False)
    completed_fits = expected_fits & {str(r.get("fit_id", r["artifact_id"])) for r in records if r.get("status") != "clone"}
    write_metadata_json(out / "progress.json", {"version": VERSION, "completed_fit_ids": sorted(completed_fits), "completed_artifact_ids": sorted(completed_artifacts), "remaining_fits": len(expected_fits - completed_fits), "remaining_artifacts": len(expected_artifacts - completed_artifacts)})
    write_metadata_json(out / "pipeline_manifest.json", {"version": VERSION, "expected_fits": len(expected_fits), "expected_artifacts": len(expected_artifacts), "completed_artifacts": len(completed_artifacts), "ready_for_extension": False})
    write_metadata_json(out / "section301_sensitivity_summary.json", {"version": VERSION, "ready_for_extension": False, "legal_mapping_changed": False})
    clear_current_fit(config)
    (out / "section301_sensitivity_report.md").write_text("# Section 301 sensitivity v5\n\nThe legal replication release gate remains unchanged and is not ready for extension.\n", encoding="utf-8")
    write_metadata_json(out / "section301_release_gate.json", {"version": VERSION, "ready_for_extension": False, "reason": "Diagnostic sensitivity run does not alter the legal policy mapping."})
    return {"version": VERSION, "expected_fits": len(expected_fits), "expected_artifacts": len(expected_artifacts), "completed_artifacts": len(completed_artifacts)}


def run_section301_regression_sensitivity(config: PipelineConfig) -> dict[str, Any]:
    """Run or report a blocked v5 preflight without fabricating estimates."""
    out = artifact_dir(config)
    package_manifest = config.verification_dir / "trade_regressions" / "package_benchmark_v5" / "package_full_manifest.json"
    if not package_manifest.exists():
        payload = {"version": VERSION, "status": "blocked", "reason": "missing package_full_manifest.json", "ready_for_extension": False}
        write_metadata_json(out / "v5_preflight.json", payload)
        return payload
    common_manifest = config.verification_dir / "trade_regressions" / "package_benchmark_v5" / "common_sample" / "package_common_sample_manifest.json"
    if not common_manifest.exists():
        payload = {"version": VERSION, "status": "blocked", "reason": "missing package_common_sample_manifest.json", "ready_for_extension": False}
        write_metadata_json(out / "v5_preflight.json", payload)
        return payload
    payload = {"version": VERSION, "status": "blocked", "reason": "v5 estimator grid requires implementation of fit execution after package and common-sample gates", "ready_for_extension": False}
    write_metadata_json(out / "v5_preflight.json", payload)
    return payload
